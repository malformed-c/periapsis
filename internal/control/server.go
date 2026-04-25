// Copyright (C) 2024-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package control

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/malformed-c/periapsis/internal/config"
	"github.com/malformed-c/periapsis/internal/psi"
	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	pawnstats "github.com/malformed-c/periapsis/internal/stats"
	"github.com/malformed-c/periapsis/internal/version"
	"github.com/malformed-c/periapsis/node"
	"github.com/varlink/go/varlink"
)

const ifaceName = "io.perigeos.Manager"

// methodEntry is a single entry in the server's unified dispatch table.
// handler is called on every invocation; tcpAllowed gates access on the
// TCP+mTLS path (remote operators). Methods that mutate local state -
// Drain, Stop - are unix-socket-only: an mTLS certificate does not grant
// the right to shut down running pods.
type methodEntry struct {
	handler    func(ctx context.Context) any
	tcpAllowed bool
}

// managerIface implements varlink's dispatcher interface for the perigeos
// manager methods. It dispatches through the Server's unified method table.
type managerIface struct {
	server *Server
}

func (m *managerIface) VarlinkGetName() string        { return ifaceName }
func (m *managerIface) VarlinkGetDescription() string { return "" }

func (m *managerIface) VarlinkDispatch(ctx context.Context, c varlink.Call, methodname string) error {
	entry, ok := m.server.methods[methodname]
	if !ok {
		return c.ReplyMethodNotFound(ctx, methodname)
	}
	return c.Reply(ctx, entry.handler(ctx))
}

// QueueProvider exposes work queue depths for a pawn's PodController.
type QueueProvider interface {
	SyncPodsFromKubernetesQueueLen() int
	DeletePodsFromKubernetesQueueLen() int
	SyncPodStatusFromProviderQueueLen() int
}

// Server exposes a Varlink socket for the apsis CLI and remote control.
// ImageLister is implemented by image.ImageManager to list cached images.
// Using a named interface avoids an import cycle between control and image packages.
type ImageLister interface {
	ListCachedImagesJSON() []map[string]any
	GetLayerCachePath() string
}

type Server struct {
	socketPath string
	tcpAddr    string // optional TCP address for remote mTLS access
	tlsCert    *tls.Certificate
	tlsCACert  *x509.Certificate
	startTime  time.Time
	config     *config.PerigeosConfig
	logger     *slog.Logger

	mu      sync.RWMutex
	gambits []*node.Gambit
	queues  map[string]QueueProvider // pawn name -> PodController

	imageLister ImageLister // optional; set via SetImageLister

	methods map[string]methodEntry // unified dispatch table; built in New()

	varlinkSrv *varlink.Service
	tcpLn      net.Listener
}

func New(socketPath string, cfg *config.PerigeosConfig, logger *slog.Logger) *Server {
	s := &Server{
		socketPath: socketPath,
		startTime:  time.Now(),
		config:     cfg,
		logger:     logger,
	}
	// Build the unified method table.
	// tcpAllowed=true  -> available on both unix socket and TCP+mTLS.
	// tcpAllowed=false -> unix socket only (root-only, local operations).
	// Adding a method: one entry here, one Client method. That's it.
	s.methods = map[string]methodEntry{
		"Status":  {func(ctx context.Context) any { return s.buildStatus() }, true},
		"Pawns":   {func(ctx context.Context) any { return s.buildPawns() }, true},
		"Pods":    {func(ctx context.Context) any { return s.buildPods() }, true},
		"Top":     {func(ctx context.Context) any { return s.buildTop() }, true},
		"Doctor":  {func(ctx context.Context) any { return s.buildDoctor(ctx) }, true},
		"Images":  {func(ctx context.Context) any { return s.buildImages() }, true},
		"Version": {func(ctx context.Context) any { return s.buildVersion() }, true},
		// Mutating/local-only - not exposed over TCP.
		"Drain": {func(ctx context.Context) any { return s.buildDrain() }, false},
		"Stop":  {func(ctx context.Context) any { return s.buildStop(ctx) }, false},
	}
	return s
}

// SetTCPListener configures an optional TCP+mTLS listener for remote access.
// cert is the server certificate, caCert is the CA used to verify clients.
func (s *Server) SetTCPListener(addr string, cert *tls.Certificate, caCert *x509.Certificate) {
	s.tcpAddr = addr
	s.tlsCert = cert
	s.tlsCACert = caCert
}

func (s *Server) RegisterGambit(g *node.Gambit) {
	s.mu.Lock()
	s.gambits = append(s.gambits, g)
	s.mu.Unlock()
}

func (s *Server) RegisterQueues(pawnName string, qp QueueProvider) {
	s.mu.Lock()
	if s.queues == nil {
		s.queues = make(map[string]QueueProvider)
	}
	s.queues[pawnName] = qp
	s.mu.Unlock()
}

// SetImageLister registers an image cache provider for the Images method.
func (s *Server) SetImageLister(il ImageLister) {
	s.mu.Lock()
	s.imageLister = il
	s.mu.Unlock()
}

func (s *Server) AllPodUIDs() map[string]struct{} {
	s.mu.RLock()
	gambits := s.gambits
	s.mu.RUnlock()
	uids := make(map[string]struct{})
	for _, g := range gambits {
		for uid := range g.PodUIDs() {
			uids[uid] = struct{}{}
		}
	}
	return uids
}

func (s *Server) Start(ctx context.Context) error {
	_ = os.Remove(s.socketPath)

	iface := &managerIface{server: s}

	svc, err := varlink.NewService("perigeos", "Perigeos Manager", version.Version,
		"https://github.com/malformed-c/perigeos")
	if err != nil {
		return fmt.Errorf("varlink service: %w", err)
	}
	if err := svc.RegisterInterface(iface); err != nil {
		return fmt.Errorf("varlink register interface: %w", err)
	}
	s.varlinkSrv = svc

	if err := os.MkdirAll(filepath.Dir(s.socketPath), 0755); err != nil {
		s.logger.Warn("Could not create socket dir", "err", err)
	}

	// TCP + mTLS - optional remote access for Apogeos operator.
	if s.tcpAddr != "" && s.tlsCert != nil && s.tlsCACert != nil {
		go s.listenTCP(ctx)
	}

	s.logger.Info("Varlink socket listening", "path", s.socketPath)
	return svc.Listen(ctx, fmt.Sprintf("unix:%s", s.socketPath), 0)
}

// listenTCP starts a TCP+mTLS listener. Clients must present a certificate
// signed by the same CA that signed the server certificate. Each connection
// is handled by a dedicated varlink service instance.
func (s *Server) listenTCP(ctx context.Context) {
	pool := x509.NewCertPool()
	pool.AddCert(s.tlsCACert)

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{*s.tlsCert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}

	ln, err := tls.Listen("tcp", s.tcpAddr, tlsCfg)
	if err != nil {
		s.logger.Error("TCP Varlink listener failed", "addr", s.tcpAddr, "err", err)
		return
	}
	s.tcpLn = ln
	defer ln.Close()

	s.logger.Info("Varlink TCP+mTLS listening", "addr", s.tcpAddr)

	// Each connection gets a JSON request/response exchange using our
	// handler functions directly. The varlink wire protocol is newline-
	// delimited JSON, but since handleConnection is unexported in v0.4.0,
	// we speak the protocol ourselves.
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Debug("TCP listener closed", "err", err)
			return
		}
		go s.handleTCPConn(ctx, conn)
	}
}

// varlinkRequest is the wire format for a varlink method call.
type varlinkRequest struct {
	Method     string           `json:"method"`
	Parameters *json.RawMessage `json:"parameters,omitempty"`
}

// varlinkResponse is the wire format for a varlink reply.
type varlinkResponse struct {
	Parameters any    `json:"parameters,omitempty"`
	Error      string `json:"error,omitempty"`
}

// handleTCPConn speaks the varlink wire protocol over a single TCP+mTLS
// connection, dispatching to the same handler functions as the unix socket.
func (s *Server) handleTCPConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	enc := json.NewEncoder(conn)

	for scanner.Scan() {
		var req varlinkRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			_ = enc.Encode(varlinkResponse{Error: "org.varlink.service.InvalidParameter"})
			continue
		}

		// Strip interface prefix: "io.perigeos.Manager.Status" -> "Status"
		method := req.Method
		if idx := strings.LastIndex(method, "."); idx >= 0 {
			method = method[idx+1:]
		}

		result, vErr := s.dispatch(ctx, method)
		if vErr != "" {
			_ = enc.Encode(varlinkResponse{Error: vErr})
		} else {
			_ = enc.Encode(varlinkResponse{Parameters: result})
		}
	}
}

// dispatch is the TCP+mTLS path. It looks up the unified method table and
// enforces tcpAllowed - mutating/local methods are unix-socket-only.
func (s *Server) dispatch(ctx context.Context, method string) (any, string) {
	entry, ok := s.methods[method]
	if !ok {
		return nil, "org.varlink.service.MethodNotFound"
	}
	if !entry.tcpAllowed {
		return nil, "org.varlink.service.PermissionDenied"
	}
	return entry.handler(ctx), ""
}

func (s *Server) Stop(_ context.Context) error {
	if s.varlinkSrv != nil {
		_ = s.varlinkSrv.Shutdown()
	}
	if s.tcpLn != nil {
		_ = s.tcpLn.Close()
	}
	_ = os.Remove(s.socketPath)
	return nil
}

// -- Response builders (shared by varlink handlers and TCP dispatch) ---

func (s *Server) buildStatus() map[string]any {
	s.mu.RLock()
	gambits := s.gambits
	s.mu.RUnlock()
	var totalPods int
	for _, g := range gambits {
		totalPods += g.PodCount()
	}
	hostname, _ := os.Hostname()
	resp := StatusResponse{
		Hostname: hostname, UptimeSecs: int64(time.Since(s.startTime).Seconds()),
		PawnCount: len(gambits), PodCount: totalPods,
		Version: version.Version, GoVersion: runtime.Version(),
		Arch: runtime.GOARCH, OS: runtime.GOOS,
		Kernel: kernelVersion(), CPUCores: runtime.NumCPU(),
	}
	if t, u, err := readHostMemory(); err == nil {
		resp.MemTotalMiB, resp.MemUsedMiB = t, u
	}
	if avg, err := readLoadAvg(); err == nil {
		resp.LoadAvg = avg
	}
	if hp, err := psi.Read(); err == nil {
		resp.PSICPUSome = hp.CPU.Avg10
		resp.PSIMemFull = hp.Memory.Avg10
	}
	resp.Machines = countMachines(gambits)
	resp.DiskDirs = countDiskDirs(gambits)
	resp.SystemdUnits = countSystemdUnits(gambits)
	resp.PerigeosRSSMiB = readSelfRSS()
	resp.LxcVeths = countLxcVeths()
	resp.NetnsCount = countNetns()
	return toMap(resp)
}

func (s *Server) buildPawns() map[string]any {
	s.mu.RLock()
	gambits := s.gambits
	queues := s.queues
	s.mu.RUnlock()
	pawns := make([]any, 0, len(gambits))
	for _, g := range gambits {
		info := PawnInfo{
			Name: g.Config.Name, IsPrimary: g.Config.IsPrimary,
			Port: g.Config.Port, NodeIP: g.NodeIP(), PodCount: g.PodCount(),
		}
		if cpuNs, err := pawnstats.ReadSliceCPU(g.Config.Name); err == nil {
			info.CPUUsageMs = int64(cpuNs / 1_000_000)
		}
		if usage, _, err := pawnstats.ReadSliceMemory(g.Config.Name); err == nil {
			info.MemoryMiB = int64(usage / (1024 * 1024))
		}
		if qp, ok := queues[g.Config.Name]; ok {
			info.SyncQueueDepth = qp.SyncPodsFromKubernetesQueueLen()
			info.DeleteQueueDepth = qp.DeletePodsFromKubernetesQueueLen()
			info.StatusQueueDepth = qp.SyncPodStatusFromProviderQueueLen()
		}
		pawns = append(pawns, toMap(info))
	}
	return map[string]any{"pawns": pawns}
}

func (s *Server) buildPods() map[string]any {
	s.mu.RLock()
	gambits := s.gambits
	s.mu.RUnlock()
	var out []any
	for _, g := range gambits {
		for _, snap := range g.SnapshotPods() {
			out = append(out, toMap(PodInfo{
				Name: snap.Name, Namespace: snap.Namespace, UID: snap.UID,
				PawnName: g.Config.Name, PodIP: snap.IP,
				Phase: string(snap.Phase), Containers: snap.Containers,
			}))
		}
	}
	return map[string]any{"pods": out}
}

func (s *Server) buildTop() map[string]any {
	s.mu.RLock()
	gambits := s.gambits
	s.mu.RUnlock()
	resp := TopResponse{TimestampNs: time.Now().UnixNano()}
	if avg, err := readLoadAvg(); err == nil {
		resp.LoadAvg = avg
	}
	if t, u, err := readHostMemory(); err == nil {
		resp.MemTotalMiB, resp.MemUsedMiB = t, u
	}
	pawns := make([]any, 0, len(gambits))
	for _, g := range gambits {
		info := PawnTopInfo{Name: g.Config.Name, IsPrimary: g.Config.IsPrimary, PodCount: g.PodCount()}
		if cpuNs, err := pawnstats.ReadSliceCPU(g.Config.Name); err == nil {
			info.CPUUsageNs = cpuNs
		}
		if usage, ws, err := pawnstats.ReadSliceMemory(g.Config.Name); err == nil {
			info.MemoryBytes, info.MemoryWSBytes = usage, ws
		}
		pawns = append(pawns, toMap(info))
	}
	return map[string]any{
		"timestamp_ns":  resp.TimestampNs,
		"load_avg":      resp.LoadAvg,
		"mem_used_mib":  resp.MemUsedMiB,
		"mem_total_mib": resp.MemTotalMiB,
		"pawns":         pawns,
	}
}

func (s *Server) buildDoctor(ctx context.Context) map[string]any {
	s.mu.RLock()
	gambits := s.gambits
	s.mu.RUnlock()
	resp := DoctorResponse{Healthy: true}
	for _, g := range gambits {
		diag := s.diagnosePawn(ctx, g)
		if len(diag.GhostPods) > 0 || len(diag.OrphanMachines) > 0 ||
			len(diag.StaleDirs) > 0 || len(diag.MissingDirs) > 0 {
			resp.Healthy = false
		}
		resp.Summary.TotalGambit += diag.GambitPods
		resp.Summary.TotalSystemd += diag.SystemdUnits
		resp.Summary.TotalDisk += diag.DiskDirs
		resp.Summary.TotalGhosts += len(diag.GhostPods)
		resp.Summary.TotalOrphans += len(diag.OrphanMachines)
		resp.Summary.TotalStaleDirs += len(diag.StaleDirs)
		resp.Summary.TotalStaleUnits += diag.StaleUnits
		if diag.SliceActive {
			resp.Summary.ActiveSlices++
		}
		resp.Pawns = append(resp.Pawns, diag)
	}
	// Count stale slices: cgroup dirs that exist but have no active gambit.
	activePawns := make(map[string]struct{}, len(gambits))
	for _, g := range gambits {
		activePawns[g.Config.Name] = struct{}{}
	}
	resp.Summary.StaleSlices = countStaleSlices(activePawns)
	resp.Summary.LxcVeths = countLxcVeths()
	resp.Summary.NetnsCount = countNetns()
	pawns := make([]any, 0, len(resp.Pawns))
	for _, p := range resp.Pawns {
		pawns = append(pawns, toMap(p))
	}
	return map[string]any{
		"healthy": resp.Healthy,
		"pawns":   pawns,
		"summary": toMap(resp.Summary),
	}
}

func (s *Server) buildImages() any {
	s.mu.RLock()
	il := s.imageLister
	s.mu.RUnlock()
	if il == nil {
		return map[string]any{"images": []any{}, "cache_dir": ""}
	}
	return map[string]any{
		"images":    il.ListCachedImagesJSON(),
		"cache_dir": il.GetLayerCachePath(),
	}
}

func (s *Server) buildDrain() string {
	s.mu.RLock()
	gambits := s.gambits
	s.mu.RUnlock()
	for _, g := range gambits {
		g.Shutdown() // marks node NotReady+Unschedulable, stops new scheduling
	}
	return "Drain initiated: nodes marked NotReady. Pods will be evicted by the scheduler."
}

// buildStop performs an active drain: marks every pawn NotReady+Unschedulable
// AND stops all running pods on each pawn before returning. Used by `apsis stop`
// to drain a host before invoking systemctl stop perigeos.
func (s *Server) buildStop(ctx context.Context) string {
	s.mu.RLock()
	gambits := s.gambits
	s.mu.RUnlock()
	for _, g := range gambits {
		g.Shutdown()
	}
	for _, g := range gambits {
		g.DrainPods(ctx)
	}
	return fmt.Sprintf("Stop drain complete: %d pawn(s) drained, pods stopped.", len(gambits))
}

func (s *Server) buildVersion() map[string]any {
	return toMap(VersionResponse{
		Version: version.Version, GoVersion: runtime.Version(),
		Arch: runtime.GOARCH, OS: runtime.GOOS, GitCommit: version.GitCommit,
	})
}

// toMap converts any struct to map[string]any via JSON round-trip.
func toMap(v any) map[string]any {
	b, _ := json.Marshal(v)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}

// -- Host info helpers ---

func kernelVersion() string {
	var buf syscall.Utsname
	if err := syscall.Uname(&buf); err != nil {
		return ""
	}
	b := make([]byte, 0, len(buf.Release))
	for _, c := range buf.Release {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	return string(b)
}

func readHostMemory() (totalMiB, usedMiB int64, err error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var memTotal, memAvailable uint64
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		val, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			memTotal = val
		case "MemAvailable:":
			memAvailable = val
		}
	}
	if memTotal == 0 {
		return 0, 0, fmt.Errorf("could not parse /proc/meminfo")
	}
	return int64(memTotal / 1024), int64((memTotal - memAvailable) / 1024), nil
}

func readLoadAvg() (string, error) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "", err
	}
	if fields := strings.Fields(string(data)); len(fields) >= 3 {
		return strings.Join(fields[:3], " "), nil
	}
	return "", fmt.Errorf("unexpected loadavg format")
}

func countMachines(gambits []*node.Gambit) int {
	ctx := context.Background()
	total := 0
	for _, g := range gambits {
		if machines, err := g.Runtime.ListManagedMachines(ctx); err == nil {
			total += len(machines)
		}
	}
	return total
}

func countDiskDirs(gambits []*node.Gambit) int {
	total := 0
	for _, g := range gambits {
		total += len(scanDiskPods(g.Config.BaseDir, g.Config.Name))
	}
	return total
}

func countSystemdUnits(gambits []*node.Gambit) int {
	ctx := context.Background()
	seen := make(map[string]struct{})
	for _, g := range gambits {
		if machines, err := g.Runtime.ListManagedMachines(ctx); err == nil {
			for _, m := range machines {
				if m.UID != "" {
					seen[m.UID+"-"+m.ContainerName] = struct{}{}
				}
			}
		}
	}
	return len(seen)
}

func readSelfRSS() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	rssPages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return rssPages * int64(os.Getpagesize()) / (1024 * 1024)
}

func countLxcVeths() int {
	entries, err := os.ReadDir("/sys/class/net")
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "lxc") {
			count++
		}
	}
	return count
}

func countNetns() int {
	entries, err := os.ReadDir("/var/run/netns")
	if err != nil {
		return 0
	}
	return len(entries)
}

// countStaleSlices counts perigeos pawn slice cgroup dirs that have no active gambit.
func countStaleSlices(activePawns map[string]struct{}) int {
	// Pawn slices live under /sys/fs/cgroup/perigeos.slice/ as perigeos-<name>.slice
	entries, err := os.ReadDir("/sys/fs/cgroup/perigeos.slice")
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || !strings.HasPrefix(name, "perigeos-") || !strings.HasSuffix(name, ".slice") {
			continue
		}
		// Extract pawn name: perigeos-compute-00.slice -> compute-00
		pawn := strings.TrimPrefix(name, "perigeos-")
		pawn = strings.TrimSuffix(pawn, ".slice")
		// Skip parent slices (e.g. "compute" which is parent of "compute-00")
		if _, ok := activePawns[pawn]; ok {
			continue
		}
		isParent := false
		for ap := range activePawns {
			if strings.HasPrefix(ap, pawn+"-") {
				isParent = true
				break
			}
		}
		if !isParent {
			count++
		}
	}
	return count
}

func (s *Server) diagnosePawn(ctx context.Context, g *node.Gambit) PawnDiagnosis {
	diag := PawnDiagnosis{Name: g.Config.Name}
	gambitUIDs := g.PodUIDs()
	diag.GambitPods = len(gambitUIDs)
	type unitInfo struct {
		name  string
		state perigeos.MachineState
	}
	systemdUIDs := make(map[string]unitInfo)
	if machines, err := g.Runtime.ListManagedMachines(ctx); err != nil {
		s.logger.Error("Doctor: failed to list machines", "pawn", g.Config.Name, "err", err)
	} else {
		for _, m := range machines {
			if m.UID != "" {
				systemdUIDs[m.UID] = unitInfo{name: m.Name, state: m.State}
			}
		}
	}
	diag.SystemdUnits = len(systemdUIDs)
	diskUIDs := scanDiskPods(g.Config.BaseDir, g.Config.Name)
	diag.DiskDirs = len(diskUIDs)
	diag.SliceActive = g.Runtime.SliceActive(ctx)

	for uid, name := range gambitUIDs {
		if _, ok := systemdUIDs[uid]; !ok {
			diag.GhostPods = append(diag.GhostPods, DoctorEntry{UID: uid, Name: name})
		}
	}
	for uid, info := range systemdUIDs {
		if _, ok := gambitUIDs[uid]; !ok {
			diag.OrphanMachines = append(diag.OrphanMachines, DoctorEntry{UID: uid, Name: info.name})
		}
		// Count dead/failed units not tracked by gambit - leftovers from crash/restart.
		if info.state == perigeos.StateExited || info.state == perigeos.StateFailed {
			if _, ok := gambitUIDs[uid]; !ok {
				diag.StaleUnits++
			}
		}
	}
	for _, uid := range diskUIDs {
		if _, ok := gambitUIDs[uid]; !ok {
			diag.StaleDirs = append(diag.StaleDirs, uid)
		}
	}
	for uid, name := range gambitUIDs {
		if !slices.Contains(diskUIDs, uid) {
			diag.MissingDirs = append(diag.MissingDirs, DoctorEntry{UID: uid, Name: name})
		}
	}
	return diag
}

func scanDiskPods(baseDir, pawnName string) []string {
	podsDir := filepath.Join(baseDir, "pawns", pawnName, "pods")
	entries, err := os.ReadDir(podsDir)
	if err != nil {
		return nil
	}
	seen := make(map[string]struct{})
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		uid := name
		if len(name) > 36 && name[36] == '-' {
			uid = name[:36]
		}
		seen[uid] = struct{}{}
	}
	uids := make([]string, 0, len(seen))
	for uid := range seen {
		uids = append(uids, uid)
	}
	return uids
}

// -- Client ---

const DefaultSocketPath = "/run/apsis/perigeos.sock"

type Client struct {
	address string
	tlsCfg  *tls.Config
}

func NewClient(socketPath string) *Client {
	return &Client{address: fmt.Sprintf("unix:%s", socketPath)}
}

// NewTCPClient creates a client connecting to a remote perigeos over TCP+mTLS.
func NewTCPClient(addr string, clientCert tls.Certificate, caCert *x509.Certificate) *Client {
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return &Client{
		address: fmt.Sprintf("tcp:%s", addr),
		tlsCfg: &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      pool,
			MinVersion:   tls.VersionTLS13,
		},
	}
}

func (c *Client) call(ctx context.Context, method string, out any) error {
	conn, err := varlink.NewConnection(ctx, c.address)
	if err != nil {
		return fmt.Errorf("varlink connect %s: %w", c.address, err)
	}
	defer conn.Close()

	if err := conn.Call(ctx, ifaceName+"."+method, nil, out); err != nil {
		return fmt.Errorf("varlink %s: %w", method, err)
	}
	return nil
}

func (c *Client) Status(ctx context.Context) (*StatusResponse, error) {
	var r StatusResponse
	return &r, c.call(ctx, "Status", &r)
}
func (c *Client) Pawns(ctx context.Context) (*PawnsResponse, error) {
	var r PawnsResponse
	return &r, c.call(ctx, "Pawns", &r)
}
func (c *Client) Pods(ctx context.Context) (*PodsResponse, error) {
	var r PodsResponse
	return &r, c.call(ctx, "Pods", &r)
}
func (c *Client) Top(ctx context.Context) (*TopResponse, error) {
	var r TopResponse
	return &r, c.call(ctx, "Top", &r)
}
func (c *Client) Doctor(ctx context.Context) (*DoctorResponse, error) {
	var r DoctorResponse
	return &r, c.call(ctx, "Doctor", &r)
}
func (c *Client) Version(ctx context.Context) (*VersionResponse, error) {
	var r VersionResponse
	return &r, c.call(ctx, "Version", &r)
}

// ImagesResponse is the response from the Images varlink method.
type ImagesResponse struct {
	CacheDir string           `json:"cache_dir"`
	Images   []ImageEntryResp `json:"images"`
}

// ImageEntryResp describes a single cached image.
type ImageEntryResp struct {
	Name      string `json:"name"`
	Digest    string `json:"digest"`
	Layers    int    `json:"layers"`
	SizeBytes int64  `json:"size_bytes"`
}

func (c *Client) Images(ctx context.Context) (*ImagesResponse, error) {
	var r ImagesResponse
	return &r, c.call(ctx, "Images", &r)
}

func (c *Client) Drain(ctx context.Context) (string, error) {
	var r string
	return r, c.call(ctx, "Drain", &r)
}

func (c *Client) Stop(ctx context.Context) (string, error) {
	var r string
	return r, c.call(ctx, "Stop", &r)
}

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
	pruntime "github.com/malformed-c/periapsis/internal/runtime"
	pawstats "github.com/malformed-c/periapsis/internal/stats"
	"github.com/malformed-c/periapsis/internal/version"
	"github.com/malformed-c/periapsis/node"
	"github.com/varlink/go/varlink"
)

const ifaceName = "io.perigeos.Manager"

// managerIface implements varlink's dispatcher interface for the perigeos
// manager methods. It dispatches method calls to the Server's handlers.
type managerIface struct {
	server *Server
}

func (m *managerIface) VarlinkGetName() string        { return ifaceName }
func (m *managerIface) VarlinkGetDescription() string  { return "" }

func (m *managerIface) VarlinkDispatch(ctx context.Context, c varlink.Call, methodname string) error {
	switch methodname {
	case "Status":
		return m.server.varlinkStatus(ctx, &c)
	case "Pawns":
		return m.server.varlinkPawns(ctx, &c)
	case "Pods":
		return m.server.varlinkPods(ctx, &c)
	case "Top":
		return m.server.varlinkTop(ctx, &c)
	case "Doctor":
		return m.server.varlinkDoctor(ctx, &c)
	case "Version":
		return m.server.varlinkVersion(ctx, &c)
	default:
		return c.ReplyMethodNotFound(ctx, methodname)
	}
}

// QueueProvider exposes work queue depths for a pawn's PodController.
type QueueProvider interface {
	SyncPodsFromKubernetesQueueLen() int
	DeletePodsFromKubernetesQueueLen() int
	SyncPodStatusFromProviderQueueLen() int
}

// Server exposes a Varlink socket for the apsis CLI and remote control.
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
	queues  map[string]QueueProvider // pawn name → PodController

	snapMu   sync.RWMutex
	snapPods []PodInfo

	varlinkSrv *varlink.Service
	tcpLn      net.Listener
}

func New(socketPath string, cfg *config.PerigeosConfig, logger *slog.Logger) *Server {
	return &Server{
		socketPath: socketPath,
		startTime:  time.Now(),
		config:     cfg,
		logger:     logger,
	}
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

	// TCP + mTLS — optional remote access for Apogeos operator.
	if s.tcpAddr != "" && s.tlsCert != nil && s.tlsCACert != nil {
		go s.listenTCP(ctx)
	}

	go s.refreshLoop()
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

		// Strip interface prefix: "io.perigeos.Manager.Status" → "Status"
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

// dispatch calls the appropriate handler and returns the result map.
func (s *Server) dispatch(ctx context.Context, method string) (any, string) {
	switch method {
	case "Status":
		return s.buildStatus(), ""
	case "Pawns":
		return s.buildPawns(), ""
	case "Pods":
		return s.buildPods(), ""
	case "Top":
		return s.buildTop(), ""
	case "Doctor":
		return s.buildDoctor(ctx), ""
	case "Version":
		return s.buildVersion(), ""
	default:
		return nil, "org.varlink.service.MethodNotFound"
	}
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

func (s *Server) refreshLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.RLock()
		gambits := s.gambits
		s.mu.RUnlock()
		if len(gambits) == 0 {
			continue
		}
		var pods []PodInfo
		for _, g := range gambits {
			snaps := g.SnapshotPods()
			for _, snap := range snaps {
				pods = append(pods, PodInfo{
					Name: snap.Name, Namespace: snap.Namespace, UID: snap.UID,
					PawnName: g.Config.Name, PodIP: snap.IP,
					Phase: string(snap.Phase), Containers: snap.Containers,
				})
			}
		}
		s.snapMu.Lock()
		s.snapPods = pods
		s.snapMu.Unlock()
	}
}

// ── Response builders (shared by varlink handlers and TCP dispatch) ───────────

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
		if cpuNs, err := pawstats.ReadSliceCPU(g.Config.Name); err == nil {
			info.CPUUsageMs = int64(cpuNs / 1_000_000)
		}
		if usage, _, err := pawstats.ReadSliceMemory(g.Config.Name); err == nil {
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
	s.snapMu.RLock()
	pods := s.snapPods
	s.snapMu.RUnlock()
	out := make([]any, 0, len(pods))
	for _, p := range pods {
		out = append(out, toMap(p))
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
		if cpuNs, err := pawstats.ReadSliceCPU(g.Config.Name); err == nil {
			info.CPUUsageNs = cpuNs
		}
		if usage, ws, err := pawstats.ReadSliceMemory(g.Config.Name); err == nil {
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
		resp.Pawns = append(resp.Pawns, diag)
	}
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

func (s *Server) buildVersion() map[string]any {
	return toMap(VersionResponse{
		Version: version.Version, GoVersion: runtime.Version(),
		Arch: runtime.GOARCH, OS: runtime.GOOS, GitCommit: version.GitCommit,
	})
}

// ── Varlink handlers (thin wrappers that reply with built responses) ─────────

func (s *Server) varlinkStatus(ctx context.Context, c *varlink.Call) error {
	return c.Reply(ctx, s.buildStatus())
}

func (s *Server) varlinkPawns(ctx context.Context, c *varlink.Call) error {
	return c.Reply(ctx, s.buildPawns())
}

func (s *Server) varlinkPods(ctx context.Context, c *varlink.Call) error {
	return c.Reply(ctx, s.buildPods())
}

func (s *Server) varlinkTop(ctx context.Context, c *varlink.Call) error {
	return c.Reply(ctx, s.buildTop())
}

func (s *Server) varlinkDoctor(ctx context.Context, c *varlink.Call) error {
	return c.Reply(ctx, s.buildDoctor(ctx))
}

func (s *Server) varlinkVersion(ctx context.Context, c *varlink.Call) error {
	return c.Reply(ctx, s.buildVersion())
}

// toMap converts any struct to map[string]any via JSON round-trip.
func toMap(v any) map[string]any {
	b, _ := json.Marshal(v)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}

// ── Host info helpers ─────────────────────────────────────────────────────────

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

func (s *Server) diagnosePawn(ctx context.Context, g *node.Gambit) PawnDiagnosis {
	diag := PawnDiagnosis{Name: g.Config.Name}
	gambitUIDs := g.PodUIDs()
	diag.GambitPods = len(gambitUIDs)
	type unitInfo struct {
		name  string
		state pruntime.MachineState
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
	for uid, name := range gambitUIDs {
		if _, ok := systemdUIDs[uid]; !ok {
			diag.GhostPods = append(diag.GhostPods, DoctorEntry{UID: uid, Name: name})
		}
	}
	for uid, info := range systemdUIDs {
		if _, ok := gambitUIDs[uid]; !ok {
			diag.OrphanMachines = append(diag.OrphanMachines, DoctorEntry{UID: uid, Name: info.name})
		}
		// Count dead/failed units not tracked by gambit — leftovers from crash/restart.
		if info.state == pruntime.StateExited || info.state == pruntime.StateFailed {
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

// ── Client ────────────────────────────────────────────────────────────────────

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

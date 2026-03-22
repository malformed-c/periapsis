package systemd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/coreos/go-systemd/v22/sdjournal"
	dbusv5 "github.com/godbus/dbus/v5"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/node/api"
	"golang.org/x/sys/unix"
)

// SystemdRuntime implements runtime.Runtime by managing transient systemd-nspawn services.
type SystemdRuntime struct {
	conn    *dbus.Conn      // go-systemd wrapper (unit lifecycle)
	rawConn *dbusv5.Conn    // raw godbus (machine1 interface, unit object properties)
	logger       *slog.Logger
	imageManager *image.ImageManager

	pawnName     string
	slice        string
	execStrategy runtime.ExecStrategy

	// attachPTYs holds PTY master fds for containers started with stdin=true.
	// Key: machineName ("pod-<uid>-<container>"), Value: *os.File (master).
	// The slave side is passed to nspawn via StandardInput/Output, and the
	// master is used by AttachToContainer to relay stdin/stdout.
	attachPTYs sync.Map
}

// Ensure compile-time interface compliance.
var _ runtime.Runtime = (*SystemdRuntime)(nil)

// Close releases the dbus connections held by the runtime.
func (s *SystemdRuntime) Close() {
	s.conn.Close()
	s.rawConn.Close()
}

func NewSystemdRuntime(
	ctx context.Context,
	pawnName string,
	im *image.ImageManager,
	logger *slog.Logger,
	execStrategy runtime.ExecStrategy,
) (*SystemdRuntime, error) {
	conn, err := dbus.NewSystemConnectionContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to system dbus: %w", err)
	}

	rawConn, err := dbusv5.ConnectSystemBus()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open raw dbus connection: %w", err)
	}

	return &SystemdRuntime{
		conn:         conn,
		rawConn:      rawConn,
		imageManager: im,
		logger:       logger,
		pawnName:     pawnName,
		slice:        sliceName(pawnName),
		execStrategy: execStrategy,
	}, nil
}

// RunMachine creates a transient systemd service running systemd-nspawn.
// Pod metadata is embedded as custom unit properties (X-Perigeos*) so it
// survives in systemd and can be recovered on restart without a separate DB.
func (s *SystemdRuntime) RunMachine(ctx context.Context, podUID string, cfg runtime.PodConfig) error {
	containerName := cfg.ContainerName
	if containerName == "" {
		containerName = cfg.Container.Name
	}

	serviceName := wrapperUnitName(s.pawnName, podUID, containerName)
	machineName := "pod-" + podUID + "-" + containerName
	slice := sliceName(s.pawnName)

	// Determine the network namespace to join.
	// HostNetwork pods share the host's netns via /proc/1/ns/net.
	// All other pods require an explicit CNI-allocated netns path.
	netNSPath := cfg.NetNSPath
	if cfg.HostNetwork {
		if netNSPath == "" {
			netNSPath = "/proc/1/ns/net"
		}
	} else if netNSPath == "" {
		return fmt.Errorf("NetNSPath is required for machine %s", podUID)
	}

	// HostPID pods cannot run inside nspawn (no PID-namespace-sharing flag exists).
	// Route to runProgram which uses a plain systemd service with RootDirectory=.
	if cfg.HostPID {
		return s.runProgram(ctx, podUID, cfg)
	}

	s.logger.Info("Starting Machine", "service", serviceName, "slice", slice, "netns", netNSPath)

	execStart := []string{
		"/usr/bin/systemd-nspawn",
		"--console=pipe",
		"--keep-unit",
		"--register=yes",
		"--machine=" + machineName,
		// --slice= is NOT passed here: --keep-unit tells nspawn to stay in the
		// calling unit's cgroup rather than creating a new scope, so nspawn
		// ignores --slice= (and warns about it). The slice is already set on
		// the transient unit itself via dbus.PropSlice — that's what matters.
		"--directory=" + cfg.RootFS,
		"--network-namespace-path=" + netNSPath,
		// Do not let nspawn bind-mount the host /etc/resolv.conf into the
		// container — we write our own cluster-DNS resolv.conf into the
		// overlayfs before starting, and nspawn's default would overwrite it.
		"--resolv-conf=off",
	}

	// Privileged containers get all capabilities — required for workloads
	// that load BPF programs, manipulate network interfaces, etc.
	if cfg.Privileged {
		execStart = append(execStart, "--capability=all")
	}

	// Pass resolved env vars into the container via --setenv
	for _, envVar := range cfg.Environment {
		execStart = append(execStart, "--setenv="+envVar)
	}
	execStart = append(execStart,
		"--setenv=PERIGEOS_PAWN="+s.pawnName,
		"--setenv=PERIGEOS_UID="+podUID,
	)

	// Bind mounts: resolved from pod Volumes + container VolumeMounts.
	// --bind=<host>:<container> for rw, --bind-ro=<host>:<container> for ro.
	// Bidirectional propagation uses +<path> suffix (shared); default is slave.
	for _, bm := range cfg.BindMounts {
		arg := bm.HostPath + ":" + bm.ContainerPath
		if bm.Propagation == "Bidirectional" {
			// nspawn +path means MS_SHARED (bidirectional)
			arg = "+" + arg
		}
		if bm.ReadOnly {
			execStart = append(execStart, "--bind-ro="+arg)
		} else {
			execStart = append(execStart, "--bind="+arg)
		}
	}

	execStart = append(execStart, "--")

	// Build env map for Kubernetes-style $(VAR_NAME) substitution in args/command.
	envMap := make(map[string]string, len(cfg.Environment))
	for _, kv := range cfg.Environment {
		if idx := strings.IndexByte(kv, '='); idx >= 0 {
			envMap[kv[:idx]] = kv[idx+1:]
		}
	}

	// Kubernetes command resolution (mirrors kubelet behaviour):
	//   Container.Command overrides image ENTRYPOINT; Container.Args overrides image CMD.
	//   If both are unset, fall back to the image's Entrypoint+Cmd.
	effectiveEntrypoint := cfg.Container.Command
	if len(effectiveEntrypoint) == 0 {
		effectiveEntrypoint = cfg.ImageEntrypoint
	}
	var effectiveCmd []string
	if len(cfg.Container.Args) > 0 {
		effectiveCmd = cfg.Container.Args
	} else if len(cfg.Container.Command) == 0 {
		// Only use image CMD when neither command nor args are overridden.
		effectiveCmd = cfg.ImageCmd
	}
	var fullCmd []string
	for _, part := range append(effectiveEntrypoint, effectiveCmd...) {
		fullCmd = append(fullCmd, substituteEnvVars(part, envMap))
	}
	if len(fullCmd) == 0 {
		// Last resort for images with no entrypoint (e.g. scratch).
		fullCmd = []string{"/bin/sleep", "infinity"}
	}
	execStart = append(execStart, fullCmd...)

	// Embed pod metadata as PERIGEOS_META_* environment variables.
	// These are stored in the unit's Environment property and survive in
	// systemd's runtime state, so ListManagedMachines can recover them on
	// restart without a separate database. We use Environment rather than
	// X-* custom properties because X-* requires systemd ≥ 256.
	metaEnv := []string{
		"PERIGEOS_META_UID=" + podUID,
		"PERIGEOS_META_NAME=" + cfg.Name,
		"PERIGEOS_META_NAMESPACE=" + cfg.Namespace,
		"PERIGEOS_META_NODENAME=" + cfg.PawnName,
		"PERIGEOS_META_IP=" + cfg.PodIP,
		"PERIGEOS_META_CONTAINER=" + containerName,
	}

	properties := []dbus.Property{
		dbus.PropDescription("Pod " + podUID),
		dbus.PropSlice(slice),
		dbus.PropExecStart(execStart, false),
		{Name: "SyslogIdentifier", Value: dbusv5.MakeVariant(cfg.Container.Name)},
		{Name: "CollectMode", Value: dbusv5.MakeVariant("inactive-or-failed")},
		{Name: "Delegate", Value: dbusv5.MakeVariant(true)},
		{Name: "KillMode", Value: dbusv5.MakeVariant("mixed")},
		{Name: "Environment", Value: dbusv5.MakeVariant(metaEnv)},
	}

	// When the container has stdin enabled (e.g. kubectl run --attach --stdin),
	// create a PTY pair. The slave becomes nspawn's stdin/stdout so the
	// container's PID 1 can read/write through it. The master is stored for
	// AttachToContainer to relay the attach streams.
	if cfg.Container != nil && cfg.Container.Stdin {
		master, slave, err := openPTY()
		if err != nil {
			return fmt.Errorf("create attach PTY for %s: %w", machineName, err)
		}
		// Set raw mode so the PTY doesn't echo input or do line buffering.
		if termios, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS); err == nil {
			termios.Iflag &^= unix.IGNBRK | unix.BRKINT | unix.PARMRK | unix.ISTRIP | unix.INLCR | unix.IGNCR | unix.ICRNL | unix.IXON
			termios.Oflag &^= unix.OPOST
			termios.Lflag &^= unix.ECHO | unix.ECHONL | unix.ICANON | unix.ISIG | unix.IEXTEN
			termios.Cflag &^= unix.CSIZE | unix.PARENB
			termios.Cflag |= unix.CS8
			_ = unix.IoctlSetTermios(int(slave.Fd()), unix.TCSETS, termios)
		}
		slavePath := slave.Name()
		slave.Close() // systemd will reopen the slave path directly

		s.attachPTYs.Store(machineName, master)
		properties = append(properties,
			dbus.Property{Name: "StandardInputFile", Value: dbusv5.MakeVariant(slavePath)},
			dbus.Property{Name: "StandardOutputFile", Value: dbusv5.MakeVariant(slavePath)},
			dbus.Property{Name: "StandardErrorFile", Value: dbusv5.MakeVariant(slavePath)},
		)
		s.logger.Info("Created attach PTY", "machine", machineName, "slave", slavePath)
	}

	// Per-container resource limits from pod spec Resources.Limits.
	if cfg.MemoryLimitBytes > 0 {
		properties = append(properties, dbus.Property{
			Name: "MemoryMax", Value: dbusv5.MakeVariant(cfg.MemoryLimitBytes),
		})
	}
	if cfg.CPULimitMillis > 0 {
		properties = append(properties, dbus.Property{
			Name: "CPUQuotaPerSecUSec", Value: dbusv5.MakeVariant(uint64(cfg.CPULimitMillis * 1000)),
		})
	}

	ch := make(chan string, 1)
	if _, err := s.conn.StartTransientUnitContext(ctx, serviceName, "replace", properties, ch); err != nil {
		// Clean up PTY on failure.
		if masterVal, ok := s.attachPTYs.LoadAndDelete(machineName); ok {
			masterVal.(*os.File).Close()
		}
		return fmt.Errorf("failed to create machine unit: %w", err)
	}

	if res := <-ch; res != "done" {
		if masterVal, ok := s.attachPTYs.LoadAndDelete(machineName); ok {
			masterVal.(*os.File).Close()
		}
		return fmt.Errorf("start machine job failed: %s", res)
	}

	return nil
}

// StopMachine stops the wrapper service with a context-respecting timeout.
func (s *SystemdRuntime) StopMachine(ctx context.Context, podUID, containerName string) error {
	s.logger.Info("Stopping Machine", "pod", podUID, "container", containerName)
	wrapperUnit := wrapperUnitName(s.pawnName, podUID, containerName)

	// Clean up any attach PTY for this container.
	machineName := "pod-" + podUID + "-" + containerName
	if masterVal, ok := s.attachPTYs.LoadAndDelete(machineName); ok {
		masterVal.(*os.File).Close()
	}


	ch := make(chan string, 1)
	_, err := s.conn.StopUnitContext(ctx, wrapperUnit, "replace", ch)
	if err != nil {
		if strings.Contains(err.Error(), "not loaded") {
			return nil
		}
		return fmt.Errorf("stop unit: %w", err)
	}

	select {
	case status := <-ch:
		if status != "done" {
			if err := s.conn.ResetFailedUnitContext(ctx, wrapperUnit); err != nil {
				s.logger.Debug("ResetFailedUnit ignored", "err", err)
			}
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}


// CheckMachined verifies systemd-machined is healthy by calling ListMachines
// over D-Bus. If machined has exhausted its file descriptor limit or its
// socket is down, this call will fail — letting callers back off early instead
// of starting an nspawn that will immediately fail to register.
func (s *SystemdRuntime) CheckMachined(ctx context.Context) error {
	obj := s.rawConn.Object("org.freedesktop.machine1", "/org/freedesktop/machine1")
	call := obj.CallWithContext(ctx, "org.freedesktop.machine1.Manager.ListMachines", 0)
	if call.Err != nil {
		return fmt.Errorf("systemd-machined health check failed: %w (is LimitNOFILE too low?)", call.Err)
	}
	return nil
}

// MachineStatus queries the unit's ActiveState via dbus.
func (s *SystemdRuntime) MachineStatus(ctx context.Context, podUID, containerName string) (runtime.MachineState, error) {
	serviceName := wrapperUnitName(s.pawnName, podUID, containerName)

	prop, err := s.conn.GetUnitPropertyContext(ctx, serviceName, "ActiveState")
	if err != nil {
		return runtime.StateExited, nil
	}

	state := strings.Trim(prop.Value.String(), "\"")

	switch state {
	case "active", "reloading":
		return runtime.StateRunning, nil
	case "activating":
		return runtime.StateCreating, nil
	case "failed":
		return runtime.StateFailed, nil
	default:
		return runtime.StateExited, nil
	}
}

// ListManagedMachines returns all active machines for this pawn.
// Pod metadata is recovered from PERIGEOS_META_* environment variables
// embedded in the unit at RunMachine time.
func (s *SystemdRuntime) ListManagedMachines(ctx context.Context) ([]runtime.PodMetadata, error) {
	pattern := fmt.Sprintf("perigeos-%s-pod-*.service", s.pawnName)
	prefix := fmt.Sprintf("perigeos-%s-pod-", s.pawnName)

	units, err := s.conn.ListUnitsByPatternsContext(ctx, nil, []string{pattern})
	if err != nil {
		return nil, fmt.Errorf("failed to list units: %w", err)
	}

	var machines []runtime.PodMetadata

	for _, unit := range units {
		state := mapActiveState(unit.ActiveState)

		env := s.readUnitEnv(ctx, unit.Name)

		uid := env["PERIGEOS_META_UID"]
		if uid == "" {
			// Fallback: parse from unit name for legacy units
			uid = strings.TrimSuffix(strings.TrimPrefix(unit.Name, prefix), ".service")
		}

		machines = append(machines, runtime.PodMetadata{
			UID:           uid,
			Name:          env["PERIGEOS_META_NAME"],
			Namespace:     env["PERIGEOS_META_NAMESPACE"],
			NodeName:      env["PERIGEOS_META_NODENAME"],
			PodIP:         env["PERIGEOS_META_IP"],
			ContainerName: env["PERIGEOS_META_CONTAINER"],
			StartedAt:     s.readUnitStartTime(ctx, unit.Name),
			State:         state,
		})
	}

	return machines, nil
}

// readUnitEnv reads the Environment property of a unit and returns it as a
// key=value map. Only PERIGEOS_META_* entries are guaranteed to be present.
func (s *SystemdRuntime) readUnitEnv(ctx context.Context, unitName string) map[string]string {
	result := make(map[string]string)

	prop, err := s.conn.GetServicePropertyContext(ctx, unitName, "Environment")
	if err != nil {
		return result
	}

	// Environment is []string{"KEY=VALUE", ...}
	entries, ok := prop.Value.Value().([]string)
	if !ok {
		return result
	}

	for _, entry := range entries {
		k, v, ok := strings.Cut(entry, "=")
		if ok {
			result[k] = v
		}
	}
	return result
}

// readUnitStartTime returns the time the unit entered the active state by
// reading the ActiveEnterTimestamp D-Bus property (microseconds since epoch).
// Returns zero time if the property is unavailable.
func (s *SystemdRuntime) readUnitStartTime(ctx context.Context, unitName string) time.Time {
	prop, err := s.conn.GetUnitPropertyContext(ctx, unitName, "ActiveEnterTimestamp")
	if err != nil {
		return time.Time{}
	}
	usec, ok := prop.Value.Value().(uint64)
	if !ok || usec == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(usec)*int64(time.Microsecond))
}

// GetLogStream returns a journald log stream for the given pod unit.
// It uses the sdjournal API directly rather than shelling out to journalctl.
func (s *SystemdRuntime) GetLogStream(
	ctx context.Context,
	podUID, containerName string,
	opts api.ContainerLogOpts,
) (io.ReadCloser, error) {
	unitName := wrapperUnitName(s.pawnName, podUID, containerName)

	j, err := sdjournal.NewJournal()
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}

	if opts.Previous {
		prevID := s.loadPreviousInvocationID(j, podUID, containerName)
		if prevID == "" {
			j.Close()
			return nil, fmt.Errorf("no previous logs available for pod %s container %s", podUID, containerName)
		}
		if err := j.AddMatch("_SYSTEMD_INVOCATION_ID" + "=" + prevID); err != nil {
			j.Close()
			return nil, fmt.Errorf("journal match invocation: %w", err)
		}
	} else {
		if err := j.AddMatch("_SYSTEMD_UNIT" + "=" + unitName); err != nil {
			j.Close()
			return nil, fmt.Errorf("journal match unit: %w", err)
		}
	}
	if err := j.AddMatch("SYSLOG_IDENTIFIER" + "=" + containerName); err != nil {
		j.Close()
		return nil, fmt.Errorf("journal match syslog id: %w", err)
	}

	if opts.SinceSeconds > 0 {
		cutoff := uint64(time.Now().Add(-time.Duration(opts.SinceSeconds)*time.Second).UnixMicro())
		if err := j.SeekRealtimeUsec(cutoff); err != nil {
			j.Close()
			return nil, fmt.Errorf("journal seek: %w", err)
		}
	} else if opts.Tail > 0 {
		// Seek to tail then step back opts.Tail entries.
		if err := j.SeekTail(); err != nil {
			j.Close()
			return nil, fmt.Errorf("journal seek tail: %w", err)
		}
		if _, err := j.PreviousSkip(uint64(opts.Tail)); err != nil {
			j.Close()
			return nil, fmt.Errorf("journal seek back: %w", err)
		}
	} else {
		if err := j.SeekHead(); err != nil {
			j.Close()
			return nil, fmt.Errorf("journal seek head: %w", err)
		}
	}

	return &journalReader{j: j, ctx: ctx, follow: opts.Follow}, nil
}

// journalReader implements io.ReadCloser over an sdjournal.Journal.
// Each Read call drains buffered bytes from the last entry, then fetches
// the next one. In follow mode it blocks on j.Wait instead of returning EOF.
type journalReader struct {
	j      *sdjournal.Journal
	buf    []byte
	ctx    context.Context
	follow bool
}

func (r *journalReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		n, err := r.j.Next()
		if err != nil {
			return 0, fmt.Errorf("journal next: %w", err)
		}
		if n == 0 {
			if !r.follow {
				return 0, io.EOF
			}
			// Block until new entries arrive or context is cancelled.
			for {
				select {
				case <-r.ctx.Done():
					return 0, r.ctx.Err()
				default:
				}
				status := r.j.Wait(250 * time.Millisecond)
				if status == sdjournal.SD_JOURNAL_APPEND {
					break
				}
				if status < 0 {
					return 0, fmt.Errorf("journal wait error: %d", status)
				}
			}
			continue
		}
		entry, err := r.j.GetEntry()
		if err != nil {
			return 0, fmt.Errorf("journal entry: %w", err)
		}
		r.buf = append([]byte(entry.Fields["MESSAGE"]), '\n')
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *journalReader) Close() error {
	return r.j.Close()
}

// RunInContainer executes a command inside a running container.
func (s *SystemdRuntime) RunInContainer(
	ctx context.Context,
	podUID, containerName string,
	cmd []string,
	attach api.AttachIO,
) error {
	switch s.execStrategy {
	case runtime.ExecMachinectl:
		return s.runInContainerMachinectl(ctx, podUID, containerName, cmd, attach)
	default:
		return s.runInContainerNsenter(ctx, podUID, containerName, cmd, attach)
	}
}

// AttachToContainer attaches stdin/stdout/stderr to the running container's
// PID 1. If the container was started with stdin=true, a PTY master was
// allocated at startup; we relay the attach streams through it. Otherwise
// we fall back to nsenter with an interactive shell.
func (s *SystemdRuntime) AttachToContainer(
	ctx context.Context,
	podUID, containerName string,
	attach api.AttachIO,
) error {
	machineName := "pod-" + podUID + "-" + containerName

	// Fast path: relay through the PTY master that was created at container
	// start for stdin-enabled containers.
	if masterVal, ok := s.attachPTYs.Load(machineName); ok {
		return s.relayAttachPTY(ctx, masterVal.(*os.File), attach)
	}

	// Fallback: nsenter into the container and start an interactive shell.
	pid, err := s.getMachineLeaderPID(machineName)
	if err != nil {
		return fmt.Errorf("could not find leader PID for machine %s: %w", machineName, err)
	}

	args := []string{
		fmt.Sprintf("--target=%d", pid),
		"--mount", "--uts", "--ipc", "--net", "--pid", "--cgroup",
		"--root=/proc/" + strconv.Itoa(pid) + "/root",
		"--wdns=/",
		"--",
		"/usr/bin/env", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"/bin/sh", "-l", "-i",
	}

	cmd := exec.CommandContext(ctx, "nsenter", args...)
	var runErr error
	if attach.TTY() {
		runErr = runWithPTY(ctx, cmd, attach)
	} else {
		wireAttach(cmd, attach)
		runErr = cmd.Run()
	}
	return suppressSignalExit(runErr)
}

// relayAttachPTY relays the attach streams through the PTY master that was
// allocated when the container started. Writes to attach.Stdin go to the
// container's stdin; reads from the container's stdout come back on
// attach.Stdout.
func (s *SystemdRuntime) relayAttachPTY(ctx context.Context, master *os.File, attach api.AttachIO) error {
	// Relay stdout: PTY master → attach.Stdout
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := master.Read(buf)
			if n > 0 && attach.Stdout() != nil {
				_, _ = attach.Stdout().Write(buf[:n])
			}
			if err != nil {
				done <- err
				return
			}
		}
	}()

	// Relay stdin: attach.Stdin → PTY master
	if attach.Stdin() != nil {
		go func() {
			buf := make([]byte, 4096)
			for {
				n, err := attach.Stdin().Read(buf)
				if n > 0 {
					_, _ = master.Write(buf[:n])
				}
				if err != nil {
					return
				}
			}
		}()
	}

	// Forward terminal resize events.
	go func() {
		for size := range attach.Resize() {
			_ = unix.IoctlSetWinsize(int(master.Fd()), unix.TIOCSWINSZ, &unix.Winsize{
				Row: uint16(size.Height),
				Col: uint16(size.Width),
			})
		}
	}()

	// Wait for the container to exit (master read returns EIO/EOF) or
	// the client to disconnect (context cancelled).
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}

// suppressSignalExit returns nil for exits caused by signals (e.g. Ctrl+C → 130,
// Ctrl+\ → 131) so that normal interactive session termination isn't surfaced
// as an "Internal error occurred" to the user.
func suppressSignalExit(err error) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ProcessState != nil {
		if status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus); ok {
			if status.Signaled() {
				return nil
			}
		}
		// Shells exit with 128+N when killed by signal N.
		// 130 = SIGINT, 131 = SIGQUIT — treat as clean detach.
		code := exitErr.ExitCode()
		if code == 130 || code == 131 {
			return nil
		}
	}
	return err
}

// WaitForMachineExit polls until the container reaches a terminal state or timeout.
// Used to wait for init containers to complete before starting app containers.
func (s *SystemdRuntime) WaitForMachineExit(ctx context.Context, podUID, containerName string, timeout time.Duration) (runtime.MachineState, error) {
	deadline := time.Now().Add(timeout)
	for {
		state, err := s.MachineStatus(ctx, podUID, containerName)
		if err != nil {
			return runtime.StateFailed, err
		}
		switch state {
		case runtime.StateExited, runtime.StateFailed:
			return state, nil
		}
		if time.Now().After(deadline) {
			return runtime.StateUnknown, fmt.Errorf("timeout waiting for container %s/%s to exit", podUID, containerName)
		}
		select {
		case <-ctx.Done():
			return runtime.StateUnknown, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// loadPreviousInvocationID uses the provided journal (already open, matches will
// be added and reset) to find all _SYSTEMD_INVOCATION_ID values for the unit
// in chronological order and returns the second-to-last — the previous run.
//
// The journal is seeked back to head before returning so the caller can
// add their own matches and seek position afterward.
func (s *SystemdRuntime) loadPreviousInvocationID(j *sdjournal.Journal, podUID, containerName string) string {
	unitName := wrapperUnitName(s.pawnName, podUID, containerName)

	// Temporarily match only on this unit to walk all invocations.
	_ = j.AddMatch("_SYSTEMD_UNIT" + "=" + unitName)
	defer j.FlushMatches()

	if err := j.SeekHead(); err != nil {
		return ""
	}

	seen := make(map[string]struct{})
	var ids []string
	for {
		n, err := j.Next()
		if err != nil || n == 0 {
			break
		}
		entry, err := j.GetEntry()
		if err != nil {
			continue
		}
		id := entry.Fields["_SYSTEMD_INVOCATION_ID"]
		if id == "" {
			continue
		}
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}

	if len(ids) < 2 {
		return ""
	}
	// ids is oldest-first; second-to-last is the previous invocation.
	return ids[len(ids)-2]
}

// mapActiveState converts a systemd ActiveState string to a runtime.MachineState.
func mapActiveState(s string) runtime.MachineState {
	switch s {
	case "active", "reloading":
		return runtime.StateRunning
	case "activating":
		return runtime.StateCreating
	case "failed":
		return runtime.StateFailed
	default:
		return runtime.StateExited
	}
}

// dbusUnitEscape converts a systemd unit name to its dbus object path component.
// e.g. "foo-bar.service" -> "foo_2dbar_2eservice"
func dbusUnitEscape(name string) string {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "_%02x", c)
		}
	}
	return b.String()
}

// Helpers

func sliceName(pawnName string) string {
	return fmt.Sprintf("perigeos-%s.slice", pawnName)
}

// substituteEnvVars replaces Kubernetes-style $(VAR_NAME) references in s with
// values from env. Unknown variables are left as-is, matching kubelet behaviour.
func substituteEnvVars(s string, env map[string]string) string {
	if !strings.Contains(s, "$(") {
		return s
	}
	var buf strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '$' && i+1 < len(s) && s[i+1] == '(' {
			end := strings.IndexByte(s[i+2:], ')')
			if end >= 0 {
				varName := s[i+2 : i+2+end]
				if val, ok := env[varName]; ok {
					buf.WriteString(val)
				} else {
					buf.WriteString(s[i : i+2+end+1])
				}
				i += 2 + end + 1
				continue
			}
		}
		buf.WriteByte(s[i])
		i++
	}
	return buf.String()
}

// wrapperUnitName produces the systemd unit name for a specific container within a pod.
// Format: perigeos-<pawn>-pod-<uid>-<container>.service
func wrapperUnitName(pawnName, podUID, containerName string) string {
	return fmt.Sprintf("perigeos-%s-pod-%s-%s.service", pawnName, podUID, containerName)
}

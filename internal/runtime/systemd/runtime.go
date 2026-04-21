package systemd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/coreos/go-systemd/v22/sdjournal"
	dbusv5 "github.com/godbus/dbus/v5"
	"github.com/malformed-c/periapsis/internal/cgroup"
	"github.com/malformed-c/periapsis/internal/image"
	"github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/node/api"
	"golang.org/x/sys/unix"
)

// maxConcurrentStarts limits the number of concurrent StartTransientUnit calls
// to prevent D-Bus/systemd overload during burst pod creation.
const maxConcurrentStarts = 8

// SystemdRuntime implements runtime.Runtime by managing transient systemd-nspawn services.
type SystemdRuntime struct {
	conn         systemdDBus // go-systemd wrapper (unit lifecycle)
	rawConn      machineDBus // raw godbus (machine1 interface, unit object properties)
	sigConn      signalDBus  // raw godbus (pawn-scoped PropertiesChanged signals)
	ownsSigConn  bool        // true when sigConn was created by this runtime (not shared)
	logger       *slog.Logger
	imageManager *image.ImageManager

	pawnName     string
	slice        string
	execStrategy runtime.ExecStrategy

	// attachPTYs holds PTY master fds for containers started with stdin=true.
	// Key: machineName ("pod-<uid>-<container>"), Value: *os.File (master).
	// The slave side is passed to nspawn via StandardInput/Output, and the
	// master is used by AttachContainer to relay stdin/stdout.
	attachPTYs sync.Map

	// startSem limits concurrent StartTransientUnit calls to avoid
	// overwhelming D-Bus and systemd during burst pod creation.
	startSem chan struct{}

	// unitWaiters allows WaitForMachineExit to be event-driven instead of
	// polling. Key: unit name, Value: channel that receives the terminal
	// SubState ("failed", "exited-success", etc.) from the SubscribeEvents
	// goroutine. Protected by unitWaitersMu.
	unitWaitersMu sync.Mutex
	unitWaiters   map[string]chan string
}

// Ensure compile-time interface compliance.
var _ runtime.Runtime = (*SystemdRuntime)(nil)

// Close releases the dbus connections held by the runtime.
func (s *SystemdRuntime) Close() {
	s.conn.Close()
	s.rawConn.Close()
	if s.sigConn != nil && s.ownsSigConn {
		s.sigConn.Close()
	}
}

// NewSystemdRuntime creates a new SystemdRuntime. If sharedSigConn is non-nil,
// it is used for D-Bus signal subscriptions (caller retains ownership and must
// close it). If nil, a dedicated connection is opened and owned by the runtime.
func NewSystemdRuntime(
	ctx context.Context,
	pawnName string,
	im *image.ImageManager,
	logger *slog.Logger,
	execStrategy runtime.ExecStrategy,
	sharedSigConn *dbusv5.Conn,
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

	sigConn := sharedSigConn
	ownsSigConn := false
	if sigConn == nil {
		sc, err := dbusv5.ConnectSystemBus()
		if err != nil {
			conn.Close()
			rawConn.Close()
			return nil, fmt.Errorf("failed to open signal dbus connection: %w", err)
		}
		sigConn = sc
		ownsSigConn = true
	}

	rt := NewSystemdRuntimeWithConns(pawnName, im, logger, execStrategy, conn, rawConn, sigConn)
	rt.ownsSigConn = ownsSigConn
	return rt, nil
}

// NewSystemdRuntimeWithConns creates a SystemdRuntime with the provided D-Bus connections.
func NewSystemdRuntimeWithConns(
	pawnName string,
	im *image.ImageManager,
	logger *slog.Logger,
	execStrategy runtime.ExecStrategy,
	conn systemdDBus,
	rawConn machineDBus,
	sigConn signalDBus,
) *SystemdRuntime {
	return &SystemdRuntime{
		conn:         conn,
		rawConn:      rawConn,
		sigConn:      sigConn,
		imageManager: im,
		logger:       logger,
		pawnName:     pawnName,
		slice:        sliceName(pawnName),
		execStrategy: execStrategy,
		startSem:     make(chan struct{}, maxConcurrentStarts),
		unitWaiters:  make(map[string]chan string),
	}
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

	// Interactive containers (stdin=true) use --console=pipe so we can wire
	// a PTY for kubectl attach. Non-interactive containers use --console=read-only
	// so nspawn allocates its own internal PTY (fixes ENXIO on /dev/stderr)
	// while stdout/stderr flow to the journal natively with correct _SYSTEMD_UNIT
	// attribution.
	//
	// Exception: containers that bind-mount /dev from the host must use
	// --console=pipe because --console=read-only tries to create /dev/console
	// inside the container, which conflicts with the host's /dev bind mount.
	consoleMode := "read-only"
	if cfg.Container != nil && cfg.Container.Stdin {
		consoleMode = "pipe"
	}
	for _, bm := range cfg.BindMounts {
		if bm.ContainerPath == "/dev" {
			consoleMode = "pipe"
			break
		}
	}

	// TODO: Can we rely on ENV?
	execStart := []string{
		"/usr/bin/systemd-nspawn",
		"--console=" + consoleMode,
		"--keep-unit",
		"--register=yes",
		"--kill-signal=SIGTERM",
		"--machine=" + machineName,
		// --slice= is NOT passed here: --keep-unit tells nspawn to stay in the
		// calling unit's cgroup rather than creating a new scope, so nspawn
		// ignores --slice= (and warns about it). The slice is already set on
		// the transient unit itself via dbus.PropSlice - that's what matters.
		"--directory=" + cfg.RootFS,
		"--network-namespace-path=" + netNSPath,
		// Do not let nspawn bind-mount the host /etc/resolv.conf into the
		// container - we write our own cluster-DNS resolv.conf into the
		// overlayfs before starting, and nspawn's default would overwrite it.
		"--resolv-conf=off",
	}

	// Privileged containers get all capabilities - required for workloads
	// that load BPF programs, manipulate network interfaces, etc.
	if cfg.Privileged {
		execStart = append(execStart, "--capability=all")
	}
	// User identity setup (ADR-0010). Inject passwd/group entries for the
	// target UID/GID so nspawn's --user= can resolve them.
	prepareUserIdentity(cfg.RootFS, cfg.RunAsUser, cfg.RunAsGroup, s.logger)

	// Userns shim: create a user namespace INSIDE the container after nspawn
	// has joined the CNI netns. This avoids the --private-users +
	// --network-namespace-path incompatibility (the userns child can't
	// setns() into an external netns without CAP_SYS_ADMIN in the init userns).
	// Privileged containers skip userns - they need host-level capabilities.
	useUserNS := cfg.RunAsUser != nil && !cfg.Privileged && usernsShimExists()
	var usernsFIFODir string
	if useUserNS {
		var err error
		usernsFIFODir, err = setupUserNSFIFOs(podUID, containerName)
		if err != nil {
			s.logger.Error("Failed to setup userns FIFOs, falling back to --user=", "error", err)
			useUserNS = false
		} else {
			// Bind-mount the shim binary and FIFO directory into the container.
			execStart = append(execStart,
				"--bind-ro="+usernsShimHostPath+":"+usernsShimContainerPath,
				"--bind="+usernsFIFODir+":/run/userns",
			)
		}
	}
	if !useUserNS && cfg.RunAsUser != nil {
		// Fallback: use nspawn's --user= (no userns isolation).
		if *cfg.RunAsUser != 0 {
			ensureGetentShim(cfg.RootFS, s.logger)
		}
		execStart = append(execStart, fmt.Sprintf("--user=%d", *cfg.RunAsUser))
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
	// nspawn auto-creates destination directories inside the container rootfs.
	//
	// By default, nspawn uses MS_SLAVE propagation (host->container visible,
	// container->host NOT visible). For mounts with Propagation: "Bidirectional",
	// bidirectional (MS_SHARED) propagation is enabled via makeSharedMounts,
	// which runs after the unit starts.
	for _, bm := range cfg.BindMounts {
		arg := bm.HostPath + ":" + bm.ContainerPath
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
	if useUserNS {
		// Prepend the userns shim - it calls unshare(CLONE_NEWUSER), waits
		// for perigeos to write uid_map/gid_map, adopts the target identity,
		// then exec()s the real workload.
		fullCmd = append([]string{usernsShimContainerPath}, fullCmd...)
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
		dbus.PropType("exec"), // exec type ensures systemd tracks the main process exit code
		dbus.PropExecStart(execStart, false),
		{Name: "SyslogIdentifier", Value: dbusv5.MakeVariant(cfg.Container.Name)},
		// No CollectMode - we manage unit lifecycle explicitly.
		// The BatchWatcher reads exit codes and cleans up dead/failed
		// units after processing their terminal state.
		{Name: "Delegate", Value: dbusv5.MakeVariant(true)},
		{Name: "KillMode", Value: dbusv5.MakeVariant("mixed")},
		{Name: "Environment", Value: dbusv5.MakeVariant(metaEnv)},
	}

	// Set TimeoutStopSec from the pod's terminationGracePeriodSeconds so
	// systemd's SIGTERM -> wait -> SIGKILL sequence matches the pod spec.
	if cfg.TerminationGracePeriodSeconds > 0 {
		properties = append(properties, dbus.Property{
			Name:  "TimeoutStopUSec",
			Value: dbusv5.MakeVariant(uint64(cfg.TerminationGracePeriodSeconds) * 1_000_000),
		})
	}

	// Interactive containers need a PTY for kubectl attach/exec stdin relay.
	// Non-interactive containers use --console=read-only (set above), which
	// lets nspawn allocate its own internal PTY. stdout/stderr flow to the
	// journal natively with correct _SYSTEMD_UNIT attribution - no forwarding
	// goroutine needed.
	if cfg.Container != nil && cfg.Container.Stdin {
		master, slave, err := openPTY()
		if err != nil {
			return fmt.Errorf("create stdio PTY for %s: %w", machineName, err)
		}
		slavePath := slave.Name()
		slave.Close() // systemd reopens the slave by path

		s.attachPTYs.Store(machineName, master)

		// Set raw mode so the PTY doesn't echo input or do line buffering.
		if termios, err := unix.IoctlGetTermios(int(master.Fd()), unix.TCGETS); err == nil {
			termios.Iflag &^= unix.IGNBRK | unix.BRKINT | unix.PARMRK | unix.ISTRIP | unix.INLCR | unix.IGNCR | unix.ICRNL | unix.IXON
			termios.Oflag &^= unix.OPOST
			termios.Lflag &^= unix.ECHO | unix.ECHONL | unix.ICANON | unix.ISIG | unix.IEXTEN
			termios.Cflag &^= unix.CSIZE | unix.PARENB
			termios.Cflag |= unix.CS8
			_ = unix.IoctlSetTermios(int(master.Fd()), unix.TCSETS, termios)
		}
		properties = append(properties,
			dbus.Property{Name: "StandardInputFile", Value: dbusv5.MakeVariant(slavePath)},
			dbus.Property{Name: "StandardOutputFile", Value: dbusv5.MakeVariant(slavePath)},
			dbus.Property{Name: "StandardErrorFile", Value: dbusv5.MakeVariant(slavePath)},
		)
		s.logger.Info("Created attach PTY", "machine", machineName, "slave", slavePath)
	}

	// Per-container resource limits from pod spec Resources.Limits.
	// Assembled as a cgroup2.Resources struct so future pod-label-driven
	// cgroup knobs (IO, Pids, memory.high, etc.) plug in without touching
	// this call site - the translation to D-Bus lives in internal/cgroup.
	properties = append(properties, cgroup.BuildSystemdProperties(buildPodResources(cfg))...)

	// We intentionally pass a nil channel instead of waiting for the job
	// completion signal. go-systemd's startJob has a race condition: it
	// registers the callback channel AFTER the D-Bus call returns, so for
	// fast-exit units the JobRemoved signal can arrive and be consumed
	// before the channel is registered, causing <-ch to block forever.
	// See: https://github.com/coreos/go-systemd/issues/485
	//
	// Instead of waiting for the job signal, the caller's waitForContainer
	// polls MachineStatus which handles both Running and Exited states.
	// Belt-and-suspenders: clear any stale unit before creating a new transient
	// one. StopMachine calls ResetFailedUnit too, but there's a small race window
	// between StopMachine returning and the unit being fully removed from the table.
	_ = s.conn.ResetFailedUnitContext(ctx, serviceName)

	// Clean up any stale unix-export bind mount from a previous SIGKILL.
	// nspawn creates this at /run/systemd/nspawn/unix-export/<machineName>
	// and removes it on clean exit, but not on SIGKILL. If it exists, the
	// next nspawn start immediately fails with "Mount point exists already".
	cleanNspawnUnixExport(machineName)

	// Acquire semaphore to limit concurrent StartTransientUnit calls.
	select {
	case s.startSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.startSem }()

	if _, err := s.conn.StartTransientUnitContext(ctx, serviceName, "replace", properties, nil); err != nil {
		// Clean up PTY and FIFOs on failure.
		if masterVal, ok := s.attachPTYs.LoadAndDelete(machineName); ok {
			masterVal.(*os.File).Close()
		}
		if useUserNS {
			cleanupUserNSFIFOs(podUID, containerName)
		}
		// "already loaded or has a fragment file" means the unit from a
		// previous attempt wasn't fully removed yet (transient unit auto-unload
		// is async). Stop+reset it and retry once.
		if strings.Contains(err.Error(), "already loaded") || strings.Contains(err.Error(), "fragment file") {
			_, _ = s.conn.StopUnitContext(ctx, serviceName, "replace", nil)
			_ = s.conn.ResetFailedUnitContext(ctx, serviceName)
			time.Sleep(200 * time.Millisecond)
			if _, retryErr := s.conn.StartTransientUnitContext(ctx, serviceName, "replace", properties, nil); retryErr != nil {
				return fmt.Errorf("failed to create machine unit (after reset retry): %w", retryErr)
			}
		} else {
			return fmt.Errorf("failed to create machine unit: %w", err)
		}
	}

	// Start the host-side userns handshake goroutine. This blocks on the
	// ready FIFO until the shim has called unshare(CLONE_NEWUSER), then
	// writes uid_map/gid_map and sends the target identity via gate FIFO.
	if useUserNS {
		targetUID := *cfg.RunAsUser
		targetGID := int64(0)
		if cfg.RunAsGroup != nil {
			targetGID = *cfg.RunAsGroup
		}
		go func() {
			s.completeUserNSSetup(usernsFIFODir, machineName, podUID, targetUID, targetGID)
			cleanupUserNSFIFOs(podUID, containerName)
		}()
	}

	return nil
}

// makeSharedMounts enables MS_SHARED (bidirectional) mount propagation for bind mounts
// that specify Propagation: "Bidirectional".
//
// For each such mount, this method:
// 1. Makes the host-side mountpoint shared via unix.Mount with MS_SHARED|MS_REC
// 2. Makes the container-side mount shared via nsenter --make-shared
//
// This allows container-side mounts to propagate back to the host, which is needed
// for CSI drivers that perform additional mounts inside the container.
func (s *SystemdRuntime) makeSharedMounts(ctx context.Context, machineName string, mounts []runtime.BindMount) error {
	// Filter for bidirectional mounts.
	var bidirectionalMounts []runtime.BindMount
	for _, bm := range mounts {
		if bm.Propagation == "Bidirectional" {
			bidirectionalMounts = append(bidirectionalMounts, bm)
		}
	}

	// If no bidirectional mounts, nothing to do.
	if len(bidirectionalMounts) == 0 {
		return nil
	}

	// Get the container's PID 1 on the host.
	containerPID, err := s.getMachineLeaderPID(machineName)
	if err != nil {
		return fmt.Errorf("get container leader PID for machine %s: %w", machineName, err)
	}

	// Process each bidirectional mount.
	for _, bm := range bidirectionalMounts {
		// Ensure the host-side path is on a shared peer group. On systemd
		// hosts the root mount is already shared, but if the path happens to
		// be its own mountpoint (e.g. a separate volume) we need to flip it.
		// EINVAL means it's not a mountpoint - that's fine, it inherits the
		// parent mount's (shared) propagation.
		if err := unix.Mount("", bm.HostPath, "", unix.MS_SHARED|unix.MS_REC, ""); err != nil && !errors.Is(err, unix.EINVAL) {
			return fmt.Errorf("make host mount shared for %s: %w", bm.HostPath, err)
		}

		// Make the container-side mount shared by entering the container's
		// mount namespace via setns and calling unix.Mount directly.
		//
		// We cannot use "nsenter -m -t <pid> -- mount --make-shared <path>"
		// because nspawn's pivot_root unmounts the old host root from the
		// container's mount namespace, so no host binaries (including mount(8))
		// are reachable there regardless of the container image contents.
		if err := makeContainerMountShared(containerPID, bm.ContainerPath); err != nil {
			return fmt.Errorf("make container mount shared for %s: %w", bm.ContainerPath, err)
		}

		s.logger.Info(
			"Made mount bidirectional",
			"machine", machineName,
			"hostPath", bm.HostPath,
			"containerPath", bm.ContainerPath,
		)
	}

	return nil
}

// MakeSharedMounts implements runtime.Runtime. It is called from launchContainer
// after waitForContainer confirms the machine is running, so getMachineLeaderPID
// is guaranteed to succeed.
func (s *SystemdRuntime) MakeSharedMounts(ctx context.Context, podUID, containerName string, mounts []runtime.BindMount) error {
	machineName := "pod-" + podUID + "-" + containerName
	return s.makeSharedMounts(ctx, machineName, mounts)
}

// makeContainerMountShared enters the container's mount namespace (by pid) and
// calls unix.Mount with MS_SHARED|MS_REC on containerPath.
//
// We do this with a raw setns syscall rather than "nsenter -- mount --make-shared"
// because nspawn's pivot_root removes the old host root from the container's
// mount namespace, so no host binaries are reachable there. The syscall needs
// no binary in the container image.
//
// The function:
//  1. Locks the current OS thread so Go's scheduler cannot migrate us.
//  2. Unshares CLONE_FS so the thread gets its own fs_struct. Without this,
//     setns(CLONE_NEWNS) fails with EINVAL in multithreaded Go programs.
//  3. Opens /proc/<pid>/ns/mnt and /proc/<pid>/root.
//  4. Enters the container's mount namespace via setns.
//  5. Changes the thread's root to the container's root via chroot.
//  6. Calls unix.Mount("", path, "", MS_SHARED|MS_REC, "").
//  7. Returns, allowing the Go runtime to terminate the locked OS thread.
//
// Steps 3-5 run inside a dedicated goroutine that is itself locked to an OS
// thread, so the setns call is always isolated to a single thread and never
// bleeds into other goroutines.
func makeContainerMountShared(pid int, containerPath string) error {
	type result struct{ err error }
	ch := make(chan result, 1)

	go func() {
		goruntime.LockOSThread()
		// Intentionally do NOT call UnlockOSThread: once we mutate the thread's
		// namespaces and root, we must not let Go reuse it for other goroutines.
		// When this goroutine exits, the Go runtime will terminate the OS thread.

		// Unshare CLONE_FS to get a private fs_struct.
		// A process/thread may not be reassociated with a new mount namespace
		// if it shares its fs_struct with other threads (which all Go threads do
		// by default). This fixes the EINVAL error from setns(CLONE_NEWNS).
		if err := unix.Unshare(unix.CLONE_FS); err != nil {
			ch <- result{fmt.Errorf("unshare CLONE_FS: %w", err)}
			return
		}

		// Open the container's mount namespace.
		nsPath := fmt.Sprintf("/proc/%d/ns/mnt", pid)
		containerNS, err := os.Open(nsPath)
		if err != nil {
			ch <- result{fmt.Errorf("open container mnt ns %s: %w", nsPath, err)}
			return
		}
		defer containerNS.Close()

		// Open the container's root directory.
		rootPath := fmt.Sprintf("/proc/%d/root", pid)
		rootDir, err := os.Open(rootPath)
		if err != nil {
			ch <- result{fmt.Errorf("open container root %s: %w", rootPath, err)}
			return
		}
		defer rootDir.Close()

		// Enter the container's mount namespace.
		if err := unix.Setns(int(containerNS.Fd()), unix.CLONE_NEWNS); err != nil {
			ch <- result{fmt.Errorf("setns into container mnt ns: %w", err)}
			return
		}

		// Change the thread's root directory to the container's root.
		// This is required because setns(CLONE_NEWNS) does not change the caller's
		// root directory. Without this, the subsequent mount() would resolve
		// containerPath relative to the host's root.
		if err := unix.Fchdir(int(rootDir.Fd())); err != nil {
			ch <- result{fmt.Errorf("fchdir to container root: %w", err)}
			return
		}
		if err := unix.Chroot("."); err != nil {
			ch <- result{fmt.Errorf("chroot to container root: %w", err)}
			return
		}

		// Make the mount shared inside the container's namespace.
		mountErr := unix.Mount("", containerPath, "", unix.MS_SHARED|unix.MS_REC, "")

		if mountErr != nil && !errors.Is(mountErr, unix.EINVAL) {
			ch <- result{fmt.Errorf("mount --make-shared %s: %w", containerPath, mountErr)}
			return
		}
		ch <- result{}
	}()

	r := <-ch
	return r.err
}

// cleanNspawnUnixExport removes the stale unix-export bind mount that
// systemd-nspawn creates at startup and cleans up on a graceful exit.
// When nspawn is SIGKILL'd (e.g. by a timed-out StopMachine), the mount
// survives. The next nspawn for the same machine name sees it and refuses:
//
//	"Mount point '...' exists already, refusing."
//
// Call this before StartTransientUnit and after StopMachine.
func cleanNspawnUnixExport(machineName string) {
	path := "/run/systemd/nspawn/unix-export/" + machineName
	// MNT_DETACH (lazy unmount): safe even if nothing is actually mounted.
	// EINVAL means path is not a mountpoint - nothing to do.
	if err := unix.Unmount(path, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		// Non-fatal: log via slog would require a logger here; the next
		// nspawn start will fail and surface the error through normal paths.
		_ = err
	}
	_ = os.Remove(path) // no-op if not present
}

// StopMachine stops the wrapper service with a context-respecting timeout.
func (s *SystemdRuntime) StopMachine(ctx context.Context, podUID, containerName string) error {
	var caller string
	if _, file, line, ok := goruntime.Caller(1); ok {
		caller = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	s.logger.Info("Stopping Machine", "pod", podUID, "container", containerName, "caller", caller)
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
	case <-ch:
		// Always reset after stopping - clears the unit from systemd's table even
		// when the container had already failed (stop job returns "done" immediately
		// for a unit already in failed/inactive state, but without ResetFailedUnit
		// the unit stays in the table and StartTransientUnit fails on the next restart).
		if err := s.conn.ResetFailedUnitContext(ctx, wrapperUnit); err != nil {
			s.logger.Debug("ResetFailedUnit ignored", "unit", wrapperUnit, "err", err)
		}
		// Clean up stale unix-export mount that survives SIGKILL.
		cleanNspawnUnixExport(machineName)
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// CheckMachined verifies systemd-machined is healthy by calling ListMachines
// over D-Bus. If machined has exhausted its file descriptor limit or its
// socket is down, this call will fail - letting callers back off early instead
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
		// D-Bus errors under load should NOT be treated as "exited" - that
		// causes WaitForMachineExit to think an init container finished when
		// it hasn't, leading to empty volumes and crash loops. Return Unknown
		// so callers retry.
		return runtime.StateUnknown, nil
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
		// Check ExecMainStatus for non-zero exit code. Systemd may briefly
		// report ActiveState=inactive before transitioning to failed.
		if exitCode := s.readExitCode(ctx, serviceName); exitCode != 0 {
			return runtime.StateFailed, nil
		}
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
		exitCode := s.readExitCode(ctx, unit.Name)

		// Systemd may briefly report ActiveState=inactive before settling
		// to failed for non-zero exits. Use ExecMainStatus as the source
		// of truth: any non-zero exit code means the container failed.
		if state == runtime.StateExited && exitCode != 0 {
			state = runtime.StateFailed
		}

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
			ExitCode:      exitCode,
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

// readExitCode reads the ExecMainStatus property of a systemd service unit,
// which contains the process exit code. Returns 0 if unavailable.
func (s *SystemdRuntime) readExitCode(ctx context.Context, unitName string) int32 {
	prop, err := s.conn.GetServicePropertyContext(ctx, unitName, "ExecMainStatus")
	if err != nil {
		return 0
	}
	code, ok := prop.Value.Value().(int32)
	if !ok {
		return 0
	}
	return code
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

	// Resolve the target invocation ID so we only return logs from one
	// lifecycle of the unit - not entries from previous restarts.
	ids := s.loadInvocationIDs(j, podUID, containerName)
	var targetID string
	if opts.Previous {
		if len(ids) < 2 {
			j.Close()
			return nil, fmt.Errorf("no previous logs available for pod %s container %s", podUID, containerName)
		}
		targetID = ids[len(ids)-2]
	} else if len(ids) > 0 {
		targetID = ids[len(ids)-1]
	}

	if targetID != "" {
		if err := j.AddMatch("_SYSTEMD_INVOCATION_ID" + "=" + targetID); err != nil {
			j.Close()
			return nil, fmt.Errorf("journal match invocation: %w", err)
		}
	} else {
		// No invocation IDs found - fall back to unit name match.
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
		cutoff := uint64(time.Now().Add(-time.Duration(opts.SinceSeconds) * time.Second).UnixMicro())
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

// AttachContainer attaches stdin/stdout/stderr to the running container's
// PID 1. If the container was started with stdin=true, a PTY master was
// allocated at startup; we relay the attach streams through it. Otherwise
// we fall back to nsenter with an interactive shell.
func (s *SystemdRuntime) AttachContainer(
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
	// Relay stdout: PTY master -> attach.Stdout
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

	// Relay stdin: attach.Stdin -> PTY master
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

// suppressSignalExit returns nil for exits caused by signals (e.g. Ctrl+C -> 130,
// Ctrl+\ -> 131) so that normal interactive session termination isn't surfaced
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
		// 130 = SIGINT, 131 = SIGQUIT - treat as clean detach.
		code := exitErr.ExitCode()
		if code == 130 || code == 131 {
			return nil
		}
	}
	return err
}

// registerWaiter creates a channel that will be notified when the given unit
// reaches a terminal D-Bus SubState. Returns a cleanup function.
func (s *SystemdRuntime) registerWaiter(unitName string) (<-chan string, func()) {
	ch := make(chan string, 4) // buffered to avoid blocking the event goroutine
	s.unitWaitersMu.Lock()
	s.unitWaiters[unitName] = ch
	s.unitWaitersMu.Unlock()
	return ch, func() {
		s.unitWaitersMu.Lock()
		delete(s.unitWaiters, unitName)
		s.unitWaitersMu.Unlock()
	}
}

// notifyWaiters is called by the SubscribeEvents goroutine to wake any
// WaitForMachineExit caller blocked on this unit.
func (s *SystemdRuntime) notifyWaiters(unitName, subState string) {
	s.unitWaitersMu.Lock()
	ch, ok := s.unitWaiters[unitName]
	s.unitWaitersMu.Unlock()
	if ok {
		select {
		case ch <- subState:
		default:
		}
	}
}

// WaitForMachineExit blocks until the container reaches a terminal state.
// It is event-driven via D-Bus unit signals, with a poll fallback for
// robustness (events can be dropped under load).
//
// Race guard: StartTransientUnit with a nil channel returns before systemd
// actually starts the process. The first status check may see
// ActiveState=inactive + ExecMainStatus=0, which looks like "exited
// successfully" but really means "hasn't started yet." To avoid this,
// we require seeing the unit actually start (Running/Creating/Failed) before
// accepting StateExited, with a grace period for the unit to be scheduled.
func (s *SystemdRuntime) WaitForMachineExit(ctx context.Context, podUID, containerName string, timeout time.Duration) (runtime.MachineState, error) {
	serviceName := wrapperUnitName(s.pawnName, podUID, containerName)
	waiterCh, cleanup := s.registerWaiter(serviceName)
	defer cleanup()

	deadline := time.Now().Add(timeout)
	started := false
	const startGrace = 5 * time.Second
	startDeadline := time.Now().Add(startGrace)

	// checkState queries MachineStatus and returns the terminal state if
	// the unit has reached one, or ("", nil) if it should keep waiting.
	checkState := func() (runtime.MachineState, error) {
		state, err := s.MachineStatus(ctx, podUID, containerName)
		if err != nil {
			return runtime.StateFailed, err
		}
		switch state {
		case runtime.StateRunning, runtime.StateCreating:
			started = true
		case runtime.StateFailed:
			return runtime.StateFailed, nil
		case runtime.StateExited:
			if started || time.Now().After(startDeadline) {
				return runtime.StateExited, nil
			}
			// Likely "not yet started" - keep waiting.
		case runtime.StateUnknown:
			// D-Bus error - keep waiting.
		}
		return "", nil
	}

	// Initial check before entering the event loop.
	if state, err := checkState(); state != "" || err != nil {
		return state, err
	}

	// Poll fallback: if events are dropped or delayed, we still converge.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return runtime.StateUnknown, ctx.Err()
		case subState := <-waiterCh:
			// D-Bus event received - check if terminal.
			switch subState {
			case "running", "start-pre", "start", "start-post":
				started = true
				continue
			case "failed":
				return runtime.StateFailed, nil
			}
			// For "dead" and other states, do a full status check to
			// get the authoritative exit code.
			if state, err := checkState(); state != "" || err != nil {
				return state, err
			}
		case <-ticker.C:
			if state, err := checkState(); state != "" || err != nil {
				return state, err
			}
		}
		if time.Now().After(deadline) {
			return runtime.StateUnknown, fmt.Errorf("timeout waiting for container %s/%s to exit", podUID, containerName)
		}
	}
}

// loadInvocationIDs walks the journal for the given unit and returns all
// distinct _SYSTEMD_INVOCATION_ID values in chronological order (oldest first).
// The journal's matches are flushed before returning so the caller can set
// up its own filters afterward.
func (s *SystemdRuntime) loadInvocationIDs(j *sdjournal.Journal, podUID, containerName string) []string {
	unitName := wrapperUnitName(s.pawnName, podUID, containerName)

	// Temporarily match only on this unit to walk all invocations.
	_ = j.AddMatch("_SYSTEMD_UNIT" + "=" + unitName)
	defer j.FlushMatches()

	if err := j.SeekHead(); err != nil {
		return nil
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

	return ids
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

// dbusUnitEscape converts a systemd unit name prefix to its D-Bus object path
// component using systemd's bus_label_escape rules.
// e.g. "perigeos-compute-00-" -> "perigeos_2dcompute_2d00_2d"
func dbusUnitEscape(name string) string {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		if isAlpha || (isDigit && i > 0) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "_%02x", c)
		}
	}
	return b.String()
}

// dbusUnitUnescape is the inverse of dbusUnitEscape: converts a D-Bus object
// path component back to the original systemd unit name.
func dbusUnitUnescape(escaped string) string {
	if escaped == "_" {
		return ""
	}
	var b strings.Builder
	for i := 0; i < len(escaped); i++ {
		if escaped[i] == '_' && i+2 < len(escaped) {
			hi := unhex(escaped[i+1])
			lo := unhex(escaped[i+2])
			if hi >= 0 && lo >= 0 {
				b.WriteByte(byte(hi<<4 | lo))
				i += 2
				continue
			}
		}
		b.WriteByte(escaped[i])
	}
	return b.String()
}

func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c - 'a' + 10)
	case c >= 'A' && c <= 'F':
		return int(c - 'A' + 10)
	default:
		return -1
	}
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

// ResetUnit cleans up a dead/failed transient unit by calling ResetFailedUnit.
// This removes the unit from systemd's listing so it doesn't accumulate.
func (s *SystemdRuntime) ResetUnit(ctx context.Context, podUID, containerName string) error {
	serviceName := wrapperUnitName(s.pawnName, podUID, containerName)
	return s.conn.ResetFailedUnitContext(ctx, serviceName)
}

// CleanupStaleUnits resets all dead/failed transient units whose pod UID is
// not in the activeUIDs set. This handles units left behind by a previous
// crash or restart where the BatchWatcher never got to clean them up.
func (s *SystemdRuntime) CleanupStaleUnits(ctx context.Context, activeUIDs map[string]bool) (int, error) {
	pattern := fmt.Sprintf("perigeos-%s-pod-*.service", s.pawnName)
	units, err := s.conn.ListUnitsByPatternsContext(ctx, nil, []string{pattern})
	if err != nil {
		return 0, fmt.Errorf("list units: %w", err)
	}

	cleaned := 0
	for _, unit := range units {
		// Only clean up non-running units.
		if unit.ActiveState == "active" || unit.ActiveState == "activating" {
			continue
		}
		// Parse UID from unit name.
		env := s.readUnitEnv(ctx, unit.Name)
		uid := env["PERIGEOS_META_UID"]
		if uid == "" {
			continue
		}
		if activeUIDs[uid] {
			continue
		}
		if err := s.conn.ResetFailedUnitContext(ctx, unit.Name); err != nil {
			s.logger.Debug("CleanupStaleUnits: reset failed", "unit", unit.Name, "err", err)
			continue
		}
		s.logger.Info("Cleaned up stale unit", "unit", unit.Name, "uid", uid)
		cleaned++
	}
	return cleaned, nil
}

// SubscribeEvents subscribes to D-Bus PropertiesChanged signals for this pawn's
// units and returns a channel that emits UnitEvents.
//
// Uses a dedicated raw godbus connection (sigConn) with a targeted match rule
// using path_namespace to filter signals at the D-Bus level. This avoids
// processing PropertiesChanged signals from every systemd unit on the host -
// only units whose object path starts with our pawn prefix are delivered.
//
// Previous approach used go-systemd's Subscribe()+SetPropertiesSubscriber which
// matches ALL PropertiesChanged signals and filters in userspace. That works but
// wastes CPU on busy hosts with many units.
func (s *SystemdRuntime) SubscribeEvents(ctx context.Context) <-chan runtime.UnitEvent {
	if s.sigConn == nil {
		s.logger.Warn("No signal D-Bus connection, falling back to poll-only")
		return nil
	}

	// Tell systemd to emit PropertiesChanged signals for units on this
	// connection. Without this call, systemd won't send any property
	// change notifications regardless of match rules.
	// When using a shared connection, the caller must have already called
	// Manager.Subscribe (it errors with "already subscribed" on repeat calls).
	if s.ownsSigConn {
		sysObj := s.sigConn.Object("org.freedesktop.systemd1", "/org/freedesktop/systemd1")
		if call := sysObj.Call("org.freedesktop.systemd1.Manager.Subscribe", 0); call.Err != nil {
			s.logger.Warn("Failed to subscribe to systemd signals, falling back to poll-only", "err", call.Err)
			return nil
		}
	}

	// Build the D-Bus object path prefix for this pawn's units.
	// systemd encodes unit names using bus_label_escape: non-alphanumeric
	// bytes become _XX (hex). E.g. "perigeos-compute-00-" becomes
	// "perigeos_2dcompute_2d00_2d" under /org/freedesktop/systemd1/unit/.
	pathPrefix := "/org/freedesktop/systemd1/unit/" + dbusUnitEscape("perigeos-"+s.pawnName+"-")

	// Register a match rule for PropertiesChanged signals from systemd units.
	// Note: path_namespace filtering is not reliable across D-Bus daemon versions,
	// so we filter by path prefix in Go after receiving the signal.
	err := s.sigConn.AddMatchSignal(
		dbusv5.WithMatchInterface("org.freedesktop.DBus.Properties"),
		dbusv5.WithMatchMember("PropertiesChanged"),
	)
	if err != nil {
		s.logger.Warn("Failed to add D-Bus match rule, falling back to poll-only", "err", err)
		return nil
	}

	sigCh := make(chan *dbusv5.Signal, 64)
	s.sigConn.Signal(sigCh)

	s.logger.Info("D-Bus event subscription active",
		"path_prefix", pathPrefix)

	eventCh := make(chan runtime.UnitEvent, 64)
	go func() {
		defer close(eventCh)
		defer s.sigConn.RemoveSignal(sigCh)
		for {
			select {
			case <-ctx.Done():
				return
			case sig, ok := <-sigCh:
				if !ok {
					return
				}
				// Filter to this pawn's units by object path prefix.
				if !strings.HasPrefix(string(sig.Path), pathPrefix) {
					continue
				}
				// PropertiesChanged signal body:
				//   [0] string - interface name
				//   [1] map[string]dbus.Variant - changed properties
				//   [2] []string - invalidated properties
				if len(sig.Body) < 2 {
					continue
				}
				iface, _ := sig.Body[0].(string)
				if iface != "org.freedesktop.systemd1.Unit" {
					continue
				}
				changed, _ := sig.Body[1].(map[string]dbusv5.Variant)
				if changed == nil {
					continue
				}
				subStateVar, ok := changed["SubState"]
				if !ok {
					continue
				}
				subState, ok := subStateVar.Value().(string)
				if !ok {
					continue
				}

				// Decode unit name from D-Bus object path.
				unitName := dbusUnitUnescape(pathBase(string(sig.Path)))

				s.logger.Debug("D-Bus unit event", "unit", unitName, "substate", subState)
				s.notifyWaiters(unitName, subState)
				select {
				case eventCh <- runtime.UnitEvent{
					UnitName: unitName,
					SubState: subState,
				}:
				default:
					s.logger.Debug("Unit event channel full, dropping event",
						"unit", unitName, "substate", subState)
				}
			}
		}
	}()

	return eventCh
}

// pathBase returns the last component of a slash-separated path.
func pathBase(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

package runtime

import (
        "context"
        "io"
        "os/exec"
        "time"

        "github.com/malformed-c/periapsis/node/api"
        corev1 "k8s.io/api/core/v1"
        "k8s.io/apimachinery/pkg/api/resource"
)

// Runtime defines the contract for managing pod lifecycle as systemd machines.
type Runtime interface {
        // RunMachine creates and starts a transient systemd-nspawn service for the pod.
        RunMachine(ctx context.Context, podUID string, cfg PodConfig) error

        // StopMachine tears down the machine and its associated systemd unit.
        // containerName must match the one passed to RunMachine.
        StopMachine(ctx context.Context, podUID, containerName string) error

        // MachineStatus returns the current OS-level state of the machine.
        MachineStatus(ctx context.Context, podUID, containerName string) (MachineState, error)

        // MachineExitCode returns the process exit code for a machine that has
        // exited or failed. Returns 0 if the machine is still running or the
        // exit code is not yet available.
        MachineExitCode(ctx context.Context, podUID, containerName string) int32

        // WaitForMachineExit blocks until the machine has exited (StateExited or StateFailed)
        // or the timeout is reached. Used to wait for init containers to complete.
        WaitForMachineExit(ctx context.Context, podUID, containerName string, timeout time.Duration) (MachineState, error)

        // ListManagedMachines returns all active machines managed by this runtime,
        // including full metadata recovered from embedded unit properties.
        ListManagedMachines(ctx context.Context) ([]PodMetadata, error)

        // GetLogStream retrieves the journald log stream for a container within a pod.
        GetLogStream(ctx context.Context, podUID, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error)

        // RunInContainer executes a command inside a running container.
        RunInContainer(ctx context.Context, podUID, containerName string, cmd []string, attach api.AttachIO) error

        // PortForward proxies a TCP connection to the given port inside the container's
        // network namespace. stream is bidirectionally copied to/from the connection.
        PortForward(ctx context.Context, podUID, containerName string, port int32, stream io.ReadWriteCloser) error

        // AttachContainer attaches to the stdio of a running container's PID 1.
        AttachContainer(ctx context.Context, podUID, containerName string, attach api.AttachIO) error

        // InitPawnSlice creates the cgroup slice for a pawn and applies resource limits.
        InitPawnSlice(ctx context.Context, cfg PawnSliceConfig) error

        // CheckMachined verifies that systemd-machined is healthy and can accept
        // new machine registrations. Returns a descriptive error if machined is
        // unreachable or resource-constrained (e.g. FD limit exhausted).
        CheckMachined(ctx context.Context) error

        // SubscribeEvents returns a channel that receives UnitEvent values
        // whenever a managed unit changes state (e.g. active->dead/failed).
        // The BatchWatcher uses this to reactively update container state
        // instead of waiting for the next poll tick.
        // The channel is closed when ctx is cancelled.
        // Implementations that cannot provide events should return a nil channel.
        SubscribeEvents(ctx context.Context) <-chan UnitEvent

        // MakeSharedMounts enables MS_SHARED (bidirectional) propagation for the
        // bind mounts of a running container. Must be called after the container
        // is confirmed running (i.e. after waitForContainer) so the machine has
        // registered with machined and its leader PID is resolvable.
        MakeSharedMounts(ctx context.Context, podUID, containerName string, mounts []BindMount) error

        // ResetUnit cleans up a dead/failed transient unit so it doesn't
        // accumulate in systemd's unit listing. Called by the BatchWatcher
        // after it has read the unit's exit code and processed terminal state.
        ResetUnit(ctx context.Context, podUID, containerName string) error

        // CleanupStaleUnits resets all dead/failed transient units that are
        // not associated with any known pod UID. Called on startup to clean
        // up units left behind by a previous crash or restart.
        CleanupStaleUnits(ctx context.Context, activeUIDs map[string]bool) (cleaned int, err error)

        // SliceActive returns whether this pawn's cgroup slice is active in systemd.
        SliceActive(ctx context.Context) bool
}

// UnitEvent is emitted by SubscribeEvents when a managed unit changes state.
type UnitEvent struct {
        UnitName string
        SubState string // systemd sub-state: "dead", "failed", "running", etc.
}

// MachineState represents the lifecycle state of a systemd-nspawn machine.
type MachineState string

const (
        StateUnknown  MachineState = "Unknown"
        StateRunning  MachineState = "Running"
        StateExited   MachineState = "Exited"
        StateFailed   MachineState = "Failed"
        StateCreating MachineState = "Creating"
)

// ExecStrategy controls how RunInContainer enters the container.
type ExecStrategy int

const (
        // ExecNsenter enters the container namespaces via nsenter.
        // Works on any container (Alpine, distroless, scratch). Default.
        ExecNsenter ExecStrategy = iota

        // ExecMachinectl uses `machinectl shell`.
        // Better PTY experience but requires systemd as PID 1 inside the container.
        ExecMachinectl
)

// BindMount represents a host->container bind mount to be passed to nspawn.
type BindMount struct {
        HostPath      string
        ContainerPath string
        ReadOnly      bool
        // Propagation mirrors Kubernetes MountPropagationMode:
        // "" / HostToContainer -> MS_SLAVE (default)
        // Bidirectional        -> MS_SHARED
        Propagation string
}

// PodConfig defines the parameters needed to start a single container as a machine.
type PodConfig struct {
        // Identity
        Name      string
        Namespace string
        UID       string

        // ContainerName identifies which container this machine runs.
        // Used in unit naming - must be set for multi-container pods.
        ContainerName string

        // The container spec to run
        Container *corev1.Container

        // Hierarchy
        PawnName string // determines parent cgroup slice

        // Filesystem
        RootFS string // absolute path to the overlayfs merged dir

        // Bind mounts: resolved from pod Volumes + container VolumeMounts.
        BindMounts []BindMount

        // Networking
        NetNSPath   string // absolute path to /var/run/netns/<uid>
        HostNetwork bool   // when true, join the host network namespace

        // Security
        Privileged bool // when true, grant all capabilities (--capability=all)
        HostPID    bool // when true, skip nspawn isolation - run directly on host PID/cgroup namespace
        // Effective run user/group for the container process. Container-level
        // securityContext values override pod-level values.
        RunAsUser  *int64
        RunAsGroup *int64

        // Tidal: fully resolved KEY=VALUE strings
        Environment []string

        // PodIP is needed here so it can be embedded into the unit properties
        PodIP string

        // Resource limits from container.Resources.Limits.
        // Applied as systemd cgroup properties (MemoryMax, CPUQuotaPerSecUSec).
        MemoryLimitBytes uint64 // 0 = no limit
        CPULimitMillis   int64  // 0 = no limit (millicores, e.g. 500 = 0.5 CPU)
        // Resource request from container.Resources.Requests.
        // Converted to systemd CPUWeight to mirror Kubernetes relative CPU shares.
        CPURequestMillis int64 // 0 = no request

        // OCI image defaults, used as fallback when Container.Command/Args are unset.
        // Follows Kubernetes command resolution: Container.Command overrides Entrypoint,
        // Container.Args overrides Cmd; unset fields fall back to the image values.
        ImageEntrypoint []string
        ImageCmd        []string

        // TerminationGracePeriodSeconds from the pod spec. Sets TimeoutStopSec
        // on the systemd unit so SIGTERM -> wait -> SIGKILL follows the pod's
        // requested grace period. 0 means use systemd default (90s).
        TerminationGracePeriodSeconds int64
}

// PodMetadata is returned by ListManagedMachines.
// All fields are recovered from embedded unit properties - no separate DB needed.
type PodMetadata struct {
        Name          string
        Namespace     string
        UID           string
        NodeName      string
        PodIP         string
        ContainerName string
        // StartedAt is the time the systemd unit entered the active state.
        // Used by the Reconciler for the orphan grace period: a machine that
        // started very recently may be mid-creation after a crash and should
        // not be culled until the grace period has elapsed.
        StartedAt time.Time
        // State is the current lifecycle state of the machine, populated by
        // ListManagedMachines so callers can avoid per-unit D-Bus queries.
        State MachineState
        // ExitCode is the process exit code from systemd's ExecMainStatus.
        // Only meaningful when State is StateExited or StateFailed.
        ExitCode int32
}

// PawnSliceConfig carries the resource limits for a pawn's cgroup slice.
type PawnSliceConfig struct {
        Name                string
        BaseDir             string
        CPU                 resource.Quantity
        Memory              resource.Quantity
        CPUWeight           uint64
        IOReadBandwidthMax  resource.Quantity
        IOWriteBandwidthMax resource.Quantity
}

// LogReadCloser wraps a ReadCloser and the Cmd that produced it.
// Closing waits for the command to exit cleanly.
type LogReadCloser struct {
        io.ReadCloser
        Cmd *exec.Cmd
}

func (l *LogReadCloser) Close() error {
        err := l.ReadCloser.Close()
        _ = l.Cmd.Wait() // killing journalctl -f is expected; ignore exit error
        return err
}

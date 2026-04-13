package systemd

import (
        "context"
        "fmt"
        "sort"
        "strings"

        "github.com/coreos/go-systemd/v22/dbus"
        dbusv5 "github.com/godbus/dbus/v5"
        "github.com/malformed-c/periapsis/internal/cgroup"
        "github.com/malformed-c/periapsis/internal/runtime"
)

// bindEntry is the D-Bus struct for one entry in BindPaths / BindReadOnlyPaths.
// Systemd D-Bus type: (ssbt) = (source, dest, ignore-if-missing, mount-flags)
type bindEntry struct {
        Source string
        Dest   string
        Ignore bool
        Flags  uint64
}

// bindsAPIVFS returns true if the bind mounts already include /proc, /sys, or
// /dev — meaning the pod provides its own API VFS and systemd should not mount
// fresh ones (MountAPIVFS=no).
func bindsAPIVFS(mounts []runtime.BindMount) bool {
        for _, bm := range mounts {
                switch bm.ContainerPath {
                case "/proc", "/sys", "/dev":
                        return true
                }
        }
        return false
}

// runProgram starts a container workload as a plain systemd transient service
// using RootDirectory= (chroot) instead of systemd-nspawn. This gives the
// process access to the host PID and cgroup namespaces — required for
// privileged infrastructure workloads such as the Constellation CNI agent.
//
// Lifecycle methods (StopMachine, MachineStatus, ListManagedMachines,
// GetLogStream) work transparently because they operate on the unit name,
// which follows the same wrapperUnitName convention as RunMachine.
func (s *SystemdRuntime) runProgram(ctx context.Context, podUID string, cfg runtime.PodConfig) error {
        containerName := cfg.ContainerName
        if containerName == "" {
                containerName = cfg.Container.Name
        }

        serviceName := wrapperUnitName(s.pawnName, podUID, containerName)
        slice := sliceName(s.pawnName)

        s.logger.Info("Starting program (host-pid mode)", "service", serviceName, "slice", slice)

        // Build env map for $(VAR_NAME) substitution in args/command.
        envMap := make(map[string]string, len(cfg.Environment))
        for _, kv := range cfg.Environment {
                if idx := strings.IndexByte(kv, '='); idx >= 0 {
                        envMap[kv[:idx]] = kv[idx+1:]
                }
        }

        // Build command with Kubernetes-style $(VAR) substitution applied.
        var fullCmd []string
        for _, part := range cfg.Container.Command {
                fullCmd = append(fullCmd, substituteEnvVars(part, envMap))
        }
        for _, part := range cfg.Container.Args {
                fullCmd = append(fullCmd, substituteEnvVars(part, envMap))
        }
        if len(fullCmd) == 0 {
                fullCmd = []string{"/bin/sleep", "infinity"}
        }

        // Pod metadata embedded in Environment so ListManagedMachines can recover
        // state on perigeos restart without a separate database.
        metaEnv := []string{
                "PERIGEOS_META_UID=" + podUID,
                "PERIGEOS_META_NAME=" + cfg.Name,
                "PERIGEOS_META_NAMESPACE=" + cfg.Namespace,
                "PERIGEOS_META_NODENAME=" + cfg.PawnName,
                "PERIGEOS_META_IP=" + cfg.PodIP,
                "PERIGEOS_META_CONTAINER=" + containerName,
        }
        allEnv := append(append([]string{},
                cfg.Environment...),
                "PERIGEOS_PAWN="+s.pawnName,
                "PERIGEOS_UID="+podUID,
        )
        allEnv = append(allEnv, metaEnv...)

        // Separate bind mounts into rw and ro lists.
        // Sort by destination path depth (parents before children) so that
        // systemd processes parent mounts first — e.g. /sys before /sys/fs/bpf.
        // Without this, a child mount can be covered by a later parent mount.
        sorted := make([]runtime.BindMount, len(cfg.BindMounts))
        copy(sorted, cfg.BindMounts)
        sort.Slice(sorted, func(i, j int) bool {
                return strings.Count(sorted[i].ContainerPath, "/") < strings.Count(sorted[j].ContainerPath, "/")
        })

        var bindPaths, bindROPaths []bindEntry
        for _, bm := range sorted {
                entry := bindEntry{
                        Source: bm.HostPath,
                        Dest:   bm.ContainerPath,
                        Flags:  0x4000, // MS_REC — recursive bind so submounts (e.g. bpffs) carry over
                }
                if bm.ReadOnly {
                        bindROPaths = append(bindROPaths, entry)
                } else {
                        bindPaths = append(bindPaths, entry)
                }
        }

        // Note: stdioLogProps is NOT used here. The ENXIO issue that stdioLogProps
        // solves is nspawn-specific (journal sockets passed via --console=pipe).
        // For RootDirectory services, systemd's default journal output works fine.
        // Logs are readable via journalctl / GetLogStream.
        properties := []dbus.Property{
                dbus.PropDescription("Pod " + podUID),
                dbus.PropSlice(slice),
                dbus.PropExecStart(fullCmd, false),
                {Name: "SyslogIdentifier", Value: dbusv5.MakeVariant(cfg.Container.Name)},
                {Name: "Delegate", Value: dbusv5.MakeVariant(true)},
                {Name: "KillMode", Value: dbusv5.MakeVariant("mixed")},
                // RootDirectory performs a chroot into the container image rootfs.
                // systemd creates a private mount namespace automatically when this is set.
                {Name: "RootDirectory", Value: dbusv5.MakeVariant(cfg.RootFS)},
                // Enable MountAPIVFS only when the pod mounts /proc, /sys, or /dev —
                // this ensures the directories exist in the chroot before BindPaths
                // overlays the host's filesystems. Recursive binds (Flags=1) then
                // carry submounts like /sys/fs/bpf sharing the host's superblock.
                {Name: "MountAPIVFS", Value: dbusv5.MakeVariant(bindsAPIVFS(cfg.BindMounts))},
                // All env vars (container + meta) go into the unit's Environment property.
                // The process inherits them from systemd, not via --setenv as in nspawn.
                {Name: "Environment", Value: dbusv5.MakeVariant(allEnv)},
        }
        // User identity setup (ADR-0010 Phase 3).
        prepareUserIdentity(cfg.RootFS, cfg.RunAsUser, cfg.RunAsGroup, s.logger)
        if cfg.RunAsUser != nil {
                properties = append(properties, dbus.Property{
                        Name: "User", Value: dbusv5.MakeVariant(fmt.Sprintf("%d", *cfg.RunAsUser)),
                })
        }
        if cfg.RunAsGroup != nil {
                properties = append(properties, dbus.Property{
                        Name: "Group", Value: dbusv5.MakeVariant(fmt.Sprintf("%d", *cfg.RunAsGroup)),
                })
        }

        // Sandbox hardening for non-privileged, non-hostNetwork workloads (ADR-0010).
        if !cfg.Privileged && !cfg.HostNetwork {
                properties = append(properties,
                        dbus.Property{Name: "NoNewPrivileges", Value: dbusv5.MakeVariant(true)},
                        dbus.Property{Name: "ProtectKernelTunables", Value: dbusv5.MakeVariant(true)},
                        dbus.Property{Name: "ProtectKernelModules", Value: dbusv5.MakeVariant(true)},
                        dbus.Property{Name: "PrivateDevices", Value: dbusv5.MakeVariant(true)},
                        dbus.Property{Name: "LockPersonality", Value: dbusv5.MakeVariant(true)},
                )
        }

        // Per-container resource limits from pod spec Resources.Limits.
        // See buildPodResources for the single seam where additional cgroup
        // knobs (IO, Pids, memory.high, cpuset, ...) should be wired in.
        properties = append(properties, cgroup.BuildSystemdProperties(buildPodResources(cfg))...)

        // No CollectMode — we manage unit lifecycle explicitly via MachineStatus
        // polling, just like the nspawn path in RunMachine. Using CollectMode=inactive-or-failed
        // with a blocking completion channel triggers the go-systemd StartTransientUnit
        // race condition: the unit can start, run, and exit before the completion
        // channel is registered, causing <-ch to block forever.
        //
        // We pass nil as the channel (fire-and-forget) and rely on the caller
        // (waitForContainer) to poll MachineStatus for startup detection.

        if len(bindPaths) > 0 {
                properties = append(properties, dbus.Property{
                        Name:  "BindPaths",
                        Value: dbusv5.MakeVariant(bindPaths),
                })
        }
        if len(bindROPaths) > 0 {
                properties = append(properties, dbus.Property{
                        Name:  "BindReadOnlyPaths",
                        Value: dbusv5.MakeVariant(bindROPaths),
                })
        }

        // Fire-and-forget: pass nil channel to avoid the go-systemd
        // StartTransientUnit race condition (coreos/go-systemd#485).
        // The caller (waitForContainer) polls MachineStatus to detect
        // that the unit has started.
        if _, err := s.conn.StartTransientUnitContext(ctx, serviceName, "replace", properties, nil); err != nil {
                return fmt.Errorf("start transient unit %s: %w", serviceName, err)
        }
        return nil
}

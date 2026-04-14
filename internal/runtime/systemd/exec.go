package systemd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"errors"

	dbusv5 "github.com/godbus/dbus/v5"
	"github.com/malformed-c/periapsis/node/api"
	"golang.org/x/sys/unix"
	utilexec "k8s.io/utils/exec"
)

// runInContainerNsenter enters the container by nsenter-ing into the nspawn
// leader process's namespaces. In TTY mode a host PTY is allocated and wired
// to the attach streams so the shell runs interactively.
func (s *SystemdRuntime) runInContainerNsenter(
	ctx context.Context,
	podUID, containerName string,
	cmd []string,
	attach api.AttachIO,
) error {
	machineName := "pod-" + podUID + "-" + containerName
	pid, err := s.getMachineLeaderPID(machineName)
	if err != nil {
		// Program-mode (hostPID) containers don't register with machined.
		// Use chroot into the unit's RootDirectory instead of nsenter - simpler
		// and avoids trying to enter namespaces that are shared with the host.
		unitName := wrapperUnitName(s.pawnName, podUID, containerName)
		return s.runInProgramContainer(ctx, unitName, cmd, attach)
	}

	args := []string{
		fmt.Sprintf("--target=%d", pid),
		"--mount", "--uts", "--ipc", "--net", "--pid", "--cgroup",
		"--root=/proc/" + strconv.Itoa(pid) + "/root",
		"--wdns=/",
		"--",
		"/usr/bin/env", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
	args = append(args, cmd...)

	execCmd := exec.CommandContext(ctx, "nsenter", args...)

	if attach.TTY() {
		return wrapExitError(runWithPTY(ctx, execCmd, attach))
	}
	wireAttach(execCmd, attach)
	return wrapExitError(execCmd.Run())
}

// runInContainerMachinectl uses `machinectl shell` to execute inside the container.
// Requires systemd running as PID 1 inside the container.
func (s *SystemdRuntime) runInContainerMachinectl(
	ctx context.Context,
	podUID, containerName string,
	cmd []string,
	attach api.AttachIO,
) error {
	machineName := "pod-" + podUID + "-" + containerName

	args := []string{"shell", "--quiet", machineName}
	if len(cmd) > 0 {
		args = append(args, cmd...)
	} else {
		args = append(args, "/bin/sh")
	}

	execCmd := exec.CommandContext(ctx, "machinectl", args...)
	if attach.TTY() {
		return wrapExitError(runWithPTY(ctx, execCmd, attach))
	}
	wireAttach(execCmd, attach)
	return wrapExitError(execCmd.Run())
}

// getMachineLeaderPID returns the host PID of the container's PID 1.
//
// Primary path: read the Leader property from the machined machine object.
// systemd v260+ exposes this as a D-Bus property directly. Prior versions
// used GetLeaderProcess() which is kept as a fallback.
//
// Fallback path: systemd v260 also exposes the Supervisor property (the
// host-side nspawn PID) on the machine object, making the previous fragile
// procfs child-scanning approach unnecessary. If the machine isn't registered
// yet we fall back to reading MainPID from the unit.
func (s *SystemdRuntime) getMachineLeaderPID(machineName string) (int, error) {
	if pid, err := s.leaderPIDFromMachined(machineName); err == nil {
		return pid, nil
	}
	return s.leaderPIDFromSupervisor(machineName)
}

// leaderPIDFromMachined reads the Leader property from the machine object.
// systemd v260+ exposes this as a D-Bus property; prior versions used
// GetLeaderProcess() which is kept as a fallback.
func (s *SystemdRuntime) leaderPIDFromMachined(machineName string) (int, error) {
	manager := s.rawConn.Object("org.freedesktop.machine1", "/org/freedesktop/machine1")

	var machinePath dbusv5.ObjectPath
	if err := manager.Call(
		"org.freedesktop.machine1.Manager.GetMachineByName", 0, machineName,
	).Store(&machinePath); err != nil {
		return 0, fmt.Errorf("GetMachineByName(%s): %w", machineName, err)
	}

	machineObj := s.rawConn.Object("org.freedesktop.machine1", machinePath)

	// systemd v260+: Leader is a D-Bus property.
	if leaderVar, err := machineObj.GetProperty("org.freedesktop.machine1.Machine.Leader"); err == nil {
		if leader, ok := leaderVar.Value().(uint32); ok && leader > 0 {
			return int(leader), nil
		}
	}

	// Pre-v260 fallback: GetLeaderProcess() method.
	var leader uint32
	if err := machineObj.Call(
		"org.freedesktop.machine1.Machine.GetLeaderProcess", 0,
	).Store(&leader); err != nil {
		return 0, fmt.Errorf("GetLeaderProcess(%s): %w", machineName, err)
	}
	if leader == 0 {
		return 0, fmt.Errorf("machined returned zero leader PID for %s", machineName)
	}
	return int(leader), nil
}

// leaderPIDFromSupervisor finds the container's PID 1 via the nspawn
// supervisor PID. systemd v260+ exposes the Supervisor property on the
// machine object directly, eliminating the need to parse unit MainPID.
// If the machine isn't registered yet, falls back to unit MainPID.
func (s *SystemdRuntime) leaderPIDFromSupervisor(machineName string) (int, error) {
	nspawnPID, err := s.supervisorPIDFromMachined(machineName)
	if err != nil {
		// Machine not yet registered - fall back to unit MainPID.
		nspawnPID, err = s.supervisorPIDFromUnit(machineName)
		if err != nil {
			return 0, err
		}
	}

	childrenPath := fmt.Sprintf("/proc/%d/task/%d/children", nspawnPID, nspawnPID)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(childrenPath)
		if err != nil {
			return 0, fmt.Errorf("read children of nspawn PID %d: %w", nspawnPID, err)
		}
		if fields := strings.Fields(string(data)); len(fields) > 0 {
			pid, err := strconv.Atoi(fields[0])
			if err != nil {
				return 0, fmt.Errorf("parse container init PID %q: %w", fields[0], err)
			}
			return pid, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return 0, fmt.Errorf("nspawn PID %d had no children after 2s", nspawnPID)
}

// supervisorPIDFromMachined reads the Supervisor property (systemd v260+)
// from the machine object. Returns the host-side nspawn PID.
func (s *SystemdRuntime) supervisorPIDFromMachined(machineName string) (int, error) {
	manager := s.rawConn.Object("org.freedesktop.machine1", "/org/freedesktop/machine1")

	var machinePath dbusv5.ObjectPath
	if err := manager.Call(
		"org.freedesktop.machine1.Manager.GetMachineByName", 0, machineName,
	).Store(&machinePath); err != nil {
		return 0, fmt.Errorf("GetMachineByName(%s): %w", machineName, err)
	}

	machineObj := s.rawConn.Object("org.freedesktop.machine1", machinePath)
	supervisorVar, err := machineObj.GetProperty("org.freedesktop.machine1.Machine.Supervisor")
	if err != nil {
		return 0, fmt.Errorf("supervisor property(%s): %w", machineName, err)
	}
	supervisor, ok := supervisorVar.Value().(uint32)
	if !ok || supervisor == 0 {
		return 0, fmt.Errorf("machined returned zero supervisor PID for %s", machineName)
	}
	return int(supervisor), nil
}

// supervisorPIDFromUnit reads MainPID from the nspawn wrapper unit.
// Used when the machine hasn't registered with machined yet.
func (s *SystemdRuntime) supervisorPIDFromUnit(machineName string) (int, error) {
	unitName := fmt.Sprintf("perigeos-%s-%s.service", s.pawnName, machineName)
	prop, err := s.conn.GetServicePropertyContext(context.Background(), unitName, "MainPID")
	if err != nil {
		return 0, fmt.Errorf("GetServiceProperty MainPID for %s: %w", unitName, err)
	}
	nspawnPID, ok := prop.Value.Value().(uint32)
	if !ok || nspawnPID == 0 {
		return 0, fmt.Errorf("unit %s has no MainPID (not running?)", unitName)
	}
	return int(nspawnPID), nil
}

// runInProgramContainer executes a command inside a program-mode (hostPID)
// container. These containers run as plain systemd transient services with
// RootDirectory= (chroot) and never register with machined.
//
// Strategy: enter ONLY the private mount namespace that systemd creates for
// RootDirectory= services (so bind-mounted volumes like /var/run/cilium are
// visible), then pivot root via --root. Skip all other namespace flags -
// the container shares host PID/net/uts/ipc/cgroup namespaces.
func (s *SystemdRuntime) runInProgramContainer(
	ctx context.Context,
	unitName string,
	cmd []string,
	attach api.AttachIO,
) error {
	pid, err := s.getUnitMainPID(ctx, unitName)
	if err != nil {
		return fmt.Errorf("could not find PID for unit %s: %w", unitName, err)
	}

	args := []string{
		fmt.Sprintf("--target=%d", pid),
		"--mount", // enter private mount namespace (bind mounts are here)
		"--root=/proc/" + strconv.Itoa(pid) + "/root",
		"--wdns=/",
		"--",
		"/usr/bin/env", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
	args = append(args, cmd...)

	// Use an independent context with a generous timeout so that probe
	// TimeoutSeconds (default 1s) does not race with nsenter startup overhead.
	// The command itself (e.g. "test -S /path") exits in milliseconds once running.
	execCtx, execCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer execCancel()

	execCmd := exec.CommandContext(execCtx, "nsenter", args...)
	if attach.TTY() {
		return wrapExitError(runWithPTY(execCtx, execCmd, attach))
	}
	wireAttach(execCmd, attach)
	return wrapExitError(execCmd.Run())
}

// getUnitMainPID returns the MainPID of a systemd service unit. This is used
// for program-mode (hostPID) containers that never register with machined, so
// getMachineLeaderPID can't find them. Unlike nspawn containers where the
// MainPID is the nspawn process and we need its first child, here the MainPID
// IS the actual container process we want to nsenter into.
func (s *SystemdRuntime) getUnitMainPID(ctx context.Context, unitName string) (int, error) {
	dbusCtx, dbusCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbusCancel()
	prop, err := s.conn.GetServicePropertyContext(dbusCtx, unitName, "MainPID")
	if err != nil {
		return 0, fmt.Errorf("GetServiceProperty MainPID for %s: %w", unitName, err)
	}
	pid, ok := prop.Value.Value().(uint32)
	if !ok || pid == 0 {
		return 0, fmt.Errorf("unit %s has no MainPID (not running?)", unitName)
	}
	return int(pid), nil
}

// runWithPTY allocates a host PTY, wires it to the exec command, and
// relays bytes between the PTY master and the attach streams.
func runWithPTY(ctx context.Context, cmd *exec.Cmd, attach api.AttachIO) error {
	master, slave, err := openPTY()
	if err != nil {
		return fmt.Errorf("failed to open PTY: %w", err)
	}

	// Set a sensible default window size before the shell starts so it
	// doesn't repeatedly send \033[6n cursor-position queries (which produce
	// ^[[row;colR garbage when size is 0x0).
	_ = unix.IoctlSetWinsize(int(master.Fd()), unix.TIOCSWINSZ, &unix.Winsize{
		Row: 24,
		Col: 80,
	})

	// slave becomes stdin/stdout/stderr of the child; Ctty=0 makes it the
	// controlling terminal (fd 0 after Go's exec dups slave → 0,1,2).
	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave
	cmd.SysProcAttr = &unix.SysProcAttr{
		Setsid:  true,
		Setctty: true,
		Ctty:    0,
	}

	if err := cmd.Start(); err != nil {
		slave.Close()
		return fmt.Errorf("cmd start: %w", err)
	}
	slave.Close() // parent doesn't need the slave end

	// Forward terminal resize events.
	go func() {
		for size := range attach.Resize() {
			_ = unix.IoctlSetWinsize(int(master.Fd()), unix.TIOCSWINSZ, &unix.Winsize{
				Row: uint16(size.Height),
				Col: uint16(size.Width),
			})
		}
	}()

	// Relay stdout: PTY master → attach.Stdout
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := master.Read(buf)
			if n > 0 && attach.Stdout() != nil {
				_, _ = attach.Stdout().Write(buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()

	// Relay stdin: attach.Stdin → PTY master
	go func() {
		if attach.Stdin() == nil {
			return
		}
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

	// Wait for process to exit. Closing master then causes the relay
	// goroutines to exit naturally on EIO.
	err = cmd.Wait()
	master.Close()
	return err
}

// openPTY opens a master/slave PTY pair on the host.
// Both TIOCSPTLCK (unlock) and TIOCGPTN (get number) require a pointer
// argument - pass via unsafe.Pointer so the kernel can read/write the value.
func openPTY() (master, slave *os.File, err error) {
	master, err = os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open /dev/ptmx: %w", err)
	}

	// Unlock the slave pts so it can be opened.
	var lock int32 = 0
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, master.Fd(),
		unix.TIOCSPTLCK, uintptr(unsafe.Pointer(&lock))); errno != 0 {
		master.Close()
		return nil, nil, fmt.Errorf("TIOCSPTLCK: %w", errno)
	}

	// Get the pts device number.
	var ptno uint32
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, master.Fd(),
		unix.TIOCGPTN, uintptr(unsafe.Pointer(&ptno))); errno != 0 {
		master.Close()
		return nil, nil, fmt.Errorf("TIOCGPTN: %w", errno)
	}

	// Open slave with O_NOCTTY to avoid making it the caller's controlling
	// terminal. The exec child sets its own controlling terminal via
	// SysProcAttr.Setctty, which works regardless of this flag.
	slavePath := fmt.Sprintf("/dev/pts/%d", ptno)
	slave, err = os.OpenFile(slavePath, os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		master.Close()
		return nil, nil, fmt.Errorf("open slave PTY %s: %w", slavePath, err)
	}

	return master, slave, nil
}

// wireAttach connects exec.Cmd's stdio to the attach streams.
func wireAttach(cmd *exec.Cmd, attach api.AttachIO) {
	if attach.Stdin() != nil {
		cmd.Stdin = attach.Stdin()
	}
	if attach.Stdout() != nil {
		cmd.Stdout = attach.Stdout()
	}
	if attach.Stderr() != nil {
		cmd.Stderr = attach.Stderr()
	}
}

// wrapExitError converts a Go *exec.ExitError into a utilexec.CodeExitError
// so that the Kubernetes remotecommand layer can extract the process exit code
// and return it to kubectl (instead of a generic "Internal error occurred").
func wrapExitError(err error) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return utilexec.CodeExitError{Err: err, Code: exitErr.ExitCode()}
	}
	return err
}

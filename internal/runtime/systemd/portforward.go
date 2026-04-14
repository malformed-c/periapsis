package systemd

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"

	"golang.org/x/sys/unix"
)

// PortForward proxies a TCP connection to port inside the container's network
// namespace. It enters the netns of the container's PID 1 via setns(2), dials
// 127.0.0.1:<port>, restores the original netns, then bidirectionally copies
// between the established connection and stream.
//
// setns(2) is per-thread, so the goroutine is locked to an OS thread for the
// duration of the enter/dial/restore sequence. The connection fd survives the
// netns restore - it remains bound to the container's network stack.
func (s *SystemdRuntime) PortForward(ctx context.Context, podUID, containerName string, port int32, stream io.ReadWriteCloser) error {
	machineName := "pod-" + podUID + "-" + containerName
	pid, err := s.getMachineLeaderPID(machineName)
	if err != nil {
		return fmt.Errorf("portforward: get leader PID for %s: %w", machineName, err)
	}

	conn, err := dialInNetNS(pid, port)
	if err != nil {
		return fmt.Errorf("portforward: dial %s port %d: %w", machineName, port, err)
	}
	defer conn.Close()

	// Propagate context cancellation.
	go func() {
		<-ctx.Done()
		conn.Close()
		stream.Close()
	}()

	errCh := make(chan error, 2)
	go func() {
		_, err := io.Copy(conn, stream)
		errCh <- err
	}()
	go func() {
		_, err := io.Copy(stream, conn)
		errCh <- err
	}()

	// Return when either direction finishes (or errors). The deferred
	// conn.Close() and stream.Close() will unblock the other direction.
	<-errCh
	return nil
}

// dialInNetNS enters the network namespace of pid, dials 127.0.0.1:port,
// then restores the calling thread's original network namespace.
// The returned conn is bound to the container's network stack.
func dialInNetNS(pid int, port int32) (net.Conn, error) {
	// Lock to a single OS thread: setns(2) applies per-thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Save our current netns so we can restore it after dialing.
	origNS, err := unix.Open("/proc/self/ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open host netns: %w", err)
	}
	defer unix.Close(origNS)

	// Open the container's netns.
	containerNSPath := fmt.Sprintf("/proc/%d/ns/net", pid)
	containerNS, err := unix.Open(containerNSPath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open container netns %s: %w", containerNSPath, err)
	}
	defer unix.Close(containerNS)

	// Enter the container's netns.
	if err := unix.Setns(containerNS, unix.CLONE_NEWNET); err != nil {
		return nil, fmt.Errorf("setns into container netns: %w", err)
	}

	// Dial while inside the container's netns.
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, dialErr := net.Dial("tcp", addr)

	// Always restore the original netns, regardless of dial outcome.
	if err := unix.Setns(origNS, unix.CLONE_NEWNET); err != nil {
		// Unrecoverable: the thread is now in an unknown netns. Terminate it.
		panic(fmt.Sprintf("portforward: failed to restore host netns: %v", err))
	}

	if dialErr != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, dialErr)
	}
	return conn, nil
}

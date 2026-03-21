package systemd

import (
	"bufio"
	"fmt"
	"os"
	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
)

// stdioLogProps creates an os.Pipe, starts a goroutine that logs each line
// from the read end via slog, and returns the systemd properties that wire
// the unit's stdout/stderr to the write end.
//
// The mechanism: we set the D-Bus property StandardOutputFile=/proc/<pid>/fd/<n>
// (which corresponds to unit-file syntax StandardOutput=file:/proc/<pid>/fd/<n>).
// When systemd starts the unit it opens that path, obtaining a dup of our pipe's
// write end — without any D-Bus Unix-FD passing.  The container then inherits
// that fd (via nspawn --console=pipe), so fd 2 inside the container is a real
// kernel pipe rather than a journal socket.  This prevents the ENXIO that
// occurs when a process opens /proc/self/fd/2 while fd 2 is a socket.
//
// The caller must close writeFd after StartTransientUnit returns; by that
// point systemd has already dup'd the fd so closing the Go-side copy is safe.
func (s *SystemdRuntime) stdioLogProps(containerName string) (props []dbus.Property, writeFd *os.File, err error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("create stdio pipe: %w", err)
	}

	logger := s.logger
	go func() {
		defer r.Close()
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			logger.Info(sc.Text(), "container", containerName)
		}
	}()

	// /proc/<pid>/fd/<n> is a path that other processes (systemd) can open to
	// obtain a write handle to the same pipe without D-Bus FD passing.
	//
	// D-Bus API note: the unit-file syntax "StandardOutput=file:/path" maps to
	// the D-Bus property "StandardOutputFile" with just the path as the value —
	// NOT "StandardOutput" with "file:/path".  Same for StandardError.
	path := fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), w.Fd())

	props = []dbus.Property{
		{Name: "StandardOutputFile", Value: dbusv5.MakeVariant(path)},
		{Name: "StandardErrorFile", Value: dbusv5.MakeVariant(path)},
	}
	return props, w, nil
}

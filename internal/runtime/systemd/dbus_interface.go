package systemd

import (
	"context"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
)

// systemdDBus defines the subset of *dbus.Conn used by SystemdRuntime.
type systemdDBus interface {
	Close()
	Subscribe() error
	SetPropertiesSubscriber(chan<- *dbus.PropertiesUpdate, chan<- error)
	StartTransientUnitContext(ctx context.Context, name string, mode string, properties []dbus.Property, ch chan<- string) (int, error)
	StartUnitContext(ctx context.Context, name string, mode string, ch chan<- string) (int, error)
	StopUnitContext(ctx context.Context, name string, mode string, ch chan<- string) (int, error)
	ResetFailedUnitContext(ctx context.Context, name string) error
	GetUnitPropertyContext(ctx context.Context, unit string, property string) (*dbus.Property, error)
	GetServicePropertyContext(ctx context.Context, unit string, property string) (*dbus.Property, error)
	ListUnitsByPatternsContext(ctx context.Context, states []string, patterns []string) ([]dbus.UnitStatus, error)
	SetUnitPropertiesContext(ctx context.Context, name string, runtime bool, properties ...dbus.Property) error
}

// machineDBus defines the subset of *dbusv5.Conn used for org.freedesktop.machine1.
type machineDBus interface {
	Close() error
	Object(dest string, path dbusv5.ObjectPath) dbusv5.BusObject
}

// signalDBus defines the subset of *dbusv5.Conn used for D-Bus signal subscriptions.
// A dedicated connection with targeted match rules avoids processing signals
// from unrelated units (go-systemd's Subscribe() matches ALL PropertiesChanged).
type signalDBus interface {
	Close() error
	Signal(ch chan<- *dbusv5.Signal)
	RemoveSignal(ch chan<- *dbusv5.Signal)
	AddMatchSignal(options ...dbusv5.MatchOption) error
}

// Ensure concrete types satisfy interfaces.
var _ systemdDBus = (*dbus.Conn)(nil)
var _ machineDBus = (*dbusv5.Conn)(nil)
var _ signalDBus = (*dbusv5.Conn)(nil)

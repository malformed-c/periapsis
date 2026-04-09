package systemd

import (
	"context"
	"fmt"
	"sync"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
)

type mockSystemdDBus struct {
	mu         sync.Mutex
	units      map[string]dbus.UnitStatus
	properties map[string]map[string]*dbus.Property

	startTransientCalled chan string
	stopUnitCalled       chan string
	resetUnitCalled      chan string

	startTransientErr error
	stopUnitErr       error

	subscriberChan chan<- *dbus.PropertiesUpdate
}

func newMockSystemdDBus() *mockSystemdDBus {
	return &mockSystemdDBus{
		units:                make(map[string]dbus.UnitStatus),
		properties:           make(map[string]map[string]*dbus.Property),
		startTransientCalled: make(chan string, 10),
		stopUnitCalled:       make(chan string, 10),
		resetUnitCalled:      make(chan string, 10),
	}
}

func (m *mockSystemdDBus) Close()           {}
func (m *mockSystemdDBus) Subscribe() error { return nil }
func (m *mockSystemdDBus) SetPropertiesSubscriber(ch chan<- *dbus.PropertiesUpdate, errCh chan<- error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscriberChan = ch
}

func (m *mockSystemdDBus) notifyWaiters(unitName, subState string) {
	m.mu.Lock()
	ch := m.subscriberChan
	m.mu.Unlock()
	if ch != nil {
		ch <- &dbus.PropertiesUpdate{
			UnitName: unitName,
			Changed: map[string]dbusv5.Variant{
				"SubState": dbusv5.MakeVariant(subState),
			},
		}
	}
}

func (m *mockSystemdDBus) StartTransientUnitContext(ctx context.Context, name string, mode string, properties []dbus.Property, ch chan<- string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startTransientCalled <- name
	if m.startTransientErr != nil {
		return 0, m.startTransientErr
	}

	m.units[name] = dbus.UnitStatus{Name: name, ActiveState: "active"}
	unitProps := make(map[string]*dbus.Property)
	for _, p := range properties {
		p := p
		unitProps[p.Name] = &p
	}
	// Default properties if not provided
	if _, ok := unitProps["ActiveState"]; !ok {
		unitProps["ActiveState"] = &dbus.Property{Name: "ActiveState", Value: dbusv5.MakeVariant("active")}
	}
	if _, ok := unitProps["SubState"]; !ok {
		unitProps["SubState"] = &dbus.Property{Name: "SubState", Value: dbusv5.MakeVariant("running")}
	}
	m.properties[name] = unitProps

	if ch != nil {
		go func() {
			select {
			case ch <- "done":
			case <-ctx.Done():
			}
		}()
	}
	return 1, nil
}

func (m *mockSystemdDBus) StartUnitContext(ctx context.Context, name string, mode string, ch chan<- string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch != nil {
		go func() {
			select {
			case ch <- "done":
			case <-ctx.Done():
			}
		}()
	}
	return 1, nil
}

func (m *mockSystemdDBus) SetUnitPropertiesContext(ctx context.Context, name string, runtime bool, properties ...dbus.Property) error {
	return nil
}

func (m *mockSystemdDBus) StopUnitContext(ctx context.Context, name string, mode string, ch chan<- string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopUnitCalled <- name
	if m.stopUnitErr != nil {
		return 0, m.stopUnitErr
	}
	if unit, ok := m.units[name]; ok {
		unit.ActiveState = "inactive"
		m.units[name] = unit
		m.properties[name]["ActiveState"] = &dbus.Property{Name: "ActiveState", Value: dbusv5.MakeVariant("inactive")}
		m.properties[name]["SubState"] = &dbus.Property{Name: "SubState", Value: dbusv5.MakeVariant("dead")}
	}
	if ch != nil {
		go func() {
			select {
			case ch <- "done":
			case <-ctx.Done():
			}
		}()
	}
	return 1, nil
}

func (m *mockSystemdDBus) ResetFailedUnitContext(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetUnitCalled <- name
	delete(m.units, name)
	delete(m.properties, name)
	return nil
}

func (m *mockSystemdDBus) GetUnitPropertyContext(ctx context.Context, unit string, property string) (*dbus.Property, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if props, ok := m.properties[unit]; ok {
		if p, ok := props[property]; ok {
			return p, nil
		}
	}
	if property == "ActiveState" {
		return &dbus.Property{Name: "ActiveState", Value: dbusv5.MakeVariant("inactive")}, nil
	}
	return nil, fmt.Errorf("property %s not found", property)
}

func (m *mockSystemdDBus) GetServicePropertyContext(ctx context.Context, unit string, property string) (*dbus.Property, error) {
	return m.GetUnitPropertyContext(ctx, unit, property)
}

func (m *mockSystemdDBus) ListUnitsByPatternsContext(ctx context.Context, states []string, patterns []string) ([]dbus.UnitStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var res []dbus.UnitStatus
	for _, u := range m.units {
		// Simplified pattern matching: just check if it contains "perigeos"
		res = append(res, u)
	}
	return res, nil
}

// Mocking machine1

type mockMachineDBus struct {
	objects map[dbusv5.ObjectPath]*mockBusObject
}

func newMockMachineDBus() *mockMachineDBus {
	return &mockMachineDBus{
		objects: make(map[dbusv5.ObjectPath]*mockBusObject),
	}
}

func (m *mockMachineDBus) Close() error { return nil }
func (m *mockMachineDBus) Object(dest string, path dbusv5.ObjectPath) dbusv5.BusObject {
	if obj, ok := m.objects[path]; ok {
		return obj
	}
	return &mockBusObject{path: path}
}

type mockBusObject struct {
	path        dbusv5.ObjectPath
	properties  map[string]dbusv5.Variant
	callResults map[string]any
}

func (o *mockBusObject) Call(method string, flags dbusv5.Flags, args ...any) *dbusv5.Call {
	res := o.callResults[method]
	return &dbusv5.Call{
		Args: []any{res},
	}
}

func (o *mockBusObject) CallWithContext(ctx context.Context, method string, flags dbusv5.Flags, args ...any) *dbusv5.Call {
	return o.Call(method, flags, args...)
}

func (o *mockBusObject) Go(method string, flags dbusv5.Flags, ch chan *dbusv5.Call, args ...any) *dbusv5.Call {
	return nil
}

func (o *mockBusObject) GoWithContext(ctx context.Context, method string, flags dbusv5.Flags, ch chan *dbusv5.Call, args ...any) *dbusv5.Call {
	return nil
}

func (o *mockBusObject) AddMatchSignal(iface, member string, options ...dbusv5.MatchOption) *dbusv5.Call {
	return &dbusv5.Call{}
}

func (o *mockBusObject) RemoveMatchSignal(iface, member string, options ...dbusv5.MatchOption) *dbusv5.Call {
	return &dbusv5.Call{}
}

func (o *mockBusObject) GetProperty(p string) (dbusv5.Variant, error) {
	if v, ok := o.properties[p]; ok {
		return v, nil
	}
	return dbusv5.Variant{}, fmt.Errorf("property %s not found", p)
}

func (o *mockBusObject) SetProperty(p string, v any) error   { return nil }
func (o *mockBusObject) StoreProperty(p string, v any) error { return nil }
func (o *mockBusObject) Destination() string                 { return "" }
func (o *mockBusObject) Path() dbusv5.ObjectPath             { return o.path }

// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package fixtures

import (
	"context"
	"fmt"
	"sync"

	"github.com/coreos/go-systemd/v22/dbus"
	dbusv5 "github.com/godbus/dbus/v5"
)

// SystemdDBusFixture implements internal/runtime/systemd/dbus_interface.go for testing.
type SystemdDBusFixture struct {
	Mu         sync.Mutex
	Units      map[string]dbus.UnitStatus
	Properties map[string]map[string]*dbus.Property

	StartTransientCalled chan string
	StopUnitCalled       chan string
	ResetUnitCalled      chan string

	StartTransientErr error
	StopUnitErr       error

	SubscriberChan chan<- *dbus.PropertiesUpdate
}

func NewSystemdDBusFixture() *SystemdDBusFixture {
	return &SystemdDBusFixture{
		Units:                make(map[string]dbus.UnitStatus),
		Properties:           make(map[string]map[string]*dbus.Property),
		StartTransientCalled: make(chan string, 100),
		StopUnitCalled:       make(chan string, 100),
		ResetUnitCalled:      make(chan string, 100),
	}
}

func (m *SystemdDBusFixture) Close()           {}
func (m *SystemdDBusFixture) Subscribe() error { return nil }
func (m *SystemdDBusFixture) SetPropertiesSubscriber(ch chan<- *dbus.PropertiesUpdate, errCh chan<- error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.SubscriberChan = ch
}

func (m *SystemdDBusFixture) NotifyWaiters(unitName, subState string) {
	m.Mu.Lock()
	ch := m.SubscriberChan
	m.Mu.Unlock()
	if ch != nil {
		ch <- &dbus.PropertiesUpdate{
			UnitName: unitName,
			Changed: map[string]dbusv5.Variant{
				"SubState": dbusv5.MakeVariant(subState),
			},
		}
	}
}

func (m *SystemdDBusFixture) StartTransientUnitContext(ctx context.Context, name string, mode string, properties []dbus.Property, ch chan<- string) (int, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.StartTransientCalled <- name
	if m.StartTransientErr != nil {
		return 0, m.StartTransientErr
	}

	m.Units[name] = dbus.UnitStatus{Name: name, ActiveState: "active"}
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
	m.Properties[name] = unitProps

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

func (m *SystemdDBusFixture) StartUnitContext(ctx context.Context, name string, mode string, ch chan<- string) (int, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
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

func (m *SystemdDBusFixture) SetUnitPropertiesContext(ctx context.Context, name string, runtime bool, properties ...dbus.Property) error {
	return nil
}

func (m *SystemdDBusFixture) StopUnitContext(ctx context.Context, name string, mode string, ch chan<- string) (int, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.StopUnitCalled <- name
	if m.StopUnitErr != nil {
		return 0, m.StopUnitErr
	}
	if unit, ok := m.Units[name]; ok {
		unit.ActiveState = "inactive"
		m.Units[name] = unit
		m.Properties[name]["ActiveState"] = &dbus.Property{Name: "ActiveState", Value: dbusv5.MakeVariant("inactive")}
		m.Properties[name]["SubState"] = &dbus.Property{Name: "SubState", Value: dbusv5.MakeVariant("dead")}
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

func (m *SystemdDBusFixture) ResetFailedUnitContext(ctx context.Context, name string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.ResetUnitCalled <- name
	delete(m.Units, name)
	delete(m.Properties, name)
	return nil
}

func (m *SystemdDBusFixture) GetUnitPropertyContext(ctx context.Context, unit string, property string) (*dbus.Property, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if props, ok := m.Properties[unit]; ok {
		if p, ok := props[property]; ok {
			return p, nil
		}
	}
	if property == "ActiveState" {
		return &dbus.Property{Name: "ActiveState", Value: dbusv5.MakeVariant("inactive")}, nil
	}
	return nil, fmt.Errorf("property %s not found", property)
}

func (m *SystemdDBusFixture) GetServicePropertyContext(ctx context.Context, unit string, property string) (*dbus.Property, error) {
	return m.GetUnitPropertyContext(ctx, unit, property)
}

func (m *SystemdDBusFixture) ListUnitsByPatternsContext(ctx context.Context, states []string, patterns []string) ([]dbus.UnitStatus, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	var res []dbus.UnitStatus
	for _, u := range m.Units {
		res = append(res, u)
	}
	return res, nil
}

func (o *SystemdDBusFixture) GetManagerProperty(p string) (string, error) {
	if p == "Version" {
		return "'260.1-2-arch'", nil
	}

	return "", fmt.Errorf("property %s not found", p)
}

// MachineDBusFixture implements internal/runtime/systemd/dbus_interface.go for testing.
type MachineDBusFixture struct {
	Mu      sync.Mutex
	Objects map[dbusv5.ObjectPath]*BusObjectFixture
}

func NewMachineDBusFixture() *MachineDBusFixture {
	return &MachineDBusFixture{
		Objects: make(map[dbusv5.ObjectPath]*BusObjectFixture),
	}
}

func (m *MachineDBusFixture) Close() error { return nil }
func (m *MachineDBusFixture) Object(dest string, path dbusv5.ObjectPath) dbusv5.BusObject {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if obj, ok := m.Objects[path]; ok {
		return obj
	}
	return &BusObjectFixture{PathVal: path}
}

type BusObjectFixture struct {
	Mu          sync.Mutex
	PathVal     dbusv5.ObjectPath
	Properties  map[string]dbusv5.Variant
	CallResults map[string]any
}

func (o *BusObjectFixture) Call(method string, flags dbusv5.Flags, args ...any) *dbusv5.Call {
	o.Mu.Lock()
	defer o.Mu.Unlock()
	res := o.CallResults[method]
	return &dbusv5.Call{
		Args: []any{res},
	}
}

func (o *BusObjectFixture) CallWithContext(ctx context.Context, method string, flags dbusv5.Flags, args ...any) *dbusv5.Call {
	return o.Call(method, flags, args...)
}

func (o *BusObjectFixture) Go(method string, flags dbusv5.Flags, ch chan *dbusv5.Call, args ...any) *dbusv5.Call {
	return nil
}

func (o *BusObjectFixture) GoWithContext(ctx context.Context, method string, flags dbusv5.Flags, ch chan *dbusv5.Call, args ...any) *dbusv5.Call {
	return nil
}

func (o *BusObjectFixture) AddMatchSignal(iface, member string, options ...dbusv5.MatchOption) *dbusv5.Call {
	return &dbusv5.Call{}
}

func (o *BusObjectFixture) RemoveMatchSignal(iface, member string, options ...dbusv5.MatchOption) *dbusv5.Call {
	return &dbusv5.Call{}
}

func (o *BusObjectFixture) GetProperty(p string) (dbusv5.Variant, error) {
	o.Mu.Lock()
	defer o.Mu.Unlock()
	if v, ok := o.Properties[p]; ok {
		return v, nil
	}
	return dbusv5.Variant{}, fmt.Errorf("property %s not found", p)
}

func (o *BusObjectFixture) SetProperty(p string, v any) error {
	o.Mu.Lock()
	defer o.Mu.Unlock()
	return nil
}
func (o *BusObjectFixture) StoreProperty(p string, v any) error {
	o.Mu.Lock()
	defer o.Mu.Unlock()
	return nil
}
func (o *BusObjectFixture) Destination() string     { return "" }
func (o *BusObjectFixture) Path() dbusv5.ObjectPath { return o.PathVal }

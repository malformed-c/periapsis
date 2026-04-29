package systemd

import (
	"github.com/malformed-c/periapsis/internal/test/fixtures"
)

type mockMachineDBus = fixtures.MachineDBusFixture
type mockBusObject = fixtures.BusObjectFixture

func newMockSystemdDBus() *fixtures.SystemdDBusFixture {
	return fixtures.NewSystemdDBusFixture()
}

func newMockMachineDBus() *fixtures.MachineDBusFixture {
	return fixtures.NewMachineDBusFixture()
}

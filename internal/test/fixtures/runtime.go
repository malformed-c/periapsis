// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package fixtures

import (
	"context"
	"io"
	"sync"
	"time"

	perigeos "github.com/malformed-c/periapsis/internal/runtime"
	"github.com/malformed-c/periapsis/node/api"
)

// RuntimeFixture implements perigeos.Runtime for testing.
type RuntimeFixture struct {
	Mu sync.Mutex

	Machines []perigeos.PodMetadata
	Stopped  []string

	// Default states for various methods
	DefaultMachineState perigeos.MachineState
	DefaultExitState    perigeos.MachineState

	// Error injectors
	RunMachineErr    error
	StopMachineErr   error
	CheckMachinedErr error

	// Event subscription
	EventsChan chan perigeos.UnitEvent
}

func NewRuntimeFixture() *RuntimeFixture {
	return &RuntimeFixture{
		DefaultMachineState: perigeos.StateRunning,
		DefaultExitState:    perigeos.StateExited,
		EventsChan:          make(chan perigeos.UnitEvent, 100),
	}
}

func (r *RuntimeFixture) RunMachine(_ context.Context, podUID string, cfg perigeos.PodConfig) error {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	if r.RunMachineErr != nil {
		return r.RunMachineErr
	}
	r.Machines = append(r.Machines, perigeos.PodMetadata{
		UID:           podUID,
		Name:          cfg.Name,
		Namespace:     cfg.Namespace,
		ContainerName: cfg.ContainerName,
		State:         perigeos.StateRunning,
	})
	return nil
}

func (r *RuntimeFixture) StopMachine(_ context.Context, uid, containerName string) error {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	if r.StopMachineErr != nil {
		return r.StopMachineErr
	}
	r.Stopped = append(r.Stopped, uid+"/"+containerName)
	return nil
}

func (r *RuntimeFixture) MachineStatus(_ context.Context, _, _ string) (perigeos.MachineState, error) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.DefaultMachineState, nil
}

func (r *RuntimeFixture) WaitForMachineExit(_ context.Context, _, _ string, _ time.Duration) (perigeos.MachineState, error) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.DefaultExitState, nil
}

func (r *RuntimeFixture) ListManagedMachines(_ context.Context) ([]perigeos.PodMetadata, error) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.Machines, nil
}

func (r *RuntimeFixture) GetLogStream(_ context.Context, _, _ string, _ api.ContainerLogOpts) (io.ReadCloser, error) {
	return io.NopCloser(nil), nil
}

func (r *RuntimeFixture) RunInContainer(_ context.Context, _, _ string, _ []string, _ api.AttachIO) error {
	return nil
}

func (r *RuntimeFixture) AttachContainer(_ context.Context, _, _ string, _ api.AttachIO) error {
	return nil
}

func (r *RuntimeFixture) InitPawnSlice(_ context.Context, _ perigeos.PawnSliceConfig) error {
	return nil
}

func (r *RuntimeFixture) CheckMachined(_ context.Context) error {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.CheckMachinedErr
}

func (r *RuntimeFixture) SubscribeEvents(_ context.Context) <-chan perigeos.UnitEvent {
	return r.EventsChan
}

func (r *RuntimeFixture) MakeSharedMounts(_ context.Context, _, _ string, _ []perigeos.BindMount) error {
	return nil
}

func (r *RuntimeFixture) ResetUnit(_ context.Context, _, _ string) error {
	return nil
}

func (r *RuntimeFixture) CleanupStaleUnits(_ context.Context, _ map[string]bool) (int, error) {
	return 0, nil
}

func (r *RuntimeFixture) MStackSupported() bool {
	return true
}

func (r *RuntimeFixture) SliceActive(ctx context.Context) bool {
	return true
}

func (r *RuntimeFixture) PortForward(ctx context.Context, podUID, containerName string, port int32, stream io.ReadWriteCloser) error {
	return nil
}

func (r *RuntimeFixture) GetContainerExitInfo(_ context.Context, _, _ string) perigeos.ContainerExitInfo {
	return perigeos.ContainerExitInfo{}
}

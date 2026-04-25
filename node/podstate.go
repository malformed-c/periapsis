// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
)

// PersistedPodState is the on-disk representation of a running pod.
// Written atomically to <baseDir>/pawns/<pawn>/pods/<uid>/pod-state.json.
// Survives perigeos restarts; on host reboot the state is used to rediscover
// what pods were running and restart them per their restartPolicy.
type PersistedPodState struct {
	// Pod is the full pod spec as delivered by the VK informer.
	Pod *corev1.Pod `json:"pod"`

	// PodIP is the IP assigned by CNI. Not in the pod spec.
	PodIP string `json:"podIP,omitempty"`

	// Phase is the pod phase at last update.
	Phase corev1.PodPhase `json:"phase"`

	// Restarts is the per-container restart count at last update.
	Restarts map[string]int32 `json:"restarts,omitempty"`

	// Backoffs is the per-container CrashLoopBackOff duration in seconds.
	// Persisted alongside restart counts so that backoff delays survive
	// perigeos restarts. Without this, a container that had accumulated
	// a 5-minute backoff resets to 10 seconds after restart.
	Backoffs map[string]float64 `json:"backoffs,omitempty"`
}

const podStateFile = "pod-state.json"

// podStateDir returns the directory path for a pod's persistent state.
func podStateDir(baseDir, pawnName, uid string) string {
	return filepath.Join(baseDir, "pawns", pawnName, "pods", uid)
}

// writePodState atomically writes the pod state to disk.
// Uses write-to-temp + rename for crash safety.
func writePodState(baseDir, pawnName string, state *PersistedPodState) error {
	uid := string(state.Pod.UID)
	dir := podStateDir(baseDir, pawnName, uid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create pod state dir: %w", err)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal pod state: %w", err)
	}

	dest := filepath.Join(dir, podStateFile)
	tmp := dest + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write pod state tmp: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return fmt.Errorf("rename pod state: %w", err)
	}
	return nil
}

// readPodState reads the pod state from disk.
func readPodState(baseDir, pawnName, uid string) (*PersistedPodState, error) {
	path := filepath.Join(podStateDir(baseDir, pawnName, uid), podStateFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state PersistedPodState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal pod state: %w", err)
	}
	return &state, nil
}

// deletePodState removes the pod state file. The directory itself is
// managed by the volume resolver cleanup - we only remove the state file.
func deletePodState(baseDir, pawnName, uid string) error {
	path := filepath.Join(podStateDir(baseDir, pawnName, uid), podStateFile)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// loadAllPodStates scans the pods directory and returns all valid pod states.
func loadAllPodStates(baseDir, pawnName string) ([]*PersistedPodState, error) {
	podsDir := filepath.Join(baseDir, "pawns", pawnName, "pods")
	entries, err := os.ReadDir(podsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read pods dir: %w", err)
	}

	var states []*PersistedPodState
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		uid := e.Name()
		state, err := readPodState(baseDir, pawnName, uid)
		if err != nil {
			if os.IsNotExist(err) {
				continue // no state file, just volume dirs
			}
			// Log and skip corrupt state files rather than failing startup.
			continue
		}
		if state.Pod == nil {
			continue
		}
		states = append(states, state)
	}
	return states, nil
}

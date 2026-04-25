// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

// Package stats implements the kubelet /stats/summary endpoint.
// The types here are a minimal subset of k8s.io/kubelet/pkg/apis/stats/v1alpha1,
// containing only the fields that metrics-server reads.
package stats

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Summary is the top-level response for GET /stats/summary.
type Summary struct {
	Node NodeStats  `json:"node"`
	Pods []PodStats `json:"pods"`
}

// NodeStats contains resource usage for the node itself.
type NodeStats struct {
	NodeName         string           `json:"nodeName"`
	SystemContainers []ContainerStats `json:"systemContainers,omitempty"`
	StartTime        metav1.Time      `json:"startTime"`
	CPU              *CPUStats        `json:"cpu,omitempty"`
	Memory           *MemoryStats     `json:"memory,omitempty"`
}

// PodStats contains resource usage for a single pod.
type PodStats struct {
	PodRef     PodReference     `json:"podRef"`
	StartTime  metav1.Time      `json:"startTime"`
	Containers []ContainerStats `json:"containers"`
	CPU        *CPUStats        `json:"cpu,omitempty"`
	Memory     *MemoryStats     `json:"memory,omitempty"`
}

// PodReference identifies a pod.
type PodReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}

// ContainerStats contains resource usage for a single container.
type ContainerStats struct {
	Name      string       `json:"name"`
	StartTime metav1.Time  `json:"startTime"`
	CPU       *CPUStats    `json:"cpu,omitempty"`
	Memory    *MemoryStats `json:"memory,omitempty"`
}

// CPUStats contains CPU usage.
type CPUStats struct {
	Time                 metav1.Time `json:"time"`
	UsageNanoCores       *uint64     `json:"usageNanoCores,omitempty"`
	UsageCoreNanoSeconds *uint64     `json:"usageCoreNanoSeconds,omitempty"`
}

// MemoryStats contains memory usage.
type MemoryStats struct {
	Time            metav1.Time `json:"time"`
	UsageBytes      *uint64     `json:"usageBytes,omitempty"`
	WorkingSetBytes *uint64     `json:"workingSetBytes,omitempty"`
}

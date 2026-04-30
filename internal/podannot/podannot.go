// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

// Package podannot defines periapsis.io annotation keys for pods and
// provides typed extraction helpers so callers never parse raw annotation
// strings themselves.
//
// All keys live under the periapsis.io/ prefix. Node labels (periapsis.io/host,
// periapsis.io/primary) are not pod annotations and are not covered here.
//
// # Swap annotation
//
// periapsis.io/swap controls the swap cgroup limit for every container in
// the pod. Supported values:
//
//   - absent            default behaviour (2× memory limit)
//   - "disabled" / "0"  no swap (SwapLimitBytes == 0)
//   - "Nx"              N× the container's memory limit  (e.g. "3x")
//   - resource.Quantity explicit byte limit              (e.g. "512Mi")
//
// The multiplier form is intentionally simple — swap is almost always
// expressed relative to RAM. An explicit quantity is available for workloads
// that need a fixed ceiling regardless of the memory limit.
package podannot

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Annotation keys for pods.
const (
	// AnnotationSwap controls the swap limit for containers in this pod.
	// See package-level doc for the full value syntax.
	AnnotationSwap = "periapsis.io/swap"
)

// defaultSwapMultiplier is used when the swap annotation is absent.
const defaultSwapMultiplier = 2

// SwapBytes returns the swap limit in bytes for a container with the given
// memory limit, according to the pod's periapsis.io/swap annotation.
//
// memBytes == 0 always yields swapBytes == 0 (nothing to base a multiplier on).
func SwapBytes(pod *corev1.Pod, memBytes uint64) (uint64, error) {
	if memBytes == 0 {
		return 0, nil
	}

	raw, ok := pod.Annotations[AnnotationSwap]
	if !ok {
		// No annotation: apply the default multiplier.
		return memBytes * defaultSwapMultiplier, nil
	}

	return parseSwapValue(raw, memBytes)
}

// parseSwapValue parses the raw annotation string into a byte count.
func parseSwapValue(raw string, memBytes uint64) (uint64, error) {
	raw = strings.TrimSpace(raw)

	switch strings.ToLower(raw) {
	case "disabled", "0", "off", "none":
		return 0, nil
	}

	// Multiplier form: "2x", "3x", etc.
	if lower := strings.ToLower(raw); strings.HasSuffix(lower, "x") {
		numStr := lower[:len(lower)-1]
		n, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil || n == 0 {
			return 0, fmt.Errorf("periapsis.io/swap: invalid multiplier %q (want e.g. \"2x\")", raw)
		}
		return memBytes * n, nil
	}

	// Explicit quantity: "512Mi", "1Gi", etc.
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return 0, fmt.Errorf("periapsis.io/swap: cannot parse %q as a multiplier or resource quantity: %w", raw, err)
	}
	if q.Sign() < 0 {
		return 0, fmt.Errorf("periapsis.io/swap: quantity must be non-negative, got %q", raw)
	}
	bytes, ok := q.AsInt64()
	if !ok {
		return 0, fmt.Errorf("periapsis.io/swap: quantity %q overflows int64", raw)
	}
	return uint64(bytes), nil
}

// String returns the value of the named annotation, or defaultVal if absent.
func String(pod *corev1.Pod, key, defaultVal string) string {
	if v, ok := pod.Annotations[key]; ok {
		return v
	}
	return defaultVal
}

// Bool returns the boolean value of the named annotation.
// Accepted true values:  "true", "1", "yes", "on"  (case-insensitive)
// Accepted false values: "false", "0", "no", "off" (case-insensitive)
// Returns (defaultVal, nil) when the annotation is absent.
// Returns (false, err) for any unrecognised value.
func Bool(pod *corev1.Pod, key string, defaultVal bool) (bool, error) {
	raw, ok := pod.Annotations[key]
	if !ok {
		return defaultVal, nil
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("annotation %q: cannot parse %q as bool", key, raw)
	}
}

// Quantity returns the resource.Quantity value of the named annotation.
// Returns (defaultVal, nil) when the annotation is absent.
// Returns (zero, err) for any unparseable value.
func Quantity(pod *corev1.Pod, key string, defaultVal resource.Quantity) (resource.Quantity, error) {
	raw, ok := pod.Annotations[key]
	if !ok {
		return defaultVal, nil
	}
	q, err := resource.ParseQuantity(strings.TrimSpace(raw))
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("annotation %q: %w", key, err)
	}
	return q, nil
}

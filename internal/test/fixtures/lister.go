// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package fixtures

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// PodListerFixture implements a simple pod lister for testing.
type PodListerFixture struct {
	Pods []*corev1.Pod
}

func (m *PodListerFixture) List(_ labels.Selector) ([]*corev1.Pod, error) {
	return m.Pods, nil
}

func (m *PodListerFixture) Get(name string) (*corev1.Pod, error) {
	for _, p := range m.Pods {
		if p.Name == name {
			return p, nil
		}
	}
	return nil, nil
}

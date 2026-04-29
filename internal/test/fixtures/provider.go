// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package fixtures

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/malformed-c/periapsis/errdefs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProviderFixture implements both PodProvider and NodeProvider.
type ProviderFixture struct {
	mu sync.Mutex

	// PodProvider state
	Pods sync.Map

	Creates          int
	Updates          int
	Deletes          int
	AttemptedDeletes int

	ErrorOnDelete error

	PodNotifier func(*corev1.Pod)

	// NodeProvider state
	Node         *corev1.Node
	NodeNotifier func(*corev1.Node)
}

func NewProviderFixture() *ProviderFixture {
	return &ProviderFixture{}
}

// --- PodProvider implementation ---

func (p *ProviderFixture) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	p.mu.Lock()
	p.Creates++
	p.mu.Unlock()

	key := buildKey(pod)
	now := metav1.NewTime(time.Now())

	podCopy := pod.DeepCopy()
	podCopy.Status = corev1.PodStatus{
		Phase:     corev1.PodRunning,
		StartTime: &now,
	}

	p.Pods.Store(key, podCopy)
	if p.PodNotifier != nil {
		p.PodNotifier(podCopy)
	}
	return nil
}

func (p *ProviderFixture) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	p.mu.Lock()
	p.Updates++
	p.mu.Unlock()

	key := buildKey(pod)
	podCopy := pod.DeepCopy()
	p.Pods.Store(key, podCopy)
	if p.PodNotifier != nil {
		p.PodNotifier(podCopy)
	}
	return nil
}

func (p *ProviderFixture) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	p.mu.Lock()
	p.AttemptedDeletes++
	err := p.ErrorOnDelete
	p.mu.Unlock()

	if err != nil {
		return err
	}

	key := buildKey(pod)
	if _, ok := p.Pods.Load(key); !ok {
		return errdefs.NotFound("pod not found")
	}

	p.mu.Lock()
	p.Deletes++
	p.mu.Unlock()

	p.Pods.Delete(key)

	podCopy := pod.DeepCopy()
	podCopy.Status.Phase = corev1.PodSucceeded
	if p.PodNotifier != nil {
		p.PodNotifier(podCopy)
	}
	return nil
}

func (p *ProviderFixture) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	key := fmt.Sprintf("%s/%s", namespace, name)
	if pod, ok := p.Pods.Load(key); ok {
		return pod.(*corev1.Pod).DeepCopy(), nil
	}
	return nil, errdefs.NotFound("pod not found")
}

func (p *ProviderFixture) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}

func (p *ProviderFixture) GetPods(ctx context.Context) ([]*corev1.Pod, error) {
	var pods []*corev1.Pod
	p.Pods.Range(func(_, value interface{}) bool {
		pods = append(pods, value.(*corev1.Pod).DeepCopy())
		return true
	})
	return pods, nil
}

func (p *ProviderFixture) NotifyPods(ctx context.Context, f func(*corev1.Pod)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.PodNotifier = f
}

// --- NodeProvider implementation ---

func (p *ProviderFixture) Ping(ctx context.Context) error {
	return nil
}

func (p *ProviderFixture) NotifyNodeStatus(ctx context.Context, f func(*corev1.Node)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.NodeNotifier = f
}

func (p *ProviderFixture) TriggerNodeStatusUpdate(n *corev1.Node) {
	p.mu.Lock()
	notifier := p.NodeNotifier
	p.mu.Unlock()
	if notifier != nil {
		notifier(n)
	}
}

func buildKey(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}

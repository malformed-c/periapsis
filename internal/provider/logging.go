package provider

// LoggingProvider wraps a Gambit and logs every VK provider interface call
// at entry and on error. This surfaces errors that VK swallows internally
// before they reach gambit — if a call never appears in logs, VK never made it.

import (
	"context"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
)

// LoggingProvider wraps Gambit to log all provider interface calls.
type LoggingProvider struct {
	inner  *Gambit
	logger *slog.Logger
}

// NewLoggingProvider wraps g with call-level logging.
func NewLoggingProvider(g *Gambit, logger *slog.Logger) *LoggingProvider {
	return &LoggingProvider{inner: g, logger: logger}
}

// Unwrap returns the underlying Gambit, used by perigeos internals that need
// direct access (e.g. RegisterGambit, apsis control server).
func (l *LoggingProvider) Unwrap() *Gambit { return l.inner }

func (l *LoggingProvider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	l.logger.Info("provider.CreatePod →", "namespace", pod.Namespace, "pod", pod.Name, "uid", pod.UID)
	err := l.inner.CreatePod(ctx, pod)
	if err != nil {
		l.logger.Error("provider.CreatePod ✗", "namespace", pod.Namespace, "pod", pod.Name, "err", err)
	}
	return err
}

func (l *LoggingProvider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	l.logger.Info("provider.UpdatePod →", "namespace", pod.Namespace, "pod", pod.Name)
	err := l.inner.UpdatePod(ctx, pod)
	if err != nil {
		l.logger.Error("provider.UpdatePod ✗", "namespace", pod.Namespace, "pod", pod.Name, "err", err)
	}
	return err
}

func (l *LoggingProvider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	l.logger.Info("provider.DeletePod →", "namespace", pod.Namespace, "pod", pod.Name)
	err := l.inner.DeletePod(ctx, pod)
	if err != nil {
		l.logger.Error("provider.DeletePod ✗", "namespace", pod.Namespace, "pod", pod.Name, "err", err)
	}
	return err
}

func (l *LoggingProvider) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	pod, err := l.inner.GetPod(ctx, namespace, name)
	if err != nil {
		l.logger.Warn("provider.GetPod ✗", "namespace", namespace, "pod", name, "err", err)
	}
	return pod, err
}

func (l *LoggingProvider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	status, err := l.inner.GetPodStatus(ctx, namespace, name)
	if err != nil {
		l.logger.Warn("provider.GetPodStatus ✗", "namespace", namespace, "pod", name, "err", err)
	}
	return status, err
}

func (l *LoggingProvider) GetPods(ctx context.Context) ([]*corev1.Pod, error) {
	pods, err := l.inner.GetPods(ctx)
	if err != nil {
		l.logger.Error("provider.GetPods ✗", "err", err)
	}
	return pods, err
}

func (l *LoggingProvider) Ping(ctx context.Context) error {
	err := l.inner.Ping(ctx)
	if err != nil {
		l.logger.Error("provider.Ping ✗", "err", err)
	}
	return err
}

func (l *LoggingProvider) NotifyNodeStatus(ctx context.Context, cb func(*corev1.Node)) {
	l.inner.NotifyNodeStatus(ctx, cb)
}

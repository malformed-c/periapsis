package node

import (
	"context"
	"fmt"
	"io"

	"github.com/malformed-c/periapsis/node/api"
)

func (g *Gambit) PortForward(ctx context.Context, namespace, podName string, port int32, stream io.ReadWriteCloser) error {
	uid, err := g.findPodUID(namespace, podName)
	if err != nil {
		return err
	}
	// PortForward targets the first running container - pick any; all share the pod netns.
	pod, err := g.store.GetPod(namespace, podName)
	if err != nil {
		return fmt.Errorf("portforward: get pod %s/%s: %w", namespace, podName, err)
	}
	if len(pod.Spec.Containers) == 0 {
		return fmt.Errorf("portforward: pod %s/%s has no containers", namespace, podName)
	}
	containerName := pod.Spec.Containers[0].Name
	return g.Runtime.PortForward(ctx, uid, containerName, port, stream)
}

func (g *Gambit) GetContainerLogs(
	ctx context.Context,
	namespace, podName, containerName string,
	opts api.ContainerLogOpts,
) (io.ReadCloser, error) {
	g.Logger.Info("GetContainerLogs", "pawn", g.Config.Name, "namespace", namespace, "pod", podName, "container", containerName)

	// Try to find the pod by namespace and name.
	uid, err := g.store.FindPodUID(namespace, podName)
	if err != nil {
		// Fall back to completed pods - journal entries survive after DeletePod
		// removes the pod from the store.
		uid = g.store.CompletedPodUID(namespace, podName)
		if uid == "" {
			return nil, fmt.Errorf("pod %s/%s not found", namespace, podName)
		}
	}

	return g.Runtime.GetLogStream(ctx, uid, containerName, opts)
}

func (g *Gambit) AttachContainer(
	ctx context.Context,
	namespace, podName, containerName string,
	attach api.AttachIO,
) error {
	uid, err := g.findPodUID(namespace, podName)
	if err != nil {
		return err
	}
	return g.Runtime.AttachContainer(ctx, uid, containerName, attach)
}

func (g *Gambit) RunInContainer(
	ctx context.Context,
	namespace, podName, containerName string,
	cmd []string,
	attach api.AttachIO,
) error {
	uid, err := g.findPodUID(namespace, podName)
	if err != nil {
		return err
	}

	return g.Runtime.RunInContainer(ctx, uid, containerName, cmd, attach)
}

func (g *Gambit) findPodUID(namespace, podName string) (string, error) {
	return g.store.FindPodUID(namespace, podName)
}

// --- Node Conditions ---------------------------------------------------------

// setKind restores Pod TypeMeta stripped by client-go informers.
// Required for the EventRecorder to construct object references correctly.

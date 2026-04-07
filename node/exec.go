package node

import (
	"context"
	"fmt"
	"io"

	"github.com/malformed-c/periapsis/node/api"
)

func (g *Gambit) GetContainerLogs(
	ctx context.Context,
	namespace, podName, containerName string,
	opts api.ContainerLogOpts,
) (io.ReadCloser, error) {
	g.Logger.Info("GetContainerLogs", "pawn", g.Config.Name, "namespace", namespace, "pod", podName, "container", containerName)

	// Try to find the pod by namespace and name.
	uid, err := g.store.FindPodUID(namespace, podName)
	if err != nil {
		// Fall back to completed pods — journal entries survive after DeletePod
		// removes the pod from the store.
		uid = g.store.CompletedPodUID(namespace, podName)
		if uid == "" {
			return nil, fmt.Errorf("pod %s/%s not found", namespace, podName)
		}
	}

	return g.Runtime.GetLogStream(ctx, uid, containerName, opts)
}

func (g *Gambit) AttachToContainer(
	ctx context.Context,
	namespace, podName, containerName string,
	attach api.AttachIO,
) error {
	uid, err := g.findPodUID(namespace, podName)
	if err != nil {
		return err
	}
	return g.Runtime.AttachToContainer(ctx, uid, containerName, attach)
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

// ─── Node Conditions ─────────────────────────────────────────────────────────

// setKind restores Pod TypeMeta stripped by client-go informers.
// Required for the EventRecorder to construct object references correctly.

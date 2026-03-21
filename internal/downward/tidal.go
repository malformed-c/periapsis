package downward

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// Tidal handles translation of Kubernetes Downward API fields into
// concrete environment variable strings for nspawn containers.
type Tidal struct {
	NodeName string
	NodeIP   string

	// APIServerHost and APIServerPort are injected as
	// KUBERNETES_SERVICE_HOST / KUBERNETES_SERVICE_PORT into every pod,
	// matching real kubelet behaviour. Required for in-cluster clients.
	APIServerHost string
	APIServerPort string
}

func NewTidal(nodeName, nodeIP string) *Tidal {
	return &Tidal{
		NodeName: nodeName,
		NodeIP:   nodeIP,
	}
}

// SetAPIServer configures the API server address injected into pods.
// host can be "host:port" or bare "host" (defaults to port 443).
func (t *Tidal) SetAPIServer(host, port string) {
	t.APIServerHost = host
	t.APIServerPort = port
	if t.APIServerPort == "" {
		t.APIServerPort = "443"
	}
}

// ResolveEnv iterates through a container's requested environment variables
// and resolves any FieldRefs using the provided runtime context (pod IP, node IP, etc).
//
// The result is deduplicated by key (last writer wins). systemd ≥ 259 rejects
// duplicate keys in the Environment property of transient units.
func (t *Tidal) ResolveEnv(pod *corev1.Pod, container *corev1.Container, podIP string) []string {
	// Use ordered key list + map so overrides replace spec values in-place
	// while preserving insertion order for non-overridden keys.
	seen := make(map[string]int) // key → index in resolved
	var resolved []string

	set := func(key, val string) {
		if idx, ok := seen[key]; ok {
			resolved[idx] = fmt.Sprintf("%s=%s", key, val)
			return
		}
		seen[key] = len(resolved)
		resolved = append(resolved, fmt.Sprintf("%s=%s", key, val))
	}

	for _, env := range container.Env {
		switch {
		case env.Value != "":
			set(env.Name, env.Value)

		case env.ValueFrom != nil && env.ValueFrom.FieldRef != nil:
			set(env.Name, t.resolveFieldPath(pod, env.ValueFrom.FieldRef.FieldPath, podIP))

		default:
			// ConfigMaps / Secrets — not supported yet
			continue
		}
	}

	// Force-inject standard metadata so containers always have these,
	// regardless of whether they were requested in the pod spec.
	// These override anything set above to prevent spoofing.
	set("MY_POD_IP", podIP)
	set("MY_HOST_IP", t.NodeIP)
	set("MY_NODE_NAME", t.NodeName)
	set("MY_POD_UID", string(pod.UID))
	set("MY_POD_NAME", pod.Name)
	set("MY_POD_NS", pod.Namespace)

	if t.APIServerHost != "" {
		set("KUBERNETES_SERVICE_HOST", t.APIServerHost)
		set("KUBERNETES_SERVICE_PORT", t.APIServerPort)
		set("KUBERNETES_SERVICE_PORT_HTTPS", t.APIServerPort)
	}

	// Strip entries with empty keys (shouldn't happen, but be defensive).
	filtered := resolved[:0]
	for _, entry := range resolved {
		if idx := strings.IndexByte(entry, '='); idx > 0 {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func (t *Tidal) resolveFieldPath(pod *corev1.Pod, path, podIP string) string {
	switch path {
	case "metadata.name":
		return pod.Name
	case "metadata.namespace":
		return pod.Namespace
	case "metadata.uid":
		return string(pod.UID)
	case "metadata.labels":
		return "" // TODO: specific key lookup
	case "metadata.annotations":
		return ""
	case "spec.nodeName":
		return t.NodeName
	case "spec.serviceAccountName":
		return pod.Spec.ServiceAccountName
	case "status.hostIP":
		return t.NodeIP
	case "status.podIP":
		return podIP
	case "status.podIPs":
		return podIP // MVP: single stack
	}
	return ""
}

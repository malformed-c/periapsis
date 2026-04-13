package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// internalIPFromCiliumNode returns the first InternalIP address in the
// CiliumNode spec, or "" if none is set.
func internalIPFromCiliumNode(u *unstructured.Unstructured) string {
	addrs, found, err := unstructured.NestedSlice(u.Object, "spec", "addresses")
	if err != nil || !found {
		return ""
	}
	for _, a := range addrs {
		m, ok := a.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := m["type"].(string); t == "InternalIP" {
			if ip, _ := m["ip"].(string); ip != "" {
				return ip
			}
		}
	}
	return ""
}

// updatedAddresses returns the existing addresses slice with InternalIP
// replaced (or appended). Preserves other address types like CiliumInternalIP.
func updatedAddresses(u *unstructured.Unstructured, newIP string) []any {
	addrs, _, _ := unstructured.NestedSlice(u.Object, "spec", "addresses")
	entry := map[string]any{"type": "InternalIP", "ip": newIP}

	for i, a := range addrs {
		m, ok := a.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := m["type"].(string); t == "InternalIP" {
			addrs[i] = entry
			return addrs
		}
	}
	return append(addrs, entry)
}

// labelsMatch checks whether all desired labels are already present on the object.
func labelsMatch(existing map[string]string, desired map[string]string) bool {
	for k, v := range desired {
		if existing[k] != v {
			return false
		}
	}
	return true
}

var ciliumNodeGVR = schema.GroupVersionResource{
	Group:    "cilium.io",
	Version:  "v2",
	Resource: "ciliumnodes",
}

// EnsureCiliumNode creates or updates a CiliumNode resource for the given pawn.
// The constellation operator fills in the IPAM section (CIDR allocation);
// perigeos only creates the skeleton with InternalIP so the operator can find it.
func EnsureCiliumNode(ctx context.Context, client dynamic.Interface, logger *slog.Logger, pawnName, nodeIP string, labels map[string]string) error {
	ciliumNodes := client.Resource(ciliumNodeGVR)

	meta := map[string]any{
		"name": pawnName,
	}
	if len(labels) > 0 {
		lbls := make(map[string]any, len(labels))
		for k, v := range labels {
			lbls[k] = v
		}
		meta["labels"] = lbls
	}

	cn := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "cilium.io/v2",
			"kind":       "CiliumNode",
			"metadata":   meta,
			"spec": map[string]any{
				"addresses": []any{
					map[string]any{
						"type": "InternalIP",
						"ip":   nodeIP,
					},
				},
				"healthAddressing": map[string]any{
					"ipv4": nodeIP,
				},
				"encryption": map[string]any{
					"key": int64(0),
				},
			},
		},
	}

	existing, err := ciliumNodes.Get(ctx, pawnName, metav1.GetOptions{})
	if err == nil {
		ipChanged := internalIPFromCiliumNode(existing) != nodeIP
		labelsChanged := !labelsMatch(existing.GetLabels(), labels)

		if ipChanged || labelsChanged {
			addrs := updatedAddresses(existing, nodeIP)
			patch := map[string]any{
				"spec": map[string]any{
					"addresses":        addrs,
					"healthAddressing": map[string]any{"ipv4": nodeIP},
				},
			}
			if labelsChanged && len(labels) > 0 {
				lbls := make(map[string]any, len(labels))
				for k, v := range labels {
					lbls[k] = v
				}
				patch["metadata"] = map[string]any{"labels": lbls}
			}
			patchBytes, _ := json.Marshal(patch)
			if _, perr := ciliumNodes.Patch(ctx, pawnName, types.MergePatchType, patchBytes, metav1.PatchOptions{}); perr != nil {
				return fmt.Errorf("patching CiliumNode %s: %w", pawnName, perr)
			}
			logger.Info("Updated CiliumNode", "node", pawnName, "ipChanged", ipChanged, "labelsChanged", labelsChanged)
			return nil
		}
		logger.Info("CiliumNode already current", "node", pawnName, "resourceVersion", existing.GetResourceVersion())
		return nil
	}

	_, err = ciliumNodes.Create(ctx, cn, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating CiliumNode %s: %w", pawnName, err)
	}
	logger.Info("Created CiliumNode for pawn", "node", pawnName, "ip", nodeIP)
	return nil
}

// DeleteCiliumNode removes the CiliumNode resource for a pawn.
// Called during graceful shutdown to clean up IPAM state.
func DeleteCiliumNode(ctx context.Context, client dynamic.Interface, logger *slog.Logger, pawnName string) {
	err := client.Resource(ciliumNodeGVR).Delete(ctx, pawnName, metav1.DeleteOptions{})
	if err != nil {
		logger.Warn("Failed to delete CiliumNode", "node", pawnName, "err", err)
		return
	}
	logger.Info("Deleted CiliumNode", "node", pawnName)
}

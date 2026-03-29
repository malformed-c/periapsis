package image

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// PeerConfig configures peer blob fetching.
type PeerConfig struct {
	Client kubernetes.Interface
	SelfIP string // skip ourself during peer lookup
}

// SetPeers enables peer-to-peer blob fetching. Call once from main after the
// kube client and pawn config are ready.
func (im *ImageManager) SetPeers(cfg PeerConfig) {
	im.peers = &cfg
}

// peerEndpoints returns "ip:port" for every perigeos pawn on other hosts.
// The port is read from node.Status.DaemonEndpoints.KubeletEndpoint.Port,
// which perigeos sets to each pawn's serving port at registration time.
// Multiple pawns on the same host are deduplicated — one endpoint per host IP.
func (im *ImageManager) peerEndpoints(ctx context.Context) []string {
	if im.peers == nil || im.peers.Client == nil {
		return nil
	}
	nodes, err := im.peers.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "periapsis.io/host",
	})
	if err != nil {
		return nil
	}

	seen := make(map[string]bool)
	var endpoints []string
	for _, n := range nodes.Items {
		port := n.Status.DaemonEndpoints.KubeletEndpoint.Port
		if port == 0 {
			continue
		}
		for _, addr := range n.Status.Addresses {
			if addr.Type == "InternalIP" && addr.Address != im.peers.SelfIP {
				ep := fmt.Sprintf("%s:%d", addr.Address, port)
				if !seen[addr.Address] {
					seen[addr.Address] = true
					endpoints = append(endpoints, ep)
				}
				break
			}
		}
	}
	return endpoints
}

// fetchFromPeers fans out to all known peer endpoints and returns the first
// successful blob response as a raw compressed stream. Caller must close it.
func (im *ImageManager) fetchFromPeers(ctx context.Context, hash string) (io.ReadCloser, bool) {
	endpoints := im.peerEndpoints(ctx)
	if len(endpoints) == 0 {
		return nil, false
	}

	results := make(chan io.ReadCloser, len(endpoints))
	peerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // content-addressed — digest is the integrity check
		DisableKeepAlives:   true,
		MaxIdleConnsPerHost: 1,
	}
	hc := &http.Client{Transport: tr}

	for _, ep := range endpoints {
		go func(ep string) {
			url := fmt.Sprintf("https://%s/blobs/%s", ep, hash)
			req, err := http.NewRequestWithContext(peerCtx, http.MethodGet, url, nil)
			if err != nil {
				results <- nil
				return
			}
			resp, err := hc.Do(req)
			if err != nil || resp.StatusCode != http.StatusOK {
				if resp != nil {
					resp.Body.Close()
				}
				results <- nil
				return
			}
			results <- resp.Body
		}(ep)
	}

	for range endpoints {
		if body := <-results; body != nil {
			cancel()
			return body, true
		}
	}
	return nil, false
}

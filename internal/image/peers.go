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
	Client   kubernetes.Interface
	SelfIP   string // skip ourself during peer lookup
	BlobPort int    // port where peers serve /blobs/{hash}
}

// SetPeers enables peer-to-peer blob fetching. Call once from main after the
// kube client and pawn config are ready.
func (im *ImageManager) SetPeers(cfg PeerConfig) {
	im.peers = &cfg
}

// peerIPs returns the IPs of all other perigeos nodes.
func (im *ImageManager) peerIPs(ctx context.Context) []string {
	if im.peers == nil || im.peers.Client == nil {
		return nil
	}
	nodes, err := im.peers.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "periapsis.io/host",
	})
	if err != nil {
		return nil
	}

	var ips []string
	for _, n := range nodes.Items {
		for _, addr := range n.Status.Addresses {
			if addr.Type == "InternalIP" && addr.Address != im.peers.SelfIP {
				ips = append(ips, addr.Address)
				break
			}
		}
	}
	return ips
}

// fetchFromPeers tries to fetch a compressed blob from any peer node.
// Returns the raw compressed (gzip) stream — caller must close it.
// Returns nil, false if no peer has the blob.
func (im *ImageManager) fetchFromPeers(ctx context.Context, hash string) (io.ReadCloser, bool) {
	ips := im.peerIPs(ctx)
	if len(ips) == 0 {
		return nil, false
	}

	results := make(chan io.ReadCloser, len(ips))
	peerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // content-addressed — digest is the verification
		DisableKeepAlives:   true,
		MaxIdleConnsPerHost: 1,
	}
	hc := &http.Client{Transport: tr}

	for _, ip := range ips {
		go func(ip string) {
			url := fmt.Sprintf("https://%s:%d/blobs/%s", ip, im.peers.BlobPort, hash)
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
		}(ip)
	}

	for range ips {
		if body := <-results; body != nil {
			cancel()
			return body, true
		}
	}
	return nil, false
}

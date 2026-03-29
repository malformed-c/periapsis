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

// peerConnsPerHost is the maximum number of simultaneous connections to a
// single peer host — one per in-flight layer download.
const peerConnsPerHost = layerConcurrency

// peerStallTimeout is how long a peer body read may make no progress before
// the download is considered stalled and the transfer is abandoned.
const peerStallTimeout = 30 * time.Second

// PeerConfig configures peer blob fetching.
type PeerConfig struct {
	Client kubernetes.Interface
	SelfIP string // skip ourself during peer lookup
}

// SetPeers enables peer-to-peer blob fetching. Call once from main after the
// kube client and pawn config are ready.
func (im *ImageManager) SetPeers(cfg PeerConfig) {
	im.peers = &cfg
	im.peerClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // content-addressed — digest is the integrity check
			ResponseHeaderTimeout: 10 * time.Second,                      // fast-fail peers that don't respond
			MaxIdleConnsPerHost:   peerConnsPerHost,
			MaxConnsPerHost:       peerConnsPerHost,
		},
	}
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

// peerResult carries one goroutine's outcome. cancel must be called by whoever
// consumes the result (either to abort a failed request or to clean up a winner
// once its body is fully consumed).
type peerResult struct {
	body   io.ReadCloser
	cancel context.CancelFunc
}

// fetchFromPeers fans out to all known peer endpoints and returns the first
// successful blob response as a raw compressed stream. Caller must close it.
//
// Each peer request runs with its own child context so only losers are
// cancelled — the winner's body read continues with the parent ctx deadline.
// ResponseHeaderTimeout on the shared transport kills slow-to-respond peers
// without touching the winner's ongoing download.
func (im *ImageManager) fetchFromPeers(ctx context.Context, hash string) (io.ReadCloser, bool) {
	if im.peerClient == nil {
		return nil, false
	}
	endpoints := im.peerEndpoints(ctx)
	if len(endpoints) == 0 {
		return nil, false
	}

	n := len(endpoints)
	results := make(chan peerResult, n)

	for _, ep := range endpoints {
		reqCtx, reqCancel := context.WithCancel(ctx)
		ep := ep
		go func() {
			url := fmt.Sprintf("https://%s/blobs/%s", ep, hash)
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
			if err != nil {
				reqCancel()
				results <- peerResult{}
				return
			}
			resp, err := im.peerClient.Do(req)
			if err != nil || resp.StatusCode != http.StatusOK {
				if resp != nil {
					resp.Body.Close()
				}
				reqCancel()
				results <- peerResult{}
				return
			}
			// Don't cancel here — caller owns reqCancel for the winner.
			results <- peerResult{body: stallReader(resp.Body, peerStallTimeout), cancel: reqCancel}
		}()
	}

	for i := range n {
		r := <-results
		if r.body != nil {
			// Drain and cancel the remaining in-flight requests in the background.
			go drainPeerResults(results, n-i-1)
			return r.body, true
		}
	}
	return nil, false
}

// drainPeerResults consumes exactly count results from ch, closing any bodies
// and cancelling any contexts for requests that were still in flight.
func drainPeerResults(ch <-chan peerResult, count int) {
	for range count {
		if r := <-ch; r.cancel != nil {
			if r.body != nil {
				r.body.Close()
			}
			r.cancel()
		}
	}
}

// stallReader wraps r and returns an error if no bytes are read within timeout.
// This lets fetchFromPeers detect a stalled peer mid-download so ensureLayer
// can fall back to the upstream registry.
func stallReader(r io.ReadCloser, timeout time.Duration) io.ReadCloser {
	return &stallDetector{ReadCloser: r, timeout: timeout}
}

type stallDetector struct {
	io.ReadCloser
	timeout time.Duration
}

func (s *stallDetector) Read(p []byte) (int, error) {
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := s.ReadCloser.Read(p)
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		return r.n, r.err
	case <-time.After(s.timeout):
		return 0, fmt.Errorf("peer stalled: no data for %s", s.timeout)
	}
}

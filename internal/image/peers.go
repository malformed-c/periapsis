package image

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// peerConnsPerHost is the maximum number of simultaneous connections to a
// single peer host - one per in-flight layer download.
const peerConnsPerHost = layerConcurrency

// peerStallTimeout is how long a peer body read may make no progress before
// the download is considered stalled and the peer is marked bad.
const peerStallTimeout = 30 * time.Second

// inflightPollInterval is how often we re-check a peer's /blobs/{hash}
// while waiting for it to finish pulling a layer we want.
const inflightPollInterval = 2 * time.Second

// inflightWaitTimeout caps how long we wait for a peer to finish a layer
// before giving up and pulling from upstream ourselves. Kept short so a
// single slow (or phantom-self) peer can't eat most of the outer pull
// timeout before the upstream fallback gets a chance to run.
const inflightWaitTimeout = 60 * time.Second

// PeerConfig configures peer blob fetching.
type PeerConfig struct {
	Client   kubernetes.Interface
	SelfHost string // host label value for this perigeos instance - skipped during peer lookup
}

// SetPeers enables peer-to-peer blob fetching. Call once from main after the
// kube client and pawn config are ready.
func (im *ImageManager) SetPeers(cfg PeerConfig) {
	im.peers = &cfg
	// Register a random marker as a permanent entry in inflightLayers so that
	// /blobs/inflight always surfaces it. peersWithInflight uses the marker to
	// detect that a "peer" is actually this same perigeos process and skip it.
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		im.selfMarker = "self-" + hex.EncodeToString(buf[:])
		done := make(chan struct{})
		close(done)
		im.inflightLayers.Store(im.selfMarker, done)
	}
	im.peerClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // content-addressed - digest is the integrity check
			ResponseHeaderTimeout: 10 * time.Second,                      // fast-fail peers that don't respond
			MaxIdleConnsPerHost:   peerConnsPerHost,
			MaxConnsPerHost:       peerConnsPerHost,
		},
	}
}

// peerEndpoints returns "ip:port" for every perigeos pawn on other hosts.
// The port is read from node.Status.DaemonEndpoints.KubeletEndpoint.Port,
// which perigeos sets to each pawn's serving port at registration time.
// Multiple pawns on the same host are deduplicated - one endpoint per host IP.
//
// Self is filtered by the periapsis.io/host label (the perigeos host name),
// not by IP. An IP-based filter is fragile because a host can have several
// interfaces and the outbound-IP probe (dial 8.8.8.8) may pick one that
// differs from the pawn's configured NodeIP - perigeos would then treat
// itself as a peer and wait-deadlock on inflight layers.
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
		if im.peers.SelfHost != "" && n.Labels["periapsis.io/host"] == im.peers.SelfHost {
			continue
		}
		port := n.Status.DaemonEndpoints.KubeletEndpoint.Port
		if port == 0 {
			continue
		}
		for _, addr := range n.Status.Addresses {
			if addr.Type == "InternalIP" {
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

// newPeerSelector snapshots the current healthy peer list for one pull.
// Returns nil if no peers are available.
func (im *ImageManager) newPeerSelector(ctx context.Context) *peerSelector {
	if im.peerClient == nil {
		return nil
	}
	eps := im.peerEndpoints(ctx)
	if len(eps) == 0 {
		return nil
	}
	return &peerSelector{healthy: eps, client: im.peerClient}
}

// peerSelector distributes layer downloads across a snapshot of healthy peers.
// Layers are assigned round-robin; a peer that stalls is evicted so subsequent
// layers avoid it. All methods are safe for concurrent use.
type peerSelector struct {
	mu      sync.Mutex
	healthy []string
	next    int // round-robin cursor
	client  *http.Client
}

// pick returns the next healthy endpoint (round-robin) and true, or ("", false)
// if no healthy peers remain.
func (ps *peerSelector) pick() (string, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if len(ps.healthy) == 0 {
		return "", false
	}
	ep := ps.healthy[ps.next%len(ps.healthy)]
	ps.next++
	return ep, true
}

// markBad removes ep from the healthy set.
func (ps *peerSelector) markBad(ep string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for i, e := range ps.healthy {
		if e == ep {
			ps.healthy = append(ps.healthy[:i], ps.healthy[i+1:]...)
			// Adjust cursor so we don't skip an entry after the removed one.
			if ps.next > i {
				ps.next--
			}
			if len(ps.healthy) > 0 {
				ps.next %= len(ps.healthy)
			} else {
				ps.next = 0
			}
			return
		}
	}
}

// peersWithInflight queries all peers for their in-flight layer hashes and
// returns a map of hash -> endpoint for hashes that match the given set.
// Used to discover which peer is pulling a layer we also want, so we can
// wait for it instead of pulling from upstream independently.
func (im *ImageManager) peersWithInflight(ctx context.Context, wantHashes map[string]bool) map[string]string {
	if im.peerClient == nil {
		return nil
	}
	eps := im.peerEndpoints(ctx)
	if len(eps) == 0 {
		return nil
	}

	type result struct {
		ep     string
		hashes []string
	}
	ch := make(chan result, len(eps))

	queried := 0
	for _, ep := range eps {
		if _, self := im.knownSelfEps.Load(ep); self {
			continue
		}
		queried++
		go func() {
			hashes, err := fetchInflightHashes(ctx, im.peerClient, ep)
			if err != nil {
				ch <- result{ep: ep}
				return
			}
			ch <- result{ep: ep, hashes: hashes}
		}()
	}

	// hash -> first peer that has it inflight
	found := make(map[string]string)
	for i := 0; i < queried; i++ {
		r := <-ch
		// Belt-and-braces self detection: if our self-marker appears in this
		// peer's inflight list, the "peer" is actually us. Cache and skip.
		if im.selfMarker != "" && slices.Contains(r.hashes, im.selfMarker) {
			im.knownSelfEps.Store(r.ep, true)
			continue
		}
		for _, h := range r.hashes {
			if wantHashes[h] {
				if _, exists := found[h]; !exists {
					found[h] = r.ep
				}
			}
		}
	}
	return found
}

// waitForPeerLayer waits up to inflightWaitTimeout for ep to finish pulling
// hash (i.e. for /blobs/{hash} to return 200). Returns the peer endpoint to
// fetch from, or "" if the wait timed out or the peer disappeared.
func waitForPeerLayer(ctx context.Context, client *http.Client, ep, hash string) bool {
	ctx, cancel := context.WithTimeout(ctx, inflightWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(inflightPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if peerHasLayer(ctx, client, ep, hash) {
				return true
			}
		}
	}
}

// peerHasLayer returns true if ep serves /blobs/{hash} with HTTP 200.
func peerHasLayer(ctx context.Context, client *http.Client, ep, hash string) bool {
	url := fmt.Sprintf("https://%s/blobs/%s", ep, hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// fetchInflightHashes retrieves the list of layer hashes currently being
// pulled by ep via GET /blobs/inflight.
func fetchInflightHashes(ctx context.Context, client *http.Client, ep string) ([]string, error) {
	url := fmt.Sprintf("https://%s/blobs/inflight", ep)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s: HTTP %d", ep, resp.StatusCode)
	}
	var hashes []string
	if err := json.NewDecoder(resp.Body).Decode(&hashes); err != nil {
		return nil, err
	}
	return hashes, nil
}

// fetch tries to fetch a blob from the next healthy peer. Returns the response
// body (wrapped with stall detection), the endpoint used, and true on success.
// On connection/header failure the peer is immediately marked bad and the next
// healthy peer is tried. Caller must close the returned body.
func (ps *peerSelector) fetch(ctx context.Context, hash string) (io.ReadCloser, string, bool) {
	for {
		ep, ok := ps.pick()
		if !ok {
			return nil, "", false
		}
		body, err := fetchOnePeer(ctx, ps.client, hash, ep)
		if err != nil {
			ps.markBad(ep)
			continue
		}
		return stallReader(body, peerStallTimeout), ep, true
	}
}

// fetchOnePeer sends a single GET /blobs/{hash} to ep and returns the response
// body on HTTP 200, or an error otherwise.
func fetchOnePeer(ctx context.Context, client *http.Client, hash, ep string) (io.ReadCloser, error) {
	url := fmt.Sprintf("https://%s/blobs/%s", ep, hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("peer %s: HTTP %d", ep, resp.StatusCode)
	}
	return resp.Body, nil
}

// stallReader wraps r and returns an error if no bytes are read within timeout.
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

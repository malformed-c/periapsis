package image

import (
	"testing"
)

func TestPeerSelector_Pick(t *testing.T) {
	ps := &peerSelector{
		healthy: []string{"peer1", "peer2", "peer3"},
	}

	// Test round-robin
	picks := []string{}
	for i := 0; i < 6; i++ {
		ep, ok := ps.pick()
		if !ok {
			t.Fatal("expected pick to succeed")
		}
		picks = append(picks, ep)
	}

	expected := []string{"peer1", "peer2", "peer3", "peer1", "peer2", "peer3"}
	for i, v := range expected {
		if picks[i] != v {
			t.Errorf("at index %d, expected %s, got %s", i, v, picks[i])
		}
	}
}

func TestPeerSelector_MarkBad(t *testing.T) {
	ps := &peerSelector{
		healthy: []string{"peer1", "peer2", "peer3"},
		next:    1, // points to peer2
	}

	// Mark peer2 bad
	ps.markBad("peer2")

	if len(ps.healthy) != 2 {
		t.Fatalf("expected 2 healthy peers, got %d", len(ps.healthy))
	}
	if ps.healthy[0] != "peer1" || ps.healthy[1] != "peer3" {
		t.Errorf("unexpected healthy set: %v", ps.healthy)
	}
	if ps.next != 1 {
		t.Errorf("expected next to be 1, got %d", ps.next)
	}

	ep, _ := ps.pick()
	if ep != "peer3" {
		t.Errorf("expected next pick to be peer3, got %s", ep)
	}

	// Mark peer1 bad
	ps.markBad("peer1")
	if len(ps.healthy) != 1 {
		t.Fatalf("expected 1 healthy peer, got %d", len(ps.healthy))
	}
	if ps.next != 0 {
		t.Errorf("expected next to be 0, got %d", ps.next)
	}

	ep, _ = ps.pick()
	if ep != "peer3" {
		t.Errorf("expected next pick to be peer3, got %s", ep)
	}

	// Mark last peer bad
	ps.markBad("peer3")
	if len(ps.healthy) != 0 {
		t.Fatalf("expected 0 healthy peers, got %d", len(ps.healthy))
	}

	_, ok := ps.pick()
	if ok {
		t.Error("expected pick to fail when no healthy peers remain")
	}
}

func TestPeerSelector_Empty(t *testing.T) {
	ps := &peerSelector{
		healthy: []string{},
	}
	_, ok := ps.pick()
	if ok {
		t.Error("expected pick to fail on empty selector")
	}
}

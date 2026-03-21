package network

import (
	"fmt"
	"sync"
	"testing"
)

func TestIPPool_AllocateSequential(t *testing.T) {
	p, err := newIPPool("10.88.0.0/16")
	if err != nil {
		t.Fatal(err)
	}

	ip1, err := p.Allocate("pod-1")
	if err != nil {
		t.Fatal(err)
	}
	if ip1 != "10.88.0.2" {
		t.Errorf("first allocation: got %s, want 10.88.0.2", ip1)
	}

	ip2, err := p.Allocate("pod-2")
	if err != nil {
		t.Fatal(err)
	}
	if ip2 != "10.88.0.3" {
		t.Errorf("second allocation: got %s, want 10.88.0.3", ip2)
	}
}

func TestIPPool_Gateway(t *testing.T) {
	p, err := newIPPool("10.88.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	if gw := p.Gateway(); gw != "10.88.0.1" {
		t.Errorf("gateway: got %s, want 10.88.0.1", gw)
	}
}

func TestIPPool_Release_AllowsReallocation(t *testing.T) {
	p, err := newIPPool("10.88.0.0/24")
	if err != nil {
		t.Fatal(err)
	}

	ip, err := p.Allocate("pod-1")
	if err != nil {
		t.Fatal(err)
	}

	p.Release("pod-1")

	// Allocate enough IPs to wrap around and reclaim the released one.
	found := false
	for i := 0; i < 253; i++ {
		candidate, err := p.Allocate(fmt.Sprintf("pod-%d", i+10))
		if err != nil {
			t.Fatalf("unexpected exhaustion at i=%d: %v", i, err)
		}
		if candidate == ip {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("released IP %s was never reallocated", ip)
	}
}

func TestIPPool_NoDuplicates(t *testing.T) {
	p, err := newIPPool("10.88.0.0/24")
	if err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]struct{})
	// /24 gives 253 usable IPs (.2 through .254)
	for i := 0; i < 253; i++ {
		ip, err := p.Allocate(fmt.Sprintf("pod-%d", i))
		if err != nil {
			t.Fatalf("allocation %d failed: %v", i, err)
		}
		if _, dup := seen[ip]; dup {
			t.Fatalf("duplicate IP allocated: %s", ip)
		}
		seen[ip] = struct{}{}
	}
}

func TestIPPool_Exhaustion(t *testing.T) {
	p, err := newIPPool("10.88.0.0/24")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 253; i++ {
		if _, err := p.Allocate(fmt.Sprintf("pod-%d", i)); err != nil {
			t.Fatalf("unexpected error at %d: %v", i, err)
		}
	}

	_, err = p.Allocate("overflow")
	if err == nil {
		t.Error("expected error on exhausted pool, got nil")
	}
}

func TestIPPool_ConcurrentAllocate(t *testing.T) {
	p, err := newIPPool("10.88.0.0/16")
	if err != nil {
		t.Fatal(err)
	}

	const n = 100
	ips := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			ip, err := p.Allocate(fmt.Sprintf("pod-%d", i))
			if err != nil {
				t.Errorf("goroutine %d: %v", i, err)
				return
			}
			ips[i] = ip
		}()
	}
	wg.Wait()

	seen := make(map[string]struct{})
	for _, ip := range ips {
		if ip == "" {
			continue
		}
		if _, dup := seen[ip]; dup {
			t.Errorf("concurrent duplicate IP: %s", ip)
		}
		seen[ip] = struct{}{}
	}
}

func TestVethName_MaxLength(t *testing.T) {
	uids := []string{
		"550e8400-e29b-41d4-a716-446655440000",
		"a",
		"12345678-1234-1234-1234-123456789abc",
		"short",
	}
	for _, uid := range uids {
		name := vethName(uid)
		if len(name) > 15 {
			t.Errorf("vethName(%q) = %q, length %d > 15", uid, name, len(name))
		}
		if len(name) == 0 {
			t.Errorf("vethName(%q) returned empty string", uid)
		}
	}
}

func TestIPPool_InvalidCIDR(t *testing.T) {
	_, err := newIPPool("not-a-cidr")
	if err == nil {
		t.Error("expected error for invalid CIDR, got nil")
	}
}

// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package network

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

// ipPool allocates IPs from a /16 subnet sequentially.
// Gateway (.1) is reserved; allocation starts at .2.
// The pool is in-process only - on restart, IPs are re-discovered from
// running netns interfaces rather than persisted state.
type ipPool struct {
	mu    sync.Mutex
	base  uint32            // network address as uint32
	next  uint32            // next host offset to try (starts at 2)
	max   uint32            // last valid host offset
	inUse map[uint32]string // offset -> podUID
}

func newIPPool(cidr string) (*ipPool, error) {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("invalid CIDR %s: %w", cidr, err)
	}

	base := binary.BigEndian.Uint32(network.IP.To4())
	ones, bits := network.Mask.Size()
	hostBits := bits - ones
	max := uint32(1<<hostBits) - 2 // exclude network and broadcast

	return &ipPool{
		base:  base,
		next:  2, // skip .0 (network) and .1 (gateway)
		max:   max,
		inUse: make(map[uint32]string),
	}, nil
}

// Allocate reserves the next free IP and returns it as a string (e.g. "10.88.0.2").
func (p *ipPool) Allocate(podUID string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	start := p.next
	for {
		if _, used := p.inUse[p.next]; !used {
			offset := p.next
			p.inUse[offset] = podUID
			p.next++
			if p.next > p.max {
				p.next = 2
			}
			ip := make(net.IP, 4)
			binary.BigEndian.PutUint32(ip, p.base+offset)
			return ip.String(), nil
		}
		p.next++
		if p.next > p.max {
			p.next = 2
		}
		if p.next == start {
			return "", fmt.Errorf("IP pool exhausted")
		}
	}
}

// Release frees the IP associated with podUID.
func (p *ipPool) Release(podUID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for offset, uid := range p.inUse {
		if uid == podUID {
			delete(p.inUse, offset)
			return
		}
	}
}

// Gateway returns the gateway IP (.1 in the subnet).
func (p *ipPool) Gateway() string {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, p.base+1)
	return ip.String()
}

// Subnet returns the prefix length (e.g. "/16").
func (p *ipPool) Subnet() string {
	bits := 32 - bitLen(p.max+1)
	return fmt.Sprintf("/%d", bits)
}

func bitLen(n uint32) int {
	l := 0
	for n > 0 {
		n >>= 1
		l++
	}
	return l
}

// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: BUSL-1.1

package pki

import (
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

// CertHolder manages a TLS certificate that can be renewed at runtime.
// It is designed to be used as the GetCertificate callback in tls.Config,
// allowing transparent cert renewal when the current cert doesn't cover
// a requested IP or hostname.
type CertHolder struct {
	mu   sync.RWMutex
	cert *tls.Certificate
	leaf *x509.Certificate

	// renewFn is called to obtain a new certificate. It should return a
	// freshly issued cert (e.g. via CSR flow or self-signed generation).
	renewFn func() (tls.Certificate, error)

	// Rate limit: at most one renewal attempt per cooldown period.
	lastRenewAttempt time.Time
	cooldown         time.Duration

	logger *slog.Logger
}

// NewCertHolder creates a CertHolder with an initial certificate and a
// renewal function that will be called when the cert doesn't match.
func NewCertHolder(initial tls.Certificate, renewFn func() (tls.Certificate, error), logger *slog.Logger) (*CertHolder, error) {
	leaf, err := x509.ParseCertificate(initial.Certificate[0])
	if err != nil {
		return nil, err
	}
	return &CertHolder{
		cert:     &initial,
		leaf:     leaf,
		renewFn:  renewFn,
		cooldown: 5 * time.Minute,
		logger:   logger,
	}, nil
}

// GetCertificate is intended for use as tls.Config.GetCertificate.
// If the current cert covers the requested name/IP, it is returned immediately.
// Otherwise, a renewal is attempted (rate-limited) and the result is returned.
func (ch *CertHolder) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	ch.mu.RLock()
	cert := ch.cert
	leaf := ch.leaf
	ch.mu.RUnlock()

	// Check if current cert covers the requested server name.
	if ch.covers(leaf, hello) {
		return cert, nil
	}

	ch.logger.Info("TLS handshake for uncovered name/IP, attempting renewal",
		"serverName", hello.ServerName,
		"certSANs", sanSummary(leaf))

	renewed, err := ch.tryRenew()
	if err != nil {
		ch.logger.Warn("Cert renewal failed, serving existing cert", "err", err)
		return cert, nil
	}
	if renewed != nil {
		return renewed, nil
	}
	// Renewal was rate-limited, serve existing cert.
	return cert, nil
}

// covers returns true if the leaf certificate is valid for the name/IP
// in the TLS ClientHello.
func (ch *CertHolder) covers(leaf *x509.Certificate, hello *tls.ClientHelloInfo) bool {
	name := hello.ServerName
	if name == "" {
		// No SNI - client connected by IP. Try to extract from conn.
		if addr, ok := hello.Conn.LocalAddr().(*net.TCPAddr); ok {
			ip := addr.IP
			for _, san := range leaf.IPAddresses {
				if san.Equal(ip) {
					return true
				}
			}
			return false
		}
		// Can't determine what to check; assume covered to avoid spurious renewals.
		return true
	}

	// Check DNS SANs.
	if err := leaf.VerifyHostname(name); err == nil {
		return true
	}

	// ServerName might be an IP literal.
	if ip := net.ParseIP(name); ip != nil {
		for _, san := range leaf.IPAddresses {
			if san.Equal(ip) {
				return true
			}
		}
	}

	return false
}

// tryRenew attempts to renew the cert, respecting the cooldown period.
// Returns (newCert, nil) on success, (nil, nil) if rate-limited, or (nil, err) on failure.
func (ch *CertHolder) tryRenew() (*tls.Certificate, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if time.Since(ch.lastRenewAttempt) < ch.cooldown {
		return nil, nil
	}
	ch.lastRenewAttempt = time.Now()

	newCert, err := ch.renewFn()
	if err != nil {
		return nil, err
	}

	leaf, err := x509.ParseCertificate(newCert.Certificate[0])
	if err != nil {
		return nil, err
	}

	ch.cert = &newCert
	ch.leaf = leaf
	ch.logger.Info("Certificate renewed", "SANs", sanSummary(leaf))
	return &newCert, nil
}

// sanSummary returns a short string listing the cert's DNS names and IPs.
func sanSummary(leaf *x509.Certificate) string {
	var parts []string
	parts = append(parts, leaf.DNSNames...)

	for _, ip := range leaf.IPAddresses {
		parts = append(parts, ip.String())
	}
	if len(parts) > 6 {
		parts = append(parts[:6], "...")
	}
	var result strings.Builder
	for i, p := range parts {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(p)
	}
	return result.String()
}

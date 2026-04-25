// Copyright (C) 2025-2026 Malformed C. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-only

package pki

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"time"
)

// GenerateCert creates a cert that covers the Node Name,
// If caCert/caKey are provided, it signs the new cert with them
// If they are nil, it creates a self-signed certificate
// but adds the physical IP and Hostname to SANs
// Hostname, Localhost, and EVERY IP address on the machine
func GenerateCert(nodeName string, caCert *x509.Certificate, caKey any) (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Perigeos"},
			CommonName:   nodeName, // Helpful for debugging, but K8s checks SANs

		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(365 * 24 * time.Hour), // A year

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// --- SANs (Subject Alternative Names) ---
	// The API Server verifies the address it DIALED against this list

	// DNS
	// The Virtual Node Name
	// If K8s connects via Hostname, this matches Hostname
	// We add this just in case your network uses internal DNS matching the node name
	template.DNSNames = append(template.DNSNames, nodeName, "localhost")
	if h, _ := os.Hostname(); h != "" && h != nodeName {
		template.DNSNames = append(template.DNSNames, h)
	}

	slog.Default().Info("Adding DNS names to cert", "names", template.DNSNames)

	// IP Addresses

	// Localhost (Always good for local testing/curl)
	template.IPAddresses = append(template.IPAddresses, net.ParseIP("127.0.0.1"))

	// The Physical Outbound IP
	// K8s usually connects via NodeInternalIP
	if outBoundIP := GetOutboundIP(); outBoundIP != nil {
		slog.Default().Info("Adding outbound IP to cert", "ip", outBoundIP)
		template.IPAddresses = append(template.IPAddresses, outBoundIP)
	}

	// IP Addresses (The "Shotgun" Approach)
	// Iterate over ALL network interfaces and add ALL IPs found.
	// This ensures that whether K3s connects via 192.168.x.x, 10.88.x.x, or VPN, it matches.
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP

		case *net.IPAddr:
			ip = v.IP
		}

		if ip == nil {
			continue
		}

		// Avoid duplicates (simplified check)
		isDuplicate := false
		for _, existing := range template.IPAddresses {
			if existing.Equal(ip) {
				isDuplicate = true

				break
			}
		}

		// Add it to the cert
		if !isDuplicate {
			slog.Default().Info("Adding interface IP to cert", "ip", ip)
			template.IPAddresses = append(template.IPAddresses, ip)
		}
	}

	// ---

	// --- Signing Logic ---
	var derBytes []byte
	if caCert != nil && caKey != nil {
		// Sign with the provided K3s CA
		derBytes, err = x509.CreateCertificate(rand.Reader, &template, caCert, &priv.PublicKey, caKey)
	} else {
		// Self-sign
		derBytes, err = x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	}
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return tls.X509KeyPair(certPEM, keyPEM)
}

// hostName returns os.Hostname, swallowing errors.
func hostName() (string, error) {
	return os.Hostname()
}

// collectIPs gathers all IP addresses that should appear in cert SANs:
// 127.0.0.1, the outbound IP, and every interface IP.
func collectIPs() []net.IP {
	var ips []net.IP
	ips = append(ips, net.ParseIP("127.0.0.1"))

	if outbound := GetOutboundIP(); outbound != nil {
		ips = append(ips, outbound)
	}

	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil {
			continue
		}
		dup := false
		for _, existing := range ips {
			if existing.Equal(ip) {
				dup = true
				break
			}
		}
		if !dup {
			ips = append(ips, ip)
		}
	}
	return ips
}

// GetOutboundIP finds the local IP address that is used to talk to the internet.
// We prefer this over iterating interfaces because it gives us the "routable" IP.
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		// Fallback: If no internet, try to resolve hostname
		name, _ := os.Hostname()
		addrs, _ := net.LookupIP(name)
		if len(addrs) > 0 {
			return addrs[0]
		}

		return nil
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}

// LoadCA attempts to load the CA from disk, supporting RSA and ECDSA keys.
func LoadCA(certPath, keyPath string) (*x509.Certificate, any, error) {
	// 1. Read and Parse Certificate
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read cert: %w", err)
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("failed to decode cert PEM")
	}

	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse cert: %w", err)
	}

	// 2. Read and Parse Private Key
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read key: %w", err)
	}

	blockKey, _ := pem.Decode(keyPEM)
	if blockKey == nil {
		return nil, nil, fmt.Errorf("failed to decode key PEM")
	}

	// 3. Try to detect Key Type
	// K3s usually uses EC (Elliptic Curve), so we must check for that.

	// Try EC first (common for K3s)
	if key, err := x509.ParseECPrivateKey(blockKey.Bytes); err == nil {
		return caCert, key, nil
	}

	// Try PKCS1 (Standard RSA)
	if key, err := x509.ParsePKCS1PrivateKey(blockKey.Bytes); err == nil {
		return caCert, key, nil
	}

	// Try PKCS8 (Generic)
	if key, err := x509.ParsePKCS8PrivateKey(blockKey.Bytes); err == nil {
		return caCert, key, nil
	}

	return nil, nil, fmt.Errorf("failed to parse private key (unsupported type)")
}

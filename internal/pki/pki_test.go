package pki_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/malformed-c/periapsis/internal/pki"
)

// ─── GenerateCert — self-signed ───────────────────────────────────────────────

func TestGenerateCert_SelfSigned_IsUsable(t *testing.T) {
	cert, err := pki.GenerateCert("pawn-01", nil, nil)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("no certificate bytes returned")
	}
}

func TestGenerateCert_SelfSigned_ContainsNodeNameDNSSAN(t *testing.T) {
	cert, err := pki.GenerateCert("my-pawn", nil, nil)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	x509Cert := parseCert(t, cert)

	if !slices.Contains(x509Cert.DNSNames, "my-pawn") {
		t.Errorf("DNSNames %v does not contain node name 'my-pawn'", x509Cert.DNSNames)
	}
}

func TestGenerateCert_SelfSigned_ContainsLocalhostSAN(t *testing.T) {
	cert, err := pki.GenerateCert("pawn-01", nil, nil)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	x509Cert := parseCert(t, cert)

	hasDNSLocalhost := slices.Contains(x509Cert.DNSNames, "localhost")
	hasIPLocalhost := slices.ContainsFunc(x509Cert.IPAddresses, func(ip net.IP) bool {
		return ip.Equal(net.ParseIP("127.0.0.1"))
	})

	if !hasDNSLocalhost {
		t.Errorf("DNSNames %v missing 'localhost'", x509Cert.DNSNames)
	}
	if !hasIPLocalhost {
		t.Errorf("IPAddresses %v missing 127.0.0.1", x509Cert.IPAddresses)
	}
}

func TestGenerateCert_SelfSigned_ServerAuthExtKeyUsage(t *testing.T) {
	cert, err := pki.GenerateCert("pawn-01", nil, nil)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	x509Cert := parseCert(t, cert)

	if !slices.Contains(x509Cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		t.Errorf("ExtKeyUsage %v missing ServerAuth", x509Cert.ExtKeyUsage)
	}
}

func TestGenerateCert_SelfSigned_NotExpired(t *testing.T) {
	cert, err := pki.GenerateCert("pawn-01", nil, nil)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	x509Cert := parseCert(t, cert)
	now := time.Now()

	if now.Before(x509Cert.NotBefore) {
		t.Errorf("cert NotBefore %v is in the future", x509Cert.NotBefore)
	}
	if now.After(x509Cert.NotAfter) {
		t.Errorf("cert expired: NotAfter %v", x509Cert.NotAfter)
	}
}

func TestGenerateCert_SelfSigned_VerifiesAgainstItself(t *testing.T) {
	cert, err := pki.GenerateCert("pawn-01", nil, nil)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	x509Cert := parseCert(t, cert)

	pool := x509.NewCertPool()
	pool.AddCert(x509Cert)

	_, err = x509Cert.Verify(x509.VerifyOptions{
		DNSName: "pawn-01",
		Roots:   pool,
	})
	if err != nil {
		t.Errorf("self-signed cert does not verify against itself: %v", err)
	}
}

// ─── GenerateCert — CA-signed ─────────────────────────────────────────────────

func TestGenerateCert_CASigned_VerifiesAgainstCA(t *testing.T) {
	caCert, caKey := makeTestCA(t)

	cert, err := pki.GenerateCert("pawn-02", caCert, caKey)
	if err != nil {
		t.Fatalf("GenerateCert (CA-signed): %v", err)
	}

	x509Cert := parseCert(t, cert)

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	_, err = x509Cert.Verify(x509.VerifyOptions{
		DNSName: "pawn-02",
		Roots:   pool,
	})
	if err != nil {
		t.Errorf("CA-signed cert failed verification: %v", err)
	}
}

func TestGenerateCert_CASigned_RejectsWithoutCA(t *testing.T) {
	caCert, caKey := makeTestCA(t)

	cert, err := pki.GenerateCert("pawn-02", caCert, caKey)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	x509Cert := parseCert(t, cert)

	// Verify against an empty root pool — must fail.
	_, err = x509Cert.Verify(x509.VerifyOptions{
		DNSName: "pawn-02",
		Roots:   x509.NewCertPool(),
	})
	if err == nil {
		t.Error("expected verification to fail without the CA in roots")
	}
}

func TestGenerateCert_CASigned_ContainsNodeNameDNSSAN(t *testing.T) {
	caCert, caKey := makeTestCA(t)

	cert, err := pki.GenerateCert("edge-pawn", caCert, caKey)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	x509Cert := parseCert(t, cert)

	if !slices.Contains(x509Cert.DNSNames, "edge-pawn") {
		t.Errorf("DNSNames %v does not contain 'edge-pawn'", x509Cert.DNSNames)
	}
}

// ─── LoadCA ───────────────────────────────────────────────────────────────────

func TestLoadCA_RSA_RoundTrip(t *testing.T) {
	caCert, caKey := makeTestCA(t)

	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")

	writeTestCA(t, certPath, keyPath, caCert, caKey)

	loadedCert, loadedKey, err := pki.LoadCA(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	if loadedCert == nil {
		t.Fatal("LoadCA returned nil cert")
	}
	if loadedKey == nil {
		t.Fatal("LoadCA returned nil key")
	}
	if loadedCert.Subject.CommonName != caCert.Subject.CommonName {
		t.Errorf("CommonName: got %q, want %q", loadedCert.Subject.CommonName, caCert.Subject.CommonName)
	}
}

func TestLoadCA_MissingCertFile_Errors(t *testing.T) {
	dir := t.TempDir()
	_, _, err := pki.LoadCA(filepath.Join(dir, "missing.crt"), filepath.Join(dir, "missing.key"))
	if err == nil {
		t.Error("expected error for missing cert file")
	}
}

func TestLoadCA_MissingKeyFile_Errors(t *testing.T) {
	caCert, caKey := makeTestCA(t)

	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")
	writeTestCA(t, certPath, keyPath, caCert, caKey)

	os.Remove(keyPath)

	_, _, err := pki.LoadCA(certPath, keyPath)
	if err == nil {
		t.Error("expected error for missing key file")
	}
}

func TestLoadCA_InvalidPEM_Errors(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")

	os.WriteFile(certPath, []byte("not valid pem"), 0600)
	os.WriteFile(keyPath, []byte("not valid pem"), 0600)

	_, _, err := pki.LoadCA(certPath, keyPath)
	if err == nil {
		t.Error("expected error for invalid PEM cert")
	}
}

func TestLoadCA_ThenSignCert_Verifies(t *testing.T) {
	caCert, caKey := makeTestCA(t)

	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")
	writeTestCA(t, certPath, keyPath, caCert, caKey)

	loadedCert, loadedKey, err := pki.LoadCA(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}

	signed, err := pki.GenerateCert("pi-pawn", loadedCert, loadedKey)
	if err != nil {
		t.Fatalf("GenerateCert with loaded CA: %v", err)
	}

	x509Signed := parseCert(t, signed)

	pool := x509.NewCertPool()
	pool.AddCert(loadedCert)

	_, err = x509Signed.Verify(x509.VerifyOptions{DNSName: "pi-pawn", Roots: pool})
	if err != nil {
		t.Errorf("cert signed by loaded CA failed verification: %v", err)
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func parseCert(t *testing.T, tlsCert tls.Certificate) *x509.Certificate {
	t.Helper()
	x509Cert, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		t.Fatalf("x509.ParseCertificate: %v", err)
	}
	return x509Cert
}

// makeTestCA generates an in-memory ECDSA CA cert+key for test signing.
func makeTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	return cert, key
}

// writeTestCA writes an ECDSA CA cert and key to disk in PEM format.
func writeTestCA(t *testing.T, certPath, keyPath string, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if err := os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatalf("write cert: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal EC key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatalf("write key: %v", err)
	}
}

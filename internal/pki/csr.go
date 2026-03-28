package pki

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// RequestServingCert obtains a TLS certificate for a pawn's API server.
//
// It first checks for an existing cert+key on disk under configDir/pki/.
// If the cert is valid and not expiring within 30 days, it is reused.
// Otherwise, a new CSR is submitted to the Kubernetes certificates API,
// self-approved, and the signed cert is persisted to disk.
//
// configDir is typically /etc/apsis/perigeos.
func RequestServingCert(ctx context.Context, client kubernetes.Interface, nodeName, configDir string, logger *slog.Logger) (tls.Certificate, error) {
	pkiDir := filepath.Join(configDir, "pki")
	certPath := filepath.Join(pkiDir, nodeName+".crt")
	keyPath := filepath.Join(pkiDir, nodeName+".key")

	// Try loading existing cert from disk.
	if tlsCert, err := loadCertIfValid(certPath, keyPath); err == nil {
		logger.Info("Using cached certificate", "node", nodeName, "cert", certPath)
		return tlsCert, nil
	}

	// Generate a fresh RSA key.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	// Build the CSR with SANs matching what GenerateCert uses.
	dnsNames := []string{nodeName, "localhost"}
	if h, _ := hostName(); h != "" && h != nodeName {
		dnsNames = append(dnsNames, h)
	}
	ipAddresses := collectIPs()

	csrTemplate := x509.CertificateRequest{
		Subject: pkix.Name{
			Organization: []string{"system:nodes"},
			CommonName:   "system:node:" + nodeName,
		},
		DNSNames:    dnsNames,
		IPAddresses: ipAddresses,
	}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &csrTemplate, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create CSR: %w", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	// Submit the CSR to the Kubernetes API.
	csrName := fmt.Sprintf("perigeos-%s-%d", nodeName, time.Now().Unix())
	csrObj := &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: csrName,
		},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request:    csrPEM,
			SignerName: certificatesv1.KubeletServingSignerName,
			Usages: []certificatesv1.KeyUsage{
				certificatesv1.UsageDigitalSignature,
				certificatesv1.UsageKeyEncipherment,
				certificatesv1.UsageServerAuth,
			},
		},
	}

	logger.Info("Submitting CSR", "name", csrName, "node", nodeName)

	created, err := client.CertificatesV1().CertificateSigningRequests().Create(ctx, csrObj, metav1.CreateOptions{})
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("submit CSR: %w", err)
	}

	// Self-approve: perigeos approves its own CSR. The kube-controller-manager
	// csrsigning controller then sees the Approved condition and signs the cert.
	// This is safe because the security boundary is the kubeconfig, not CSR approval.
	if err := approveCSR(ctx, client, created.Name, logger); err != nil {
		return tls.Certificate{}, fmt.Errorf("self-approve CSR: %w", err)
	}

	// Wait for the certificate to be signed by kube-controller-manager.
	certPEM, err := waitForCert(ctx, client, created.Name, logger)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("wait for CSR approval: %w", err)
	}

	// Persist cert + key to disk for reuse across restarts.
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err := writeCert(pkiDir, certPath, keyPath, certPEM, keyPEM); err != nil {
		logger.Warn("Failed to persist certificate to disk", "err", err)
		// Non-fatal — cert still works in memory for this session.
	} else {
		logger.Info("Certificate persisted", "cert", certPath, "key", keyPath)
	}

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse issued cert: %w", err)
	}

	logger.Info("CSR approved, certificate issued", "name", csrName)
	return tlsCert, nil
}

// loadCertIfValid loads a cert+key from disk and checks that the cert
// is not expired and has at least 30 days of validity remaining.
func loadCertIfValid(certPath, keyPath string) (tls.Certificate, error) {
	tlsCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, err
	}

	leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		return tls.Certificate{}, err
	}

	remaining := time.Until(leaf.NotAfter)
	if remaining < 30*24*time.Hour {
		return tls.Certificate{}, fmt.Errorf("cert expires in %s, renewing", remaining.Round(time.Hour))
	}

	return tlsCert, nil
}

// writeCert persists a cert+key to disk with appropriate permissions.
func writeCert(pkiDir, certPath, keyPath string, certPEM, keyPEM []byte) error {
	if err := os.MkdirAll(pkiDir, 0700); err != nil {
		return fmt.Errorf("create pki dir: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}
	return nil
}

// waitForCert watches the CSR until it is approved and the certificate is
// populated, or the context is cancelled.
func waitForCert(ctx context.Context, client kubernetes.Interface, csrName string, logger *slog.Logger) ([]byte, error) {
	// Check if already issued (fast path).
	csr, err := client.CertificatesV1().CertificateSigningRequests().Get(ctx, csrName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if cert := extractCert(csr); cert != nil {
		return cert, nil
	}
	if denied := isDenied(csr); denied {
		return nil, fmt.Errorf("CSR %s was denied", csrName)
	}

	// Watch for updates.
	watcher, err := client.CertificatesV1().CertificateSigningRequests().Watch(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=" + csrName,
	})
	if err != nil {
		return nil, fmt.Errorf("watch CSR: %w", err)
	}
	defer watcher.Stop()

	timeout := time.After(5 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, fmt.Errorf("timed out waiting for CSR %s approval (5m)", csrName)
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return nil, fmt.Errorf("CSR watch closed unexpectedly")
			}
			if event.Type == watch.Deleted {
				return nil, fmt.Errorf("CSR %s was deleted", csrName)
			}
			csr, ok := event.Object.(*certificatesv1.CertificateSigningRequest)
			if !ok {
				continue
			}
			if denied := isDenied(csr); denied {
				return nil, fmt.Errorf("CSR %s was denied", csrName)
			}
			if cert := extractCert(csr); cert != nil {
				return cert, nil
			}
			logger.Debug("CSR pending", "name", csrName)
		}
	}
}

// approveCSR adds an Approved condition to the CSR so kube-controller-manager
// signs it. This is the self-approval path — perigeos approves its own CSRs.
func approveCSR(ctx context.Context, client kubernetes.Interface, csrName string, logger *slog.Logger) error {
	csr, err := client.CertificatesV1().CertificateSigningRequests().Get(ctx, csrName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	csr.Status.Conditions = append(csr.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
		Type:               certificatesv1.CertificateApproved,
		Status:             "True",
		Reason:             "PerigeosSelfApproval",
		Message:            "Approved by perigeos (self-approval)",
		LastUpdateTime:     metav1.Now(),
	})

	_, err = client.CertificatesV1().CertificateSigningRequests().UpdateApproval(ctx, csrName, csr, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	logger.Info("CSR self-approved", "name", csrName)
	return nil
}

func extractCert(csr *certificatesv1.CertificateSigningRequest) []byte {
	if len(csr.Status.Certificate) > 0 {
		return csr.Status.Certificate
	}
	return nil
}

func isDenied(csr *certificatesv1.CertificateSigningRequest) bool {
	for _, c := range csr.Status.Conditions {
		if c.Type == certificatesv1.CertificateDenied {
			return true
		}
	}
	return false
}

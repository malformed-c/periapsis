package join

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// discoverKubeconfig builds a kubeconfig for the given API server and token.
//
// It tries kubeadm-style discovery first: fetches the cluster-info ConfigMap
// from kube-public (which contains the full kubeconfig including the CA).
// If that fails, it falls back to k3s-style discovery: fetches /cacerts to
// get the CA certificate, then builds a minimal kubeconfig with token auth.
func discoverKubeconfig(ctx context.Context, apiServer, token string, logger *slog.Logger) (*clientcmdapi.Config, error) {
	cfg, err := kubeadmDiscover(ctx, apiServer, token, logger)
	if err == nil {
		return cfg, nil
	}
	logger.Debug("kubeadm discovery failed, trying k3s /cacerts", "err", err)

	cfg, err = k3sDiscover(ctx, apiServer, token, logger)
	if err != nil {
		return nil, fmt.Errorf("cluster discovery failed (also tried kubeadm cluster-info): %w", err)
	}
	return cfg, nil
}

// kubeadmDiscover fetches the cluster-info ConfigMap from kube-public.
// The ConfigMap is world-readable by design and contains the CA + API server URL.
func kubeadmDiscover(ctx context.Context, apiServer, token string, logger *slog.Logger) (*clientcmdapi.Config, error) {
	// Bootstrap with an insecure client to fetch cluster-info - the CA inside
	// it is what we use to verify future connections.
	restCfg := &rest.Config{
		Host:        apiServer,
		BearerToken: token,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}

	cm, err := client.CoreV1().ConfigMaps("kube-public").Get(ctx, "cluster-info", metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get cluster-info: %w", err)
	}

	raw, ok := cm.Data["kubeconfig"]
	if !ok {
		return nil, fmt.Errorf("cluster-info ConfigMap has no 'kubeconfig' key")
	}

	cfg, err := clientcmd.Load([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("parse cluster-info kubeconfig: %w", err)
	}

	injectToken(cfg, token)
	logger.Info("kubeadm discovery succeeded (cluster-info ConfigMap)")
	return cfg, nil
}

// k3sDiscover fetches the CA certificate from the k3s /cacerts endpoint
// and builds a minimal kubeconfig with token auth.
func k3sDiscover(ctx context.Context, apiServer, token string, logger *slog.Logger) (*clientcmdapi.Config, error) {
	caCert, err := fetchCACerts(ctx, apiServer)
	if err != nil {
		return nil, fmt.Errorf("fetch /cacerts: %w", err)
	}

	cfg := buildKubeconfig(apiServer, caCert, token)
	logger.Info("k3s discovery succeeded (/cacerts endpoint)")
	return cfg, nil
}

// fetchCACerts downloads the CA PEM from the k3s /cacerts endpoint,
// tolerating self-signed certificates on first contact (TOFU).
func fetchCACerts(ctx context.Context, apiServer string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // intentional for bootstrap CA fetch
	}
	hc := &http.Client{Transport: tr}

	url := strings.TrimRight(apiServer, "/") + "/cacerts"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from /cacerts", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// buildKubeconfig creates a minimal kubeconfig with CA data and token auth.
func buildKubeconfig(apiServer string, caData []byte, token string) *clientcmdapi.Config {
	return &clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"perigeos": {
				Server:                   apiServer,
				CertificateAuthorityData: caData,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"perigeos": {
				Token: token,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			"perigeos": {
				Cluster:  "perigeos",
				AuthInfo: "perigeos",
			},
		},
		CurrentContext: "perigeos",
	}
}

// injectToken replaces all auth infos in cfg with token-based auth.
func injectToken(cfg *clientcmdapi.Config, token string) {
	for name := range cfg.AuthInfos {
		cfg.AuthInfos[name] = &clientcmdapi.AuthInfo{Token: token}
	}
	if len(cfg.AuthInfos) == 0 {
		cfg.AuthInfos["perigeos"] = &clientcmdapi.AuthInfo{Token: token}
		for _, ctx := range cfg.Contexts {
			ctx.AuthInfo = "perigeos"
		}
	}
}

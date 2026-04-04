package join

import (
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{
			name:    "kubeconfig only",
			opts:    Options{Kubeconfig: "/tmp/kc"},
			wantErr: false,
		},
		{
			name:    "api-server + token",
			opts:    Options{APIServer: "https://k8s:6443", Token: "abc"},
			wantErr: false,
		},
		{
			name:    "missing everything",
			opts:    Options{},
			wantErr: true,
		},
		{
			name:    "api-server without token",
			opts:    Options{APIServer: "https://k8s:6443"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSanitizePawnName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"myhost", "myhost"},
		{"MY_HOST", "my-host"},
		{"my.host.local", "my-host-local"},
		{"", "pawn-01"},
		{"-leading", "leading"},
	}
	for _, c := range cases {
		got := sanitizePawnName(c.in)
		if got != c.want {
			t.Errorf("sanitizePawnName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestGenerateConfig(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{
		ConfigDir: dir,
		BaseDir:   "/var/lib/apsis/perigeos",
	}

	logger := slog.Default()

	path, err := generateConfig(opts, true, false, logger)
	if err != nil {
		t.Fatalf("generateConfig: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	content := string(data)
	if len(content) == 0 {
		t.Fatal("generated config is empty")
	}

	// Calling again should be a no-op (idempotent).
	path2, err := generateConfig(opts, false, false, logger)
	if err != nil {
		t.Fatalf("second generateConfig: %v", err)
	}
	if path != path2 {
		t.Errorf("path changed on second call: %q vs %q", path, path2)
	}

	// Content should be unchanged (first call wins).
	data2, _ := os.ReadFile(path2)
	if string(data2) != content {
		t.Error("config content changed on second call")
	}
}

func TestGenerateConfigCNI(t *testing.T) {
	t.Run("cni enabled", func(t *testing.T) {
		dir := t.TempDir()
		opts := &Options{ConfigDir: dir, BaseDir: "/var/lib/apsis/perigeos"}

		path, err := generateConfig(opts, true, true, slog.Default())
		if err != nil {
			t.Fatalf("generateConfig: %v", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read config: %v", err)
		}
		content := string(data)

		// [global.cni] should be uncommented.
		if !strings.Contains(content, "\n[global.cni]\n") {
			t.Error("expected uncommented [global.cni] when constellationCNI=true")
		}
		if strings.Contains(content, "# [global.cni]") {
			t.Error("expected no commented-out [global.cni] when constellationCNI=true")
		}
		if !strings.Contains(content, "Constellation CNI detected") {
			t.Error("expected detection comment when constellationCNI=true")
		}
	})

	t.Run("cni disabled", func(t *testing.T) {
		dir := t.TempDir()
		opts := &Options{ConfigDir: dir, BaseDir: "/var/lib/apsis/perigeos"}

		path, err := generateConfig(opts, true, false, slog.Default())
		if err != nil {
			t.Fatalf("generateConfig: %v", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read config: %v", err)
		}
		content := string(data)

		// [global.cni] should be commented out.
		if !strings.Contains(content, "# [global.cni]") {
			t.Error("expected commented [global.cni] when constellationCNI=false")
		}
		if strings.Contains(content, "Constellation CNI detected") {
			t.Error("expected no detection comment when constellationCNI=false")
		}
		if !strings.Contains(content, "was not detected") {
			t.Error("expected 'was not detected' comment when constellationCNI=false")
		}
	})
}


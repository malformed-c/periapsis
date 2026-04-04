package image

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractLayer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "extract-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	files := []struct {
		Name     string
		Body     string
		Linkname string
		Type     byte
		Mode     int64
	}{
		{Name: "file.txt", Body: "hello world", Type: tar.TypeReg, Mode: 0644},
		{Name: "dir/", Type: tar.TypeDir, Mode: 0755},
		{Name: "dir/subdir/", Type: tar.TypeDir, Mode: 0755},
		{Name: "dir/subdir/nested.txt", Body: "nested content", Type: tar.TypeReg, Mode: 0644},
		{Name: "symlink.txt", Linkname: "file.txt", Type: tar.TypeSymlink, Mode: 0644},
		{Name: "hardlink.txt", Linkname: "file.txt", Type: tar.TypeLink, Mode: 0644},
	}

	for _, f := range files {
		hdr := &tar.Header{
			Name:     f.Name,
			Mode:     f.Mode,
			Size:     int64(len(f.Body)),
			Typeflag: f.Type,
			Linkname: f.Linkname,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if f.Type == tar.TypeReg {
			if _, err := tw.Write([]byte(f.Body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	tw.Close()

	tr := tar.NewReader(&buf)
	if err := extractLayer(tmpDir, tr); err != nil {
		t.Fatalf("extractLayer failed: %v", err)
	}

	// Verify files
	for _, f := range files {
		path := filepath.Join(tmpDir, f.Name)
		switch f.Type {
		case tar.TypeDir:
			fi, err := os.Stat(path)
			if err != nil || !fi.IsDir() {
				t.Errorf("expected directory %s to exist", f.Name)
			}
		case tar.TypeReg:
			content, err := os.ReadFile(path)
			if err != nil || string(content) != f.Body {
				t.Errorf("expected file %s to have content %q, got %q (err: %v)", f.Name, f.Body, string(content), err)
			}
		case tar.TypeSymlink:
			link, err := os.Readlink(path)
			if err != nil || link != f.Linkname {
				t.Errorf("expected symlink %s to point to %q, got %q (err: %v)", f.Name, f.Linkname, link, err)
			}
		case tar.TypeLink:
			fi1, err := os.Stat(filepath.Join(tmpDir, f.Linkname))
			if err != nil {
				t.Fatal(err)
			}
			fi2, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(fi1, fi2) {
				t.Errorf("expected %s to be a hardlink to %s", f.Name, f.Linkname)
			}
		}
	}
}

func TestExtractLayerZipSlip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "zipslip-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name string
		path string
	}{
		{"parent", "../evil.txt"},
		{"absolute", "/tmp/evil.txt"},
		{"deep", "ok/../../evil.txt"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)

			hdr := &tar.Header{
				Name:     tc.path,
				Mode:     0644,
				Size:     int64(len("evil data")),
				Typeflag: tar.TypeReg,
			}
			if err := tw.WriteHeader(hdr); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write([]byte("evil data")); err != nil {
				t.Fatal(err)
			}
			tw.Close()

			tr := tar.NewReader(&buf)
			err = extractLayer(tmpDir, tr)
			if err == nil || !strings.Contains(err.Error(), "security violation") {
				t.Errorf("expected security violation error for %s, got %v", tc.path, err)
			}
		})
	}
}

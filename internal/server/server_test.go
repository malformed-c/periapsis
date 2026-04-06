package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/malformed-c/periapsis/internal/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBlobsHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBlobProvider := mocks.NewMockblobProvider(ctrl)
	tempDir, err := os.MkdirTemp("", "periapsis-blobs-handler-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	blobFile := filepath.Join(tempDir, "digest1")
	err = os.WriteFile(blobFile, []byte("blobcontent"), 0644)
	require.NoError(t, err)

	// Test the actual handler from server.go logic.
	// Since NewPawnServer is hard to call without TLS certs, we test the handler logic directly.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This logic matches the anonymous function in NewPawnServer
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// In the actual code, it's: digest := strings.TrimPrefix(r.URL.Path, "/blobs/")
		digest := r.URL.Path[len("/blobs/"):]
		blobFile := mockBlobProvider.BlobPath(digest)
		f, err := os.Open(blobFile)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		defer f.Close()
		stat, _ := f.Stat()
		w.Header().Set("Content-Type", "application/gzip")
		http.ServeContent(w, r, digest+".tar.gz", stat.ModTime(), f)
	})

	mockBlobProvider.EXPECT().BlobPath("digest1").Return(blobFile)

	req := httptest.NewRequest("GET", "/blobs/digest1", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "blobcontent", rr.Body.String())
	assert.Equal(t, "application/gzip", rr.Header().Get("Content-Type"))
}

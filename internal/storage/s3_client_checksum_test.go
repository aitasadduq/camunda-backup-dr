package storage

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// TestS3Client_PutObject_NoAwsChunked verifies that uploads do NOT use the
// "aws-chunked" content encoding or the streaming-trailer checksum. The AWS SDK
// default (RequestChecksumCalculation = WhenSupported) adds a default CRC32
// checksum that forces Content-Encoding: aws-chunked with an
// x-amz-content-sha256 of STREAMING-UNSIGNED-PAYLOAD-TRAILER. Several
// S3-compatible providers (e.g. Oracle OCI Object Storage) reject that, so
// NewS3Client sets RequestChecksumCalculation = WhenRequired. This test pins
// that behaviour by inspecting the actual request headers on the wire.
func TestS3Client_PutObject_NoAwsChunked(t *testing.T) {
	var (
		mu                sync.Mutex
		sawPut            bool
		contentEncoding   string
		contentSHA        string
		checksumAlgorithm string
		checksumCRC32     string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			mu.Lock()
			sawPut = true
			contentEncoding = r.Header.Get("Content-Encoding")
			contentSHA = r.Header.Get("x-amz-content-sha256")
			checksumAlgorithm = r.Header.Get("x-amz-sdk-checksum-algorithm")
			checksumCRC32 = r.Header.Get("x-amz-checksum-crc32")
			mu.Unlock()
			_, _ = io.Copy(io.Discard, r.Body)
		}
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewS3Client(S3Config{
		Endpoint:     server.URL,
		AccessKey:    "test",
		SecretKey:    "test",
		Bucket:       "test-bucket",
		Region:       "us-east-1",
		UsePathStyle: true,
	}, utils.NewLogger("debug"))
	if err != nil {
		t.Fatalf("NewS3Client failed: %v", err)
	}

	if err := client.StoreLatestBackupID("inst", "20240101120000"); err != nil {
		t.Fatalf("StoreLatestBackupID failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if !sawPut {
		t.Fatal("expected a PUT request to reach the S3 endpoint")
	}
	if strings.Contains(strings.ToLower(contentEncoding), "aws-chunked") {
		t.Errorf("PutObject used aws-chunked content encoding (breaks OCI): Content-Encoding=%q", contentEncoding)
	}
	if strings.Contains(strings.ToUpper(contentSHA), "STREAMING") {
		t.Errorf("PutObject used streaming trailer checksum (breaks OCI): x-amz-content-sha256=%q", contentSHA)
	}
	// The SDK default (WhenSupported) attaches a CRC32 checksum to every upload;
	// WhenRequired must not. This is the header OCI rejects, so its absence is
	// the precise regression guard.
	if checksumAlgorithm != "" {
		t.Errorf("PutObject attached a default checksum algorithm (breaks OCI): x-amz-sdk-checksum-algorithm=%q", checksumAlgorithm)
	}
	if checksumCRC32 != "" {
		t.Errorf("PutObject attached a default CRC32 checksum (breaks OCI): x-amz-checksum-crc32=%q", checksumCRC32)
	}
}

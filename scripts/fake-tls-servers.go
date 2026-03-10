// scripts/fake-tls-servers.go
//
// Starts 3 HTTPS servers with a self-signed certificate for testing
// PROBE_INSECURE_SKIP_VERIFY. No external tools or files needed.
//
// Usage:
//   go run scripts/fake-tls-servers.go
//
// Then in another terminal:
//   DATA_DIR=./data PROBE_INSECURE_SKIP_VERIFY=true PROBE_ALLOW_PRIVATE_IPS=true go run cmd/server/main.go
//
// Use these URLs in the UI:
//   Camunda:        https://localhost:8443
//   Elasticsearch:  https://localhost:9243
//   S3/MinIO:       https://localhost:9443

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	tlsCert, err := generateSelfSignedCert()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate certificate: %v\n", err)
		os.Exit(1)
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{tlsCert}}

	// --- Camunda fake ---
	camundaMux := http.NewServeMux()
	camundaMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"version":"8.3.0","product":"camunda-platform"}`)
	})

	// --- Elasticsearch fake ---
	esMux := http.NewServeMux()
	esMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
  "name": "fake-es",
  "cluster_name": "test-cluster",
  "cluster_uuid": "abc123",
  "version": { "number": "8.11.0" },
  "tagline": "You Know, for Search"
}`)
	})

	// --- S3/MinIO fake ---
	s3Mux := http.NewServeMux()
	s3Mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head><title>MinIO Console</title></head><body>MinIO</body></html>`)
	})

	servers := []struct {
		name string
		addr string
		mux  *http.ServeMux
	}{
		{"Camunda", ":8443", camundaMux},
		{"Elasticsearch", ":9243", esMux},
		{"S3/MinIO", ":9443", s3Mux},
	}

	for _, s := range servers {
		srv := &http.Server{
			Addr:      s.addr,
			Handler:   s.mux,
			TLSConfig: tlsConfig.Clone(),
		}
		name := s.name
		addr := s.addr
		go func() {
			fmt.Printf("  %-18s https://localhost%s\n", name, addr)
			if err := srv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "%s server error: %v\n", name, err)
			}
		}()
	}

	fmt.Println("\n✅ All fake HTTPS servers running (self-signed cert)")
	fmt.Println("   Press Ctrl+C to stop\n")
	fmt.Println("Test with PROBE_INSECURE_SKIP_VERIFY=true  → should connect")
	fmt.Println("Test with PROBE_INSECURE_SKIP_VERIFY unset → should show TLS error")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("\nShutting down...")
}

func generateSelfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))

	template := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"Fake Test CA"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),

		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,

		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:    []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return tls.X509KeyPair(certPEM, keyPEM)
}

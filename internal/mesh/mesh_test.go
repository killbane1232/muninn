package mesh

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/killbane1232/muninn/internal/store"
)

func testMesh(t *testing.T, mode, secret string) (*Mesh, *httptest.Server) {
	t.Helper()
	ts := httptest.NewUnstartedServer(nil)
	m, err := New(Config{Enabled: true, PublicURL: "http://" + ts.Listener.Addr().String(), Mode: mode, SharedSecret: secret, AllowPrivateNetworks: true}, store.NewMemory(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	m.RegisterRoutes(mux)
	ts.Config.Handler = mux
	ts.Start()
	t.Cleanup(func() { m.Close(); ts.Close() })
	return m, ts
}
func TestURLPolicy(t *testing.T) {
	for _, raw := range []string{"file:///etc/passwd", "ftp://node.example", "http://user:pass@node.example", "https://node.example/a", "https://node.example?x=1", "https://node.example#x", "http://127.0.0.1", "http://localhost", "http://[::1]", "http://10.0.0.1", "http://169.254.169.254", "http://100.64.0.1", "http://[::ffff:127.0.0.1]", "http://[fe80::1%25lo0]", "http://0.0.0.0", "http://224.0.0.1"} {
		if _, err := normalizeURL(raw, false); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{"http://169.254.169.254", "http://[fe80::1]", "http://0.0.0.0"} {
		if _, err := normalizeURL(raw, true); err == nil {
			t.Errorf("accepted metadata/unspecified %s", raw)
		}
	}
	got, err := normalizeURL(" HTTPS://EXAMPLE.com:443/ ", false)
	if err != nil || got != "https://example.com" {
		t.Fatalf("canonical URL %s %v", got, err)
	}
	if allowedIP(net.ParseIP("127.0.0.1"), false) || !allowedIP(net.ParseIP("127.0.0.1"), true) {
		t.Fatal("loopback policy")
	}
}
func TestDialPolicyAndRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("private endpoint reached") }))
	defer target.Close()
	cfg, _ := (Config{Enabled: true, PublicURL: "https://node.example"}).normalized()
	client := newClient(cfg)
	defer client.CloseIdleConnections()
	if _, err := client.Get(target.URL); err == nil {
		t.Fatal("dialed private literal")
	}
	if _, err := client.Get(strings.Replace(target.URL, "127.0.0.1", "localhost", 1)); err == nil {
		t.Fatal("DNS private address was allowed")
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer redirect.Close()
	cfg.AllowPrivateNetworks = true
	client = newClient(cfg)
	defer client.CloseIdleConnections()
	resp, err := client.Get(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 302 {
		t.Fatal("followed redirect")
	}
}
func TestClosedAuthenticationAndReplay(t *testing.T) {
	secret := strings.Repeat("secret", 8)
	m, ts := testMesh(t, "closed", secret)
	body := `{"version":1,"url":"https://other.example"}`
	var nonce [16]byte
	rand.Read(nonce[:])
	n := hex.EncodeToString(nonce[:])
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	send := func(signature string) int {
		req, _ := http.NewRequest("POST", ts.URL+"/api/v1/mesh/hello", strings.NewReader(body))
		req.Header.Set("X-Muninn-Mode", "closed")
		req.Header.Set("X-Muninn-Time", stamp)
		req.Header.Set("X-Muninn-Nonce", n)
		req.Header.Set("X-Muninn-MAC", signature)
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if send("wrong") != 401 {
		t.Fatal("accepted invalid MAC")
	}
	valid := mac(secret, "POST", "/api/v1/mesh/hello", stamp, n, body)
	if send(valid) != 200 {
		t.Fatal("rejected valid MAC")
	}
	if send(valid) != 429 {
		t.Fatal("accepted replay")
	}
	// Knowing a secret must not disclose it to a candidate node, and unsigned
	// responses cannot enroll an arbitrary HTTPS/HTTP service in a closed mesh.
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bytes, _ := io.ReadAll(r.Body)
		if strings.Contains(string(bytes), secret) || strings.Contains(r.Header.Get("Authorization"), secret) {
			t.Error("secret leaked")
		}
		json.NewEncoder(w).Encode(hello{Version: 1, URL: "https://fake.example"})
	}))
	defer candidate.Close()
	var h hello
	if err := m.exchange(context.Background(), candidate.URL, "/api/v1/mesh/hello", hello{Version: 1, URL: m.cfg.PublicURL}, &h); err == nil {
		t.Fatal("accepted unauthenticated response")
	}
}
func TestDiscoveryPersistsAcrossRestart(t *testing.T) {
	a, _ := testMesh(t, "open", "")
	b, ts := testMesh(t, "open", "")
	a.cfg.StateFile = filepath.Join(t.TempDir(), "nodes.json")
	a.learn([]string{ts.URL})
	a.Sync(context.Background())
	if len(a.Nodes()) != 1 {
		t.Fatal("discovery failed")
	}
	// Hello makes the connection reciprocal even with a one-sided seed list.
	b.Sync(context.Background())
	if len(b.Nodes()) != 1 {
		t.Fatal("reciprocal discovery failed")
	}
	restored, err := New(a.cfg, store.NewMemory(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if len(restored.Nodes()) != 0 {
		t.Fatal("persisted node must be revalidated")
	}
	ts.Close()
	restored.Sync(context.Background())
	data, err := os.ReadFile(a.cfg.StateFile)
	if err != nil || !strings.Contains(string(data), ts.URL) {
		t.Fatalf("offline saved node lost: %s %v", data, err)
	}
}
func TestConfigValidation(t *testing.T) {
	for _, c := range []Config{
		{Enabled: true, PublicURL: "https://node.example", Mode: "invalid"},
		{Enabled: true, PublicURL: "https://node.example", Mode: "closed", SharedSecret: "short"},
		{Enabled: true, PublicURL: "https://node.example", SharedSecret: strings.Repeat("s", 32)},
		{Enabled: true, PublicURL: "https://node.example", MaxNodes: 257},
	} {
		if _, err := c.normalized(); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "mesh.json")
	os.WriteFile(file, []byte(`{"enabled":true,"public_url":"https://node.example","state_file":"nodes.json"}`), 0600)
	c, err := LoadConfig(file)
	if err != nil || c.StateFile != filepath.Join(dir, "nodes.json") {
		t.Fatalf("config: %+v %v", c, err)
	}
	os.WriteFile(file, []byte(`{"enabled":true,"typo":true}`), 0600)
	if _, err := LoadConfig(file); err == nil {
		t.Fatal("unknown field silently accepted")
	}
}

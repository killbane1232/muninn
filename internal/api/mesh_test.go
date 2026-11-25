package api_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/killbane1232/muninn/internal/api"
	"github.com/killbane1232/muninn/internal/mesh"
	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
	"github.com/killbane1232/muninn/internal/store"
	_ "modernc.org/sqlite"
)

type node struct {
	unavailable atomic.Bool
	server      *api.Server
	http        *httptest.Server
	local       store.Store
}

func newNode(t *testing.T, driver, mode, secret string, seeds ...string) *node {
	t.Helper()
	var st store.Store = store.NewMemory()
	if driver == "sqlite" {
		var err error
		st, err = store.NewDB("sqlite", "file:"+filepath.Join(t.TempDir(), "muninn.db"))
		if err != nil {
			t.Fatal(err)
		}
	}
	ts := httptest.NewUnstartedServer(nil)
	cfg := api.DefaultConfig()
	cfg.PurgeInterval = 0
	cfg.Mesh = mesh.Config{Enabled: true, PublicURL: "http://" + ts.Listener.Addr().String(), Mode: mode, SharedSecret: secret, Seeds: seeds, AllowPrivateNetworks: true, SyncIntervalSeconds: 1, RequestTimeoutSeconds: 2, StateFile: filepath.Join(t.TempDir(), "nodes.json")}
	srv, err := api.NewServer(cfg, st)
	if err != nil {
		t.Fatal(err)
	}
	n := &node{server: srv, http: ts, local: st}
	ts.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.unavailable.Load() {
			http.Error(w, "temporarily unavailable", 503)
			return
		}
		srv.Handler.ServeHTTP(w, r)
	})
	ts.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		ts.Close()
		if c, ok := st.(interface{ Close() error }); ok {
			c.Close()
		}
	})
	return n
}
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	until := time.Now().Add(7 * time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("mesh did not converge")
}
func request(t *testing.T, n *node, method, path string, body any, status int, out any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(method, n.http.URL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != status {
		t.Fatalf("%s %s: status %d want %d: %s", method, path, resp.StatusCode, status, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
	}
}
func register(t *testing.T, n *node, id string, hints ...string) (model.Peer, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	req := model.RegisterRequest{ID: id, Login: id, SignatureKey: sign.PublicKeyBase64(pub), EncryptionKey: "encryption-" + id, TTLSeconds: 300, MuninnServers: hints}
	request(t, n, "POST", "/api/v1/peers", req, 200, nil)
	p, err := n.local.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p, priv
}
func ws(t *testing.T, n *node, id string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(n.http.URL, "http")+"/api/v1/ws?peer_id="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func receive(t *testing.T, c *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(4 * time.Second))
	var v map[string]json.RawMessage
	if err := c.ReadJSON(&v); err != nil {
		t.Fatal(err)
	}
	return v
}
func TestMeshCommunication(t *testing.T) {
	for _, driver := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"open", "closed"} {
			t.Run(driver+"/"+mode, func(t *testing.T) {
				secret := ""
				if mode == "closed" {
					secret = strings.Repeat("secret", 8)
				}
				a := newNode(t, driver, mode, secret)
				b := newNode(t, driver, mode, secret, a.http.URL)
				c := newNode(t, driver, mode, secret, b.http.URL)
				alice, aliceKey := register(t, a, "alice")
				bob, bobKey := register(t, c, "bob")
				eventually(t, func() bool {
					return len(a.server.Mesh.Nodes()) == 2 && len(b.server.Mesh.Nodes()) == 2 && len(c.server.Mesh.Nodes()) == 2
				})
				var peers []model.Peer
				request(t, a, "GET", "/api/v1/peers", nil, 200, &peers)
				if len(peers) != 2 {
					t.Fatalf("peers: %+v", peers)
				}
				p, err := a.server.Mesh.Get(context.Background(), bob.ID)
				if err != nil || p.SignatureKey != bob.SignatureKey {
					t.Fatalf("remote lookup: %+v %v", p, err)
				}
				ps, err := a.server.Mesh.GetByKey(context.Background(), bob.Login, bob.SignatureKey)
				if err != nil || len(ps) != 1 {
					t.Fatalf("remote key lookup: %v %v", ps, err)
				}
				if _, err := a.local.Get(context.Background(), bob.ID); err != store.ErrNotFound {
					t.Fatal("remote peer leaked into local store")
				}
				aw, bw := ws(t, a, alice.ID), ws(t, c, bob.ID)
				if err := aw.WriteJSON(map[string]any{"id": "offer", "method": "connect_to_peer", "params": map[string]string{"target_id": bob.ID, "offer": "alice SDP"}}); err != nil {
					t.Fatal(err)
				}
				incoming := receive(t, bw)
				var signal model.Signal
				json.Unmarshal(incoming["params"], &signal)
				if signal.From != alice.ID || signal.Data != "alice SDP" {
					t.Fatalf("offer: %+v", signal)
				}
				ack := receive(t, aw)
				if ack["error"] != nil {
					t.Fatalf("offer ack: %s", ack["error"])
				}
				if err := bw.WriteJSON(map[string]any{"id": "answer", "method": "signal_relay", "params": map[string]string{"target_id": alice.ID, "from": "spoof", "type": "answer", "data": "bob SDP"}}); err != nil {
					t.Fatal(err)
				}
				incoming = receive(t, aw)
				json.Unmarshal(incoming["params"], &signal)
				if signal.From != bob.ID || signal.Data != "bob SDP" {
					t.Fatalf("answer: %+v", signal)
				}
				receive(t, bw)
				// HTTP fallback must push to an already connected remote WebSocket.
				request(t, a, "POST", "/api/v1/peers/bob/signals", model.Signal{From: alice.ID, Type: "offer", Data: "HTTP SDP"}, 204, nil)
				incoming = receive(t, bw)
				json.Unmarshal(incoming["params"], &signal)
				if signal.Data != "HTTP SDP" {
					t.Fatal(signal)
				}
				// A disconnected endpoint receives its queue once at the owning server.
				offline, _ := register(t, c, "offline")
				request(t, a, "POST", "/api/v1/peers/offline/signals", model.Signal{From: alice.ID, Type: "offer", Data: "queued"}, 204, nil)
				queued := ws(t, c, offline.ID)
				incoming = receive(t, queued)
				json.Unmarshal(incoming["params"], &signal)
				if signal.Data != "queued" {
					t.Fatal(signal)
				}
				var signals []model.Signal
				request(t, c, "GET", "/api/v1/peers/offline/signals", nil, 200, &signals)
				if len(signals) != 0 {
					t.Fatal("queue delivered twice")
				}
				const file = "mesh-message"
				const hash = "deadbeef01234567"
				chunk := model.RegisterChunkRequest{SenderID: alice.Key(), RecipientID: bob.Key(), Hash: hash, Signature: sign.Sign(aliceKey, sign.ExpectedPayload(file, 0, hash)), PeerID: alice.ID, TTL: 86400}
				request(t, a, "PUT", "/api/v1/files/"+file+"/chunks/0", chunk, 204, nil)
				// Another server may register a replica signed by a remote sender.
				chunk.PeerID = bob.ID
				request(t, c, "PUT", "/api/v1/files/"+file+"/chunks/0", chunk, 204, nil)
				chunks, err := c.server.Mesh.GetChunksByRecipient(context.Background(), bob.Key(), time.Now().Unix()+3600)
				if err != nil || len(chunks) != 2 {
					t.Fatalf("federated records bypass cursor: %+v %v", chunks, err)
				}
				report := model.ChunkReportRequest{ReporterID: bob.ID, FileID: file, ChunkIndex: 0, Hash: hash, Signature: sign.Sign(bobKey, sign.ReportedPayload(file, 0, hash, alice.ID))}
				request(t, c, "POST", "/api/v1/peers/alice/chunk-reports", report, 200, nil)
				confirm := model.ConfirmChunkRequest{RecipientID: bob.Key(), FileID: file, ChunkIndex: 0, Hash: hash, Signature: sign.Sign(bobKey, sign.ConfirmedPayload(file, 0, hash))}
				request(t, c, "POST", "/api/v1/chunks/confirm", confirm, 200, nil)
				before, _ := a.local.Get(context.Background(), alice.ID)
				request(t, c, "POST", "/api/v1/chunks/confirm", confirm, 200, nil)
				after, _ := a.local.Get(context.Background(), alice.ID)
				if before.QualityScore != after.QualityScore {
					t.Fatal("confirmation retry changed quality")
				}
				request(t, c, "POST", "/api/v1/chunks/read", model.ReadChunkRequest{RecipientID: bob.Key(), FileID: file, Signature: sign.Sign(bobKey, sign.ReadPayload(file))}, 200, nil)
				chunks, err = a.local.GetChunksByFileID(context.Background(), file)
				if err != nil || len(chunks) != 1 || !chunks[0].Confirmed || !chunks[0].Readed {
					t.Fatalf("receipts: %+v %v", chunks, err)
				}
				confirm.Signature = sign.Sign(aliceKey, sign.ConfirmedPayload(file, 0, hash))
				request(t, c, "POST", "/api/v1/chunks/confirm", confirm, 401, nil)
			})
		}
	}
}
func TestRegistrationDiscoversServer(t *testing.T) {
	a := newNode(t, "memory", "open", "")
	b := newNode(t, "memory", "open", "")
	register(t, a, "alice", b.http.URL)
	eventually(t, func() bool { return len(a.server.Mesh.Nodes()) == 1 && len(b.server.Mesh.Nodes()) == 1 })
	request(t, a, "POST", "/api/v1/peers", model.RegisterRequest{ID: "bad", MuninnServers: []string{"http://169.254.169.254"}}, 200, nil)
}
func TestMeshModesAreIsolated(t *testing.T) {
	a := newNode(t, "memory", "closed", strings.Repeat("a", 32))
	b := newNode(t, "memory", "closed", strings.Repeat("b", 32), a.http.URL)
	c := newNode(t, "memory", "open", "", a.http.URL)
	time.Sleep(200 * time.Millisecond)
	if len(a.server.Mesh.Nodes()) != 0 || len(b.server.Mesh.Nodes()) != 0 || len(c.server.Mesh.Nodes()) != 0 {
		t.Fatal("incompatible mesh nodes enrolled")
	}
	// The normal public API remains usable in a closed mesh.
	register(t, a, "alice")
}
func TestMeshDisabled(t *testing.T) {
	cfg := api.DefaultConfig()
	cfg.PurgeInterval = 0
	srv, err := api.NewServer(cfg, store.NewMemory())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())
	r := httptest.NewRecorder()
	srv.Handler.ServeHTTP(r, httptest.NewRequest("POST", "/api/v1/mesh/hello", strings.NewReader("{}")))
	if r.Code != 404 {
		t.Fatal(fmt.Sprint(r.Code))
	}
}

func TestUnavailableNodeRecoversWithoutSkippingOldMessages(t *testing.T) {
	a := newNode(t, "memory", "open", "")
	b := newNode(t, "memory", "open", "", a.http.URL)
	alice, _ := register(t, a, "alice")
	bob, key := register(t, b, "bob")
	eventually(t, func() bool { return len(a.server.Mesh.Nodes()) == 1 && len(b.server.Mesh.Nodes()) == 1 })
	const file = "old-message"
	const hash = "deadbeef01234567"
	request(t, b, "PUT", "/api/v1/files/"+file+"/chunks/0", model.RegisterChunkRequest{SenderID: bob.Key(), RecipientID: alice.Key(), Hash: hash, Signature: sign.Sign(key, sign.ExpectedPayload(file, 0, hash)), PeerID: bob.ID, TTL: 86400}, 204, nil)
	b.unavailable.Store(true)
	records, err := a.server.Mesh.GetChunksByRecipient(context.Background(), alice.Key(), time.Now().Unix()+3600)
	if err != nil || len(records) != 0 {
		t.Fatalf("partial read: %+v %v", records, err)
	}
	request(t, a, "GET", "/api/v1/peers/unknown", nil, 503, nil)
	b.unavailable.Store(false)
	records, err = a.server.Mesh.GetChunksByRecipient(context.Background(), alice.Key(), time.Now().Unix()+3600)
	if err != nil || len(records) != 1 || records[0].FileID != file {
		t.Fatalf("recovered old record: %+v %v", records, err)
	}
	// Deleting/expiring a peer is immediately visible; there is no replicated TTL.
	request(t, b, "DELETE", "/api/v1/peers/bob", nil, 204, nil)
	request(t, a, "GET", "/api/v1/peers/bob", nil, 404, nil)
}

func TestConcurrentMeshSignalsAndReconnect(t *testing.T) {
	a := newNode(t, "memory", "open", "")
	b := newNode(t, "memory", "open", "", a.http.URL)
	register(t, a, "alice")
	register(t, b, "bob")
	eventually(t, func() bool { return len(a.server.Mesh.Nodes()) == 1 && len(b.server.Mesh.Nodes()) == 1 })
	old := ws(t, b, "bob")
	current := ws(t, b, "bob")
	old.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := old.ReadMessage(); err == nil {
		t.Fatal("old connection remained open")
	}
	const count = 24
	failures := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			body, _ := json.Marshal(model.Signal{From: "alice", Type: "offer", Data: fmt.Sprint(i)})
			resp, err := http.Post(a.http.URL+"/api/v1/peers/bob/signals", "application/json", bytes.NewReader(body))
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode != 204 {
					err = fmt.Errorf("signal status %d", resp.StatusCode)
				}
			}
			failures <- err
		}(i)
	}
	seen := map[string]bool{}
	for i := 0; i < count; i++ {
		msg := receive(t, current)
		var sig model.Signal
		json.Unmarshal(msg["params"], &sig)
		seen[sig.Data] = true
	}
	for i := 0; i < count; i++ {
		if err := <-failures; err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != count {
		t.Fatalf("received %d unique signals", len(seen))
	}
}

func TestClientHandoverPrefersLiveSocketWithoutDeletingOldPeer(t *testing.T) {
	a := newNode(t, "sqlite", "closed", strings.Repeat("s", 32))
	b := newNode(t, "sqlite", "closed", strings.Repeat("s", 32), a.http.URL)
	alice, _ := register(t, a, "alice")
	register(t, a, "sender")
	eventually(t, func() bool { return len(a.server.Mesh.Nodes()) == 1 && len(b.server.Mesh.Nodes()) == 1 })
	// Preserve Alice's old endpoint: switching a client does not delete it.
	request(t, b, "POST", "/api/v1/peers", model.RegisterRequest{ID: alice.ID, Login: alice.Login, SignatureKey: alice.SignatureKey, EncryptionKey: alice.EncryptionKey}, 200, nil)
	if _, err := a.local.Get(context.Background(), alice.ID); err != nil {
		t.Fatal("old peer was removed")
	}
	request(t, a, "POST", "/api/v1/peers/alice/signals", model.Signal{From: "sender", Type: "offer", Data: "during handover"}, 204, nil)
	current := ws(t, b, alice.ID)
	message := receive(t, current)
	var signal model.Signal
	json.Unmarshal(message["params"], &signal)
	if signal.Data != "during handover" {
		t.Fatalf("queued signal: %+v", signal)
	}
	// A still has a local directory record, but the socket on B must win.
	request(t, a, "POST", "/api/v1/peers/alice/signals", model.Signal{From: "sender", Type: "offer", Data: "after handover"}, 204, nil)
	message = receive(t, current)
	json.Unmarshal(message["params"], &signal)
	if signal.Data != "after handover" {
		t.Fatalf("live signal: %+v", signal)
	}
	oldQueue, err := a.local.PollSignals(context.Background(), alice.ID)
	if err != nil || len(oldQueue) != 0 {
		t.Fatalf("signal stranded at previous home: %v %v", oldQueue, err)
	}
}

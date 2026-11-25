package mesh

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
	"github.com/killbane1232/muninn/internal/store"
	"github.com/killbane1232/muninn/internal/webrtc"
	_ "modernc.org/sqlite"
)

type testLinks struct {
	mu      sync.Mutex
	allowed map[string]bool
	calls   map[string]int
	drop    string
	dropped bool
	hang    string
}
type gatedTransport struct {
	base  http.RoundTripper
	from  string
	links *testLinks
}

func (g gatedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	edge := g.from + " -> " + r.URL.Scheme + "://" + r.URL.Host
	g.links.mu.Lock()
	allowed := g.links.allowed[edge]
	hang := g.links.hang == edge
	g.links.calls[edge+" "+r.URL.Path]++
	drop := g.links.drop == edge && r.URL.Path == relayPath && !g.links.dropped
	if drop {
		g.links.dropped = true
	}
	g.links.mu.Unlock()
	if !allowed {
		if hang {
			<-r.Context().Done()
		}
		return nil, fmt.Errorf("test partition: %s", edge)
	}
	response, err := g.base.RoundTrip(r)
	if err == nil && drop {
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return nil, io.ErrUnexpectedEOF
	}
	return response, err
}
func (g gatedTransport) CloseIdleConnections() {
	if c, ok := g.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

type transitNetwork struct {
	nodes   []*Mesh
	servers []*httptest.Server
	links   *testLinks
}

func newTransitNetwork(t *testing.T, mode, driver string, n int) *transitNetwork {
	t.Helper()
	network := &transitNetwork{links: &testLinks{allowed: map[string]bool{}, calls: map[string]int{}}}
	secret := ""
	if mode == "closed" {
		secret = strings.Repeat("s", 32)
	}
	for i := 0; i < n; i++ {
		var st store.Store = store.NewMemory()
		if driver == "sqlite" {
			var err error
			st, err = store.NewDB("sqlite", "file:"+filepath.Join(t.TempDir(), "muninn.db"))
			if err != nil {
				t.Fatal(err)
			}
		}
		rtc := webrtc.NewHandler(st)
		ts := httptest.NewUnstartedServer(nil)
		m, err := New(Config{Enabled: true, PublicURL: "http://" + ts.Listener.Addr().String(), Mode: mode, SharedSecret: secret, AllowPrivateNetworks: true}, st, rtc.DeliverLocal)
		if err != nil {
			t.Fatal(err)
		}
		rtc.RouteSignal = m.SetSignal
		mux := http.NewServeMux()
		m.RegisterRoutes(mux)
		mux.HandleFunc("/api/v1/ws", rtc.HandleWebSocket)
		ts.Config.Handler = mux
		ts.Start()
		m.client.Transport = gatedTransport{base: m.client.Transport, from: ts.URL, links: network.links}
		network.nodes = append(network.nodes, m)
		network.servers = append(network.servers, ts)
		t.Cleanup(func() {
			rtc.CloseAll()
			m.Close()
			ts.Close()
			if c, ok := st.(interface{ Close() error }); ok {
				c.Close()
			}
		})
	}
	for i := 0; i < n-1; i++ {
		network.link(i, i+1, true)
		network.link(i+1, i, true)
		network.nodes[i].learn([]string{network.servers[i+1].URL})
		network.nodes[i+1].learn([]string{network.servers[i].URL})
	}
	network.sync(t)
	return network
}
func (n *transitNetwork) link(from, to int, allowed bool) {
	n.links.mu.Lock()
	defer n.links.mu.Unlock()
	n.links.allowed[n.servers[from].URL+" -> "+n.servers[to].URL] = allowed
}
func (n *transitNetwork) sync(t *testing.T) {
	t.Helper()
	for i := 0; i < len(n.nodes)+2; i++ {
		for _, node := range n.nodes {
			node.Sync(context.Background())
		}
	}
	for _, node := range n.nodes {
		if len(node.Nodes()) != len(n.nodes)-1 {
			t.Fatalf("discovery at %s: %v", node.cfg.PublicURL, node.Nodes())
		}
	}
}
func registerTransitPeer(t *testing.T, m *Mesh, id string) (model.Peer, ed25519.PrivateKey) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Upsert(context.Background(), model.RegisterRequest{ID: id, Login: id, SignatureKey: sign.PublicKeyBase64(pub), PeerFlag: model.PeerFlagThick}); err != nil {
		t.Fatal(err)
	}
	peer, err := m.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return peer, key
}
func transitSocket(t *testing.T, m *Mesh, id string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(m.cfg.PublicURL, "http")+"/api/v1/ws?peer_id="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func receiveTransit(t *testing.T, c *websocket.Conn) (model.Signal, bool) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var message struct {
		Method string       `json:"method"`
		Params model.Signal `json:"params"`
		Error  string       `json:"error"`
	}
	if err := c.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	if message.Error != "" {
		t.Fatal(message.Error)
	}
	return message.Params, message.Method == "incoming_signal"
}
func TestTransitCommunication(t *testing.T) {
	for _, driver := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"open", "closed"} {
			t.Run(driver+"/"+mode, func(t *testing.T) {
				network := newTransitNetwork(t, mode, driver, 3)
				a, c := network.nodes[0], network.nodes[2]
				a.mu.RLock()
				direct := a.nodes[c.cfg.PublicURL].Direct
				a.mu.RUnlock()
				if direct {
					t.Fatal("partitioned node marked direct")
				}
				alice, aliceKey := registerTransitPeer(t, a, "alice")
				bob, bobKey := registerTransitPeer(t, c, "bob")
				peers, err := a.List(context.Background())
				if err != nil || len(peers) != 2 {
					t.Fatalf("directory: %v %v", peers, err)
				}
				peers, err = a.GetByKey(context.Background(), bob.Login, bob.SignatureKey)
				if err != nil || len(peers) != 1 {
					t.Fatalf("key: %v %v", peers, err)
				}
				peers, err = c.GetBestThickPeers(context.Background(), 10)
				if err != nil || len(peers) != 2 {
					t.Fatalf("storage peers: %v %v", peers, err)
				}
				aw, bw := transitSocket(t, a, alice.ID), transitSocket(t, c, bob.ID)
				if err := aw.WriteJSON(map[string]any{"id": "offer", "method": "connect_to_peer", "params": map[string]string{"target_id": bob.ID, "offer": "SDP"}}); err != nil {
					t.Fatal(err)
				}
				sig, incoming := receiveTransit(t, bw)
				if !incoming || sig.From != alice.ID || sig.Data != "SDP" {
					t.Fatalf("offer: %+v", sig)
				}
				receiveTransit(t, aw)
				if err := bw.WriteJSON(map[string]any{"id": "answer", "method": "signal_relay", "params": map[string]string{"target_id": alice.ID, "type": "answer", "data": "answer SDP"}}); err != nil {
					t.Fatal(err)
				}
				sig, incoming = receiveTransit(t, aw)
				if !incoming || sig.From != bob.ID || sig.Data != "answer SDP" {
					t.Fatalf("answer: %+v", sig)
				}
				receiveTransit(t, bw)
				offline, _ := registerTransitPeer(t, c, "offline")
				if err := a.SetSignal(context.Background(), offline.ID, model.Signal{From: alice.ID, Type: "offer", Data: "queued"}); err != nil {
					t.Fatal(err)
				}
				signals, err := c.Store.PollSignals(context.Background(), offline.ID)
				if err != nil || len(signals) != 1 {
					t.Fatalf("queue: %v %v", signals, err)
				}
				const file = "transit-message"
				const hash = "deadbeef01234567"
				if err := a.SetChunkHash(context.Background(), file, 0, model.RegisterChunkRequest{SenderID: alice.Key(), RecipientID: bob.Key(), Hash: hash, Signature: sign.Sign(aliceKey, sign.ExpectedPayload(file, 0, hash)), PeerID: alice.ID, TTL: 86400}); err != nil {
					t.Fatal(err)
				}
				chunks, err := c.GetChunksByRecipient(context.Background(), bob.Key(), time.Now().Unix()+3600)
				if err != nil || len(chunks) != 1 {
					t.Fatalf("metadata: %v %v", chunks, err)
				}
				_, err = c.ReportChunk(context.Background(), alice.ID, model.ChunkReportRequest{ReporterID: bob.ID, FileID: file, ChunkIndex: 0, Hash: hash, Signature: sign.Sign(bobKey, sign.ReportedPayload(file, 0, hash, alice.ID))})
				if err != nil {
					t.Fatal(err)
				}
				_, err = c.ConfirmChunk(context.Background(), model.ConfirmChunkRequest{RecipientID: bob.Key(), FileID: file, ChunkIndex: 0, Hash: hash, Signature: sign.Sign(bobKey, sign.ConfirmedPayload(file, 0, hash))})
				if err != nil {
					t.Fatal(err)
				}
				_, err = c.ReadChunk(context.Background(), model.ReadChunkRequest{RecipientID: bob.Key(), FileID: file, Signature: sign.Sign(bobKey, sign.ReadPayload(file))})
				if err != nil {
					t.Fatal(err)
				}
				chunks, err = a.Store.GetChunksByFileID(context.Background(), file)
				if err != nil || len(chunks) != 1 || !chunks[0].Confirmed || !chunks[0].Readed {
					t.Fatalf("receipts: %v %v", chunks, err)
				}
			})
		}
	}
}

func TestTransitMultipleHopsAndRecovery(t *testing.T) {
	n := newTransitNetwork(t, "closed", "memory", 4)
	a, d := n.nodes[0], n.nodes[3]
	peer, _ := registerTransitPeer(t, d, "destination")
	if _, err := a.Get(context.Background(), peer.ID); err != nil {
		t.Fatal(err)
	}
	paths := a.routePaths(d.cfg.PublicURL)
	if len(paths) == 0 || len(paths[0]) != 3 {
		t.Fatalf("multi-hop paths: %v", paths)
	}
	n.link(0, 1, false)
	if _, err := a.Get(context.Background(), peer.ID); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("partition error: %v", err)
	}
	n.link(0, 1, true)
	if _, err := a.Get(context.Background(), peer.ID); err != nil {
		t.Fatalf("route recovery: %v", err)
	}
	n.link(0, 3, true)
	n.link(3, 0, true)
	a.Sync(context.Background())
	d.Sync(context.Background())
	a.mu.RLock()
	direct := a.nodes[d.cfg.PublicURL].Direct
	a.mu.RUnlock()
	if !direct {
		t.Fatal("direct connection not restored")
	}
	n.link(0, 1, false)
	if _, err := a.Get(context.Background(), peer.ID); err != nil {
		t.Fatalf("restored direct route: %v", err)
	}
}

func TestLostDirectResponseUsesSameID(t *testing.T) {
	n := newTransitNetwork(t, "closed", "memory", 3)
	a, c := n.nodes[0], n.nodes[2]
	peer, _ := registerTransitPeer(t, c, "target")
	n.link(0, 2, true)
	n.link(2, 0, true)
	a.Sync(context.Background())
	c.Sync(context.Background())
	n.links.mu.Lock()
	n.links.drop = a.cfg.PublicURL + " -> " + c.cfg.PublicURL
	n.links.mu.Unlock()
	_, err := call[struct{}](a, context.Background(), c.cfg.PublicURL, "signal", signalRequest{ID: peer.ID, Signal: model.Signal{From: "source", Type: "offer", Data: "once"}})
	if err != nil {
		t.Fatal(err)
	}
	signals, err := c.Store.PollSignals(context.Background(), peer.ID)
	if err != nil || len(signals) != 1 {
		t.Fatalf("lost response duplicated delivery: %v %v", signals, err)
	}
	n.links.mu.Lock()
	dropped := n.links.dropped
	n.links.mu.Unlock()
	if !dropped {
		t.Fatal("response was not dropped")
	}
}

func relaySignal(t *testing.T, n *transitNetwork, id string) relayRequest {
	t.Helper()
	args, _ := json.Marshal(signalRequest{ID: "target", Signal: model.Signal{From: "source", Type: "offer", Data: "once"}})
	body, _ := json.Marshal(rpcRequest{Method: "signal", Args: args})
	return relayRequest{ID: id, Target: n.nodes[2].cfg.PublicURL, Path: rpcPath, Body: body, Route: []string{n.nodes[0].cfg.PublicURL, n.nodes[1].cfg.PublicURL, n.nodes[2].cfg.PublicURL}, Position: 1}
}
func TestRelayCachesAtEveryInstance(t *testing.T) {
	n := newTransitNetwork(t, "open", "memory", 3)
	a, b, c := n.nodes[0], n.nodes[1], n.nodes[2]
	registerTransitPeer(t, c, "target")
	req := relaySignal(t, n, strings.Repeat("a", 32))
	beforeKey := b.cfg.PublicURL + " -> " + c.cfg.PublicURL + " " + relayPath
	n.links.mu.Lock()
	before := n.links.calls[beforeKey]
	n.links.mu.Unlock()
	var wg sync.WaitGroup
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var response relayResponse
			err := a.wireExchange(context.Background(), b.cfg.PublicURL, relayPath, req, &response)
			if err == nil && (response.Error != "" || response.Target != c.cfg.PublicURL) {
				err = fmt.Errorf("relay response %+v", response)
			}
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	n.links.mu.Lock()
	forwarded := n.links.calls[beforeKey] - before
	n.links.mu.Unlock()
	if forwarded != 1 {
		t.Fatalf("intermediate forwarded %d times", forwarded)
	}
	signals, err := c.Store.PollSignals(context.Background(), "target")
	if err != nil || len(signals) != 1 {
		t.Fatalf("destination processed duplicates: %v %v", signals, err)
	}
	// Reusing an ID with different content is rejected, never executed again.
	req.Body = json.RawMessage(`{"method":"list","args":{}}`)
	var response relayResponse
	if err := a.wireExchange(context.Background(), b.cfg.PublicURL, relayPath, req, &response); err != nil || response.Error != "invalid" {
		t.Fatalf("ID collision: %+v %v", response, err)
	}
}

func TestTransitCacheExpiresAfterOneMinute(t *testing.T) {
	cache := newTransitCache()
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	cache.now = func() time.Time { return time.Unix(0, clock.Load()) }
	req := relayRequest{ID: strings.Repeat("1", 32), Target: "https://c.example", Path: rpcPath, Body: json.RawMessage(`{"method":"list","args":{}}`), Route: []string{"https://a.example", "https://b.example", "https://c.example"}, Position: 1}
	calls := 0
	run := func() relayResponse {
		calls++
		return relayResponse{Target: req.Target, Value: json.RawMessage(`{"value":[]}`)}
	}
	cache.do(context.Background(), req, run)
	clock.Add(int64(59 * time.Second))
	cache.do(context.Background(), req, run)
	if calls != 1 {
		t.Fatal("cache expired before one minute")
	}
	clock.Add(int64(time.Second))
	cache.do(context.Background(), req, run)
	if calls != 2 {
		t.Fatal("cache did not expire after one minute")
	}
}

func TestSlowDirectConnectionLeavesTimeForTransit(t *testing.T) {
	n := newTransitNetwork(t, "open", "memory", 3)
	a, c := n.nodes[0], n.nodes[2]
	registerTransitPeer(t, c, "target")
	n.link(0, 2, true)
	n.link(2, 0, true)
	a.Sync(context.Background())
	c.Sync(context.Background())
	n.link(0, 2, false)
	n.links.mu.Lock()
	n.links.hang = a.cfg.PublicURL + " -> " + c.cfg.PublicURL
	n.links.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	result, err := call[model.Peer](a, ctx, c.cfg.PublicURL, "get", query{ID: "target"})
	if err != nil || result.ID != "target" {
		t.Fatalf("fallback after direct timeout: %+v %v", result, err)
	}
}

func TestRelayRejectsCyclesHopOverflowAndUnknownNextHop(t *testing.T) {
	n := newTransitNetwork(t, "open", "memory", 3)
	a, b, c := n.nodes[0], n.nodes[1], n.nodes[2]
	req := relaySignal(t, n, strings.Repeat("b", 32))
	req.Route = []string{a.cfg.PublicURL, b.cfg.PublicURL, a.cfg.PublicURL, c.cfg.PublicURL}
	if b.validRelay(req) {
		t.Fatal("accepted cyclic route")
	}
	req.Route = []string{a.cfg.PublicURL, b.cfg.PublicURL, "https://d.example", "https://e.example", "https://f.example", c.cfg.PublicURL}
	if b.validRelay(req) {
		t.Fatal("accepted too many relays")
	}
	req.Route = []string{a.cfg.PublicURL, b.cfg.PublicURL, "https://unknown.example", c.cfg.PublicURL}
	body, _ := json.Marshal(req)
	response := b.relay(context.Background(), body).(relayResponse)
	if response.Error != "unavailable" {
		t.Fatalf("unknown next hop was not rejected: %+v", response)
	}
	req.Route = []string{a.cfg.PublicURL, b.cfg.PublicURL, c.cfg.PublicURL}
	req.Path = "/admin"
	if b.validRelay(req) {
		t.Fatal("accepted arbitrary HTTP path")
	}
	req.Path = rpcPath
	req.Route = []string{a.cfg.PublicURL, b.cfg.PublicURL, "http://169.254.169.254"}
	req.Target = req.Route[2]
	if b.validRelay(req) {
		t.Fatal("accepted metadata endpoint")
	}
}

func TestTransitCacheRetainsIDsUnderResponseMemoryPressure(t *testing.T) {
	cache := newTransitCache()
	cache.bytes = maxTransitBytes
	req := relayRequest{ID: strings.Repeat("2", 32), Target: "https://b.example", Path: rpcPath, Body: json.RawMessage(`{}`), Route: []string{"https://a.example", "https://b.example"}, Position: 1}
	var calls atomic.Int32
	run := func() relayResponse {
		calls.Add(1)
		return relayResponse{Target: req.Target, Value: json.RawMessage(`{"value":[]}`)}
	}
	if first := cache.do(context.Background(), req, run); first.Error != "" {
		t.Fatal(first.Error)
	}
	if repeat := cache.do(context.Background(), req, run); repeat.Error != "processed" {
		t.Fatal("missing retained ID")
	}
	if calls.Load() != 1 {
		t.Fatal("cache pressure re-executed request")
	}
}

func TestTransitUsesAlternativeIntermediate(t *testing.T) {
	n := newTransitNetwork(t, "closed", "memory", 4)
	a, target := n.nodes[0], n.nodes[3]
	// Turn the chain A-B-D-C into two alternatives A-B-C and A-D-C.
	n.link(0, 2, true)
	n.link(2, 0, true)
	n.link(1, 3, true)
	n.link(3, 1, true)
	n.sync(t)
	registerTransitPeer(t, target, "target")
	paths := a.routePaths(target.cfg.PublicURL)
	if len(paths) < 2 {
		t.Fatalf("missing alternative routes: %v", paths)
	}
	for index, server := range n.servers {
		if server.URL == paths[0][0] {
			n.link(0, index, false)
		}
	}
	_, err := call[struct{}](a, context.Background(), target.cfg.PublicURL, "signal", signalRequest{ID: "target", Signal: model.Signal{From: "source", Type: "offer", Data: "alternative"}})
	if err != nil {
		t.Fatal(err)
	}
	signals, err := target.Store.PollSignals(context.Background(), "target")
	if err != nil || len(signals) != 1 || signals[0].Data != "alternative" {
		t.Fatalf("alternative delivery: %v %v", signals, err)
	}
}

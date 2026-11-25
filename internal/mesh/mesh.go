// Package mesh federates directory, signaling and chunk metadata. Store calls
// fan out to destinations; bounded source routes relay them when necessary.
package mesh

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/store"
)

type lookupContextKey struct{}
type forwardContextKey struct{}
type signingResolver struct{ m *Mesh }

func (r signingResolver) Get(ctx context.Context, id string) (model.Peer, error) {
	return r.m.Get(context.WithValue(ctx, lookupContextKey{}, true), id)
}

type nodeState struct {
	Direct      bool
	Relay       bool
	Pinned      bool
	Verified    bool
	Failures    int
	NextAttempt time.Time
}
type Mesh struct {
	// Installed before serving, alongside the WebSocket delivery callback.
	SignalConnected func(string) bool
	inbound         chan struct{}
	relaySlots      chan struct{}
	transits        *transitCache
	routes          map[string]map[string]routeHint
	syncMu          sync.Mutex
	lookupSlots     chan struct{}
	store.Store
	cfg    Config
	client *http.Client
	mu     sync.RWMutex
	nodes  map[string]nodeState
	nonces map[string]time.Time
	slots  chan struct{}
	wake   chan struct{}
	cancel context.CancelFunc
	done   chan struct{}
	signal func(context.Context, string, model.Signal) error
}
type hello struct {
	Version int                 `json:"version"`
	Relay   bool                `json:"relay,omitempty"`
	Routes  map[string][]string `json:"routes,omitempty"`
	URL     string              `json:"url"`
	Nodes   []string            `json:"nodes,omitempty"`
}

func New(cfg Config, local store.Store, signal func(context.Context, string, model.Signal) error) (*Mesh, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return nil, fmt.Errorf("mesh is disabled")
	}
	if signal == nil {
		signal = local.SetSignal
	}
	m := &Mesh{transits: newTransitCache(), routes: map[string]map[string]routeHint{}, relaySlots: make(chan struct{}, 16), Store: local, cfg: cfg, client: newClient(cfg), nodes: map[string]nodeState{}, nonces: map[string]time.Time{}, inbound: make(chan struct{}, 64), slots: make(chan struct{}, 16), lookupSlots: make(chan struct{}, 8), wake: make(chan struct{}, 1), done: make(chan struct{}), signal: signal}
	if cfg.StateFile != "" {
		data, err := os.ReadFile(cfg.StateFile)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("mesh state: %w", err)
		}
		if err == nil {
			var saved []string
			if err := json.Unmarshal(data, &saved); err != nil {
				return nil, fmt.Errorf("mesh state: %w", err)
			}
			// Seeds take precedence when loading a full saved address book.
			m.remember(cfg.Seeds)
			m.remember(saved)
		}
	}
	m.remember(cfg.Seeds)
	return m, nil
}

func (m *Mesh) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/mesh/hello", m.serve(m.hello))
	mux.HandleFunc("POST /api/v1/mesh/rpc", m.serve(m.rpc))
	mux.HandleFunc("POST "+relayPath, m.serve(m.relay))
}

// Start is called once by the server after handlers have been wired.
func (m *Mesh) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(time.Duration(m.cfg.SyncIntervalSeconds) * time.Second)
		defer ticker.Stop()
		m.Sync(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.Sync(ctx)
			case <-m.wake:
				m.Sync(ctx)
			}
		}
	}()
}
func (m *Mesh) Close() {
	if m.cancel != nil {
		m.cancel()
		<-m.done
	}
	m.client.CloseIdleConnections()
}

// Nodes contains only servers that completed a protocol/authentication handshake.
func (m *Mesh) Nodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []string{}
	for url, state := range m.nodes {
		if state.Verified {
			out = append(out, url)
		}
	}
	sort.Strings(out)
	return out
}
func (m *Mesh) learn(addresses []string) {
	changed := false
	m.mu.Lock()
	for _, raw := range addresses {
		if len(m.nodes) >= m.cfg.MaxNodes {
			break
		}
		addr, err := normalizeURL(raw, m.cfg.AllowPrivateNetworks)
		if err != nil || addr == m.cfg.PublicURL {
			continue
		}
		if _, exists := m.nodes[addr]; exists {
			continue
		}
		m.nodes[addr] = nodeState{}
		changed = true
	}
	m.mu.Unlock()
	if changed {
		select {
		case m.wake <- struct{}{}:
		default:
		}
	}
}
func (m *Mesh) remember(addresses []string) {
	m.learn(addresses)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, raw := range addresses {
		addr, err := normalizeURL(raw, m.cfg.AllowPrivateNetworks)
		if err != nil {
			continue
		}
		if state, ok := m.nodes[addr]; ok {
			state.Pinned = true
			m.nodes[addr] = state
		}
	}
}

func (m *Mesh) hello(_ context.Context, body []byte) any {
	var h hello
	if json.Unmarshal(body, &h) != nil || h.Version != 1 {
		return rpcResponse{Error: "invalid"}
	}
	// An advertised address is a candidate only. It is dialed and verified by Sync.
	m.learn([]string{h.URL})
	return hello{Version: 1, URL: m.cfg.PublicURL, Nodes: m.Nodes(), Relay: true, Routes: m.advertisedRoutes()}
}

// Sync performs one bounded discovery round, including retry of unavailable nodes.
func (m *Mesh) Sync(ctx context.Context) {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()
	m.mu.RLock()
	pending := []string{}
	for addr, state := range m.nodes {
		if !time.Now().Before(state.NextAttempt) {
			pending = append(pending, addr)
		}
	}
	m.mu.RUnlock()
	sort.Strings(pending)
	jobs := make(chan string)
	var workers sync.WaitGroup
	for i := 0; i < min(4, len(pending)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for addr := range jobs {
				m.syncNode(ctx, addr)
			}
		}()
	}
queue:
	for _, addr := range pending {
		select {
		case jobs <- addr:
		case <-ctx.Done():
			break queue
		}
	}
	close(jobs)
	workers.Wait()
	if ctx.Err() != nil {
		return
	}
	if err := m.save(); err != nil {
		log.Printf("mesh state: %v", err)
	}
}

func (m *Mesh) syncNode(ctx context.Context, addr string) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.transitTimeout())
	defer cancel()
	var h hello
	probe, stop := m.attemptContext(ctx, true)
	err := m.wireExchange(probe, addr, helloPath, hello{Version: 1, URL: m.cfg.PublicURL}, &h)
	stop()
	direct := err == nil
	if err != nil {
		// The failed direct probe already consumed its budget. A known transit-only
		// node must not be probed a second time inside exchange.
		m.mu.Lock()
		state := m.nodes[addr]
		state.Direct = false
		m.nodes[addr] = state
		m.mu.Unlock()
		if len(m.routePaths(addr)) > 0 {
			err = m.exchangeIndirect(ctx, addr, helloPath, hello{Version: 1, URL: m.cfg.PublicURL}, &h)
		}
	}
	if ctx.Err() != nil {
		return
	}
	if err == nil && (h.Version != 1 || h.URL != addr) {
		err = fmt.Errorf("mesh identity mismatch")
	}
	m.mu.Lock()
	state := m.nodes[addr]
	first := !state.Verified
	state.Direct = direct && err == nil
	if err == nil {
		state.Verified = true
		state.Relay = h.Relay
		state.Failures = 0
		state.NextAttempt = time.Time{}
	} else {
		state.Failures++
		state.NextAttempt = time.Now().Add(time.Duration(1<<min(state.Failures, 6)) * time.Second)
		if !state.Verified && !state.Pinned && state.Failures >= 6 {
			delete(m.nodes, addr)
			delete(m.routes, addr)
			m.mu.Unlock()
			return
		}
	}
	m.nodes[addr] = state
	m.mu.Unlock()
	if err == nil {
		if first {
			log.Printf("mesh: connected to %s (direct=%t)", addr, direct)
		}
		m.learn(h.Nodes)
		if direct && h.Relay {
			m.learnRoutes(addr, h.Routes)
		}
	}
}

func (m *Mesh) save() error {
	if m.cfg.StateFile == "" {
		return nil
	}
	m.mu.RLock()
	addresses := []string{}
	for addr, state := range m.nodes {
		if state.Verified || state.Pinned {
			addresses = append(addresses, addr)
		}
	}
	m.mu.RUnlock()
	sort.Strings(addresses)
	data, err := json.MarshalIndent(addresses, "", "  ")
	if err != nil {
		return err
	}
	if previous, err := os.ReadFile(m.cfg.StateFile); err == nil && bytes.Equal(previous, data) {
		return nil
	}
	f, err := os.CreateTemp(filepath.Dir(m.cfg.StateFile), ".mesh-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), m.cfg.StateFile)
}
func (m *Mesh) localContext(ctx context.Context) context.Context {
	return store.WithPeerResolver(ctx, signingResolver{m})
}

func (m *Mesh) Upsert(ctx context.Context, req model.RegisterRequest) error {
	if len(req.MuninnServers) > 16 {
		return store.ErrInvalidPeer
	}
	if err := m.Store.Upsert(ctx, req); err != nil {
		return err
	}
	m.learn(req.MuninnServers)
	return nil
}
func (m *Mesh) SetChunkHash(ctx context.Context, id string, index int, req model.RegisterChunkRequest) error {
	return m.Store.SetChunkHash(m.localContext(ctx), id, index, req)
}

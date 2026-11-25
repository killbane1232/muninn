package mesh

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"
)

const (
	relayPath    = "/api/v1/mesh/relay"
	helloPath    = "/api/v1/mesh/hello"
	rpcPath      = "/api/v1/mesh/rpc"
	maxRelayHops = 8
)

type routeHint struct {
	path    []string // first hop through destination, excluding this server
	expires time.Time
}

type relayRequest struct {
	ID       string          `json:"id"`
	Target   string          `json:"target"`
	Path     string          `json:"path"`
	Body     json.RawMessage `json:"body"`
	Route    []string        `json:"route"` // origin, intermediates, destination
	Position int             `json:"position"`
}
type relayResponse struct {
	Target string          `json:"target,omitempty"`
	Value  json.RawMessage `json:"value,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// exchange retains a single ID across the direct attempt and every fallback.
// Every receiving instance caches it, including intermediates and destination.
func (m *Mesh) exchange(ctx context.Context, node, path string, in, out any) error {
	return m.exchangeRouted(ctx, node, path, in, out, true)
}
func (m *Mesh) exchangeIndirect(ctx context.Context, node, path string, in, out any) error {
	return m.exchangeRouted(ctx, node, path, in, out, false)
}
func (m *Mesh) exchangeRouted(ctx context.Context, node, path string, in, out any, tryDirect bool) error {
	if path != helloPath && path != rpcPath {
		return fmt.Errorf("unsupported mesh route")
	}
	canonical, err := normalizeURL(node, m.cfg.AllowPrivateNetworks)
	if err != nil || canonical != node || node == m.cfg.PublicURL {
		return fmt.Errorf("invalid mesh target")
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.transitTimeout())
	defer cancel()
	m.mu.RLock()
	state := m.nodes[node]
	m.mu.RUnlock()
	// Compatibility with direct-only v1 nodes. They are never advertised as relays.
	if state.Verified && state.Direct && !state.Relay {
		return m.wireExchange(ctx, node, path, in, out)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	req := relayRequest{ID: hex.EncodeToString(id[:]), Target: node, Path: path, Body: body, Position: 1}
	paths := m.routePaths(node)
	// Only Sync probes a known indirect node directly again; avoid paying a failed
	// TCP timeout on every client request while its direct route is unavailable.
	if tryDirect && (!state.Verified || state.Direct) {
		paths = append([][]string{{node}}, paths...)
	}
	for i, path := range paths {
		if ctx.Err() != nil {
			break
		}
		req.Route = append([]string{m.cfg.PublicURL}, path...)
		attemptCtx, stop := m.attemptContext(ctx, i < len(paths)-1)
		var response relayResponse
		err := m.wireExchange(attemptCtx, path[0], relayPath, req, &response)
		stop()
		if err != nil || response.Error != "" {
			continue
		}
		if response.Target != node || len(response.Value) == 0 {
			continue
		}
		return json.Unmarshal(response.Value, out)
	}
	return fmt.Errorf("no mesh route to %s", node)
}

func (m *Mesh) attemptContext(ctx context.Context, reserveFallback bool) (context.Context, context.CancelFunc) {
	budget := m.cfg.transitTimeout()
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}
	if reserveFallback {
		budget = min(m.cfg.timeout(), budget/2)
	}
	return context.WithTimeout(ctx, budget)
}

func (m *Mesh) relay(ctx context.Context, body []byte) any {
	var req relayRequest
	if json.Unmarshal(body, &req) != nil || !m.validRelay(req) {
		return relayResponse{Error: "invalid"}
	}
	return m.transits.do(ctx, req, func() relayResponse {
		if req.Target == m.cfg.PublicURL {
			var result any
			switch req.Path {
			case helloPath:
				result = m.hello(ctx, req.Body)
			case rpcPath:
				result = m.rpc(ctx, req.Body)
			}
			data, err := json.Marshal(result)
			if err != nil {
				return relayResponse{Error: "unavailable"}
			}
			return relayResponse{Target: m.cfg.PublicURL, Value: data}
		}
		next := req.Route[req.Position+1]
		m.mu.RLock()
		peer := m.nodes[next]
		m.mu.RUnlock()
		// Never dial an arbitrary URL on a caller's behalf. Each edge must have
		// completed its own direct handshake, including mode/authentication checks.
		if !peer.Verified || !peer.Direct || !peer.Relay {
			return relayResponse{Error: "unavailable"}
		}
		req.Position++
		var response relayResponse
		if err := m.wireExchange(context.WithValue(ctx, forwardContextKey{}, true), next, relayPath, req, &response); err != nil {
			return relayResponse{Error: "unavailable"}
		}
		return response
	})
}

func (m *Mesh) validRelay(req relayRequest) bool {
	id, err := hex.DecodeString(req.ID)
	if err != nil || len(id) != 16 || req.ID != hex.EncodeToString(id) || (req.Path != helloPath && req.Path != rpcPath) || !json.Valid(req.Body) {
		return false
	}
	if len(req.Route) < 2 || len(req.Route) > m.cfg.MaxRelayHops+2 || len(req.Route) > maxRelayHops+2 || req.Position < 1 || req.Position >= len(req.Route) {
		return false
	}
	if req.Route[req.Position] != m.cfg.PublicURL || req.Route[len(req.Route)-1] != req.Target {
		return false
	}
	seen := make(map[string]bool, len(req.Route))
	for _, node := range req.Route {
		canonical, err := normalizeURL(node, m.cfg.AllowPrivateNetworks)
		if err != nil || canonical != node || seen[node] {
			return false
		}
		seen[node] = true
	}
	return true
}

func (m *Mesh) routePaths(target string) [][]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	paths := [][]string{}
	for via, hint := range m.routes[target] {
		peer := m.nodes[via]
		if peer.Verified && peer.Direct && peer.Relay && time.Now().Before(hint.expires) && len(hint.path)-1 <= m.cfg.MaxRelayHops {
			paths = append(paths, append([]string(nil), hint.path...))
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		if len(paths[i]) != len(paths[j]) {
			return len(paths[i]) < len(paths[j])
		}
		return slices.Compare(paths[i], paths[j]) < 0
	})
	return paths
}

// Advertise complete acyclic paths, rather than just reachability. This prevents
// two partitioned neighbors from advertising a route back through each other.
func (m *Mesh) advertisedRoutes() map[string][]string {
	routes := map[string][]string{}
	for _, node := range m.Nodes() {
		m.mu.RLock()
		peer := m.nodes[node]
		m.mu.RUnlock()
		if peer.Direct && peer.Relay {
			routes[node] = []string{m.cfg.PublicURL, node}
			continue
		}
		if paths := m.routePaths(node); len(paths) > 0 {
			routes[node] = append([]string{m.cfg.PublicURL}, paths[0]...)
		}
	}
	return routes
}

func (m *Mesh) learnRoutes(via string, advertised map[string][]string) {
	changed := false
	m.mu.Lock()
	// A fresh advertisement replaces the previous snapshot from this neighbor.
	old := map[string]routeHint{}
	for target, paths := range m.routes {
		if hint, ok := paths[via]; ok {
			old[target] = hint
			delete(paths, via)
		}
		if len(paths) == 0 {
			delete(m.routes, target)
		}
	}
	for target, path := range advertised {
		if _, known := m.nodes[target]; !known || target == m.cfg.PublicURL || len(path) < 2 || len(path) > m.cfg.MaxRelayHops+1 || path[0] != via || path[len(path)-1] != target {
			continue
		}
		valid := true
		seen := map[string]bool{m.cfg.PublicURL: true}
		for _, addr := range path {
			canonical, err := normalizeURL(addr, m.cfg.AllowPrivateNetworks)
			if err != nil || canonical != addr || seen[addr] {
				valid = false
				break
			}
			seen[addr] = true
		}
		if !valid {
			continue
		}
		if m.routes[target] == nil {
			m.routes[target] = map[string]routeHint{}
		}
		m.routes[target][via] = routeHint{path: append([]string(nil), path...), expires: time.Now().Add(max(30*time.Second, 3*time.Duration(m.cfg.SyncIntervalSeconds)*time.Second))}
		if previous, ok := old[target]; !ok || !slices.Equal(previous.path, path) {
			changed = true
			state := m.nodes[target]
			state.NextAttempt = time.Time{}
			m.nodes[target] = state
		}
	}
	m.mu.Unlock()
	if changed {
		select {
		case m.wake <- struct{}{}:
		default:
		}
	}
}

package mesh

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"sync"
	"time"
)

const (
	transitTTL        = time.Minute
	maxTransitEntries = 4096
	maxTransitBytes   = 32 << 20
)

type transitEntry struct {
	fingerprint [32]byte
	route       []string
	done        chan struct{}
	completed   bool
	expires     time.Time
	response    relayResponse
}

type transitCache struct {
	mu      sync.Mutex
	entries map[string]*transitEntry
	bytes   int
	now     func() time.Time
}

func newTransitCache() *transitCache {
	return &transitCache{entries: make(map[string]*transitEntry), now: time.Now}
}

// Payload identity excludes the route: retries over another path must share the
// result. Keep the ID even if the bounded response cache cannot retain its body.
func (c *transitCache) do(ctx context.Context, req relayRequest, process func() relayResponse) relayResponse {
	identity, _ := json.Marshal(struct {
		Origin string
		Target string
		Path   string
		Body   json.RawMessage
	}{req.Route[0], req.Target, req.Path, req.Body})
	fingerprint := sha256.Sum256(identity)
	c.mu.Lock()
	now := c.now()
	for id, entry := range c.entries {
		if entry.completed && !now.Before(entry.expires) {
			c.bytes -= len(entry.response.Value)
			delete(c.entries, id)
		}
	}
	if previous, ok := c.entries[req.ID]; ok {
		if previous.fingerprint != fingerprint {
			c.mu.Unlock()
			return relayResponse{Error: "invalid"}
		}
		if previous.completed {
			response := previous.response
			c.mu.Unlock()
			return response
		}
		// Different in-flight paths can cross. Do not make two relays wait for each
		// other; identical concurrent retries and duplicates at the destination wait.
		if req.Position < len(req.Route)-1 && !slices.Equal(req.Route, previous.route) {
			c.mu.Unlock()
			return relayResponse{Error: "in_progress"}
		}
		done := previous.done
		c.mu.Unlock()
		select {
		case <-done:
			return previous.response
		case <-ctx.Done():
			return relayResponse{Error: "unavailable"}
		}
	}
	if len(c.entries) >= maxTransitEntries {
		c.mu.Unlock()
		return relayResponse{Error: "busy"}
	}
	entry := &transitEntry{fingerprint: fingerprint, route: append([]string(nil), req.Route...), done: make(chan struct{})}
	c.entries[req.ID] = entry
	c.mu.Unlock()
	response := process()
	c.mu.Lock()
	entry.response = response
	if c.bytes+len(response.Value) > maxTransitBytes {
		entry.response = relayResponse{Error: "processed"}
	}
	c.bytes += len(entry.response.Value)
	entry.completed = true
	entry.expires = c.now().Add(transitTTL)
	close(entry.done)
	c.mu.Unlock()
	return response
}

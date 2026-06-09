package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/killbane1232/muninn/internal/model"
)

func TestMemoryStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{ID: "alice", Username: "alice", Addresses: []string{"192.168.1.10:9000"}})
	if err != nil {
		t.Fatal(err)
	}

	peer, err := s.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if peer.ID != "alice" {
		t.Fatalf("got id %q", peer.ID)
	}
	if peer.Username != "alice" {
		t.Fatalf("got username %q", peer.Username)
	}

	peers, err := s.List(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("list: %v len=%d", err, len(peers))
	}

	if err := s.Delete(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "alice"); err != ErrNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestMemoryStoreGetByUsername(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{ID: "id-1", Username: "alice", Addresses: []string{"10.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}

	peer, err := s.GetByUsername(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if peer.ID != "id-1" {
		t.Fatalf("got id %q", peer.ID)
	}
	if peer.Username != "alice" {
		t.Fatalf("got username %q", peer.Username)
	}

	if _, err := s.GetByUsername(ctx, "nonexistent"); err != ErrNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestMemoryStoreUsernameGeneratedFromSignatureKey(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID:           "node-1",
		Addresses:    []string{"10.0.0.1:1"},
		SignatureKey: "MCowBQYDK2VwAyEAlvQf3R6k7RwnlM5a4K7BcX9fGzJ0YQdN1sP2R3A4B5Y=",
	})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := s.Get(ctx, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if peer.Username == "" {
		t.Fatal("username should be generated from signature_key")
	}

	got, err := s.GetByUsername(ctx, peer.Username)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "node-1" {
		t.Fatalf("got id %q", got.ID)
	}
}

func TestMemoryStoreUsernameGeneratedIsDeterministic(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID:           "node-1",
		Addresses:    []string{"10.0.0.1:1"},
		SignatureKey: "MCowBQYDK2VwAyEAlvQf3R6k7RwnlM5a4K7BcX9fGzJ0YQdN1sP2R3A4B5Y=",
	})
	if err != nil {
		t.Fatal(err)
	}
	peer1, _ := s.Get(ctx, "node-1")

	s2 := NewMemory()
	_, err = s2.Upsert(ctx, model.RegisterRequest{
		ID:           "node-2",
		Addresses:    []string{"10.0.0.2:1"},
		SignatureKey: "MCowBQYDK2VwAyEAlvQf3R6k7RwnlM5a4K7BcX9fGzJ0YQdN1sP2R3A4B5Y=",
	})
	if err != nil {
		t.Fatal(err)
	}
	peer2, _ := s2.Get(ctx, "node-2")

	if peer1.Username != peer2.Username {
		t.Fatalf("same key should produce same username: %q vs %q", peer1.Username, peer2.Username)
	}
}

func TestMemoryStoreUsernameGeneratedFallsBackToPublicKey(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID:        "node-1",
		Addresses: []string{"10.0.0.1:1"},
		PublicKey: "some-legacy-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := s.Get(ctx, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if peer.Username == "" {
		t.Fatal("username should be generated from public_key")
	}
}

func TestMemoryStoreUsernameGeneratedNoKeys(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID:        "node-1",
		Addresses: []string{"10.0.0.1:1"},
	})
	if err != ErrInvalidPeer {
		t.Fatalf("expected ErrInvalidPeer, got %v", err)
	}
}

func TestMemoryStoreUsernameUnique(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{ID: "id-1", Username: "alice", Addresses: []string{"10.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Upsert(ctx, model.RegisterRequest{ID: "id-2", Username: "alice", Addresses: []string{"10.0.0.2:1"}})
	if err != ErrUsernameTaken {
		t.Fatalf("expected ErrUsernameTaken, got %v", err)
	}

	_, err = s.Upsert(ctx, model.RegisterRequest{ID: "id-1", Username: "alice", Addresses: []string{"10.0.0.1:2"}})
	if err != nil {
		t.Fatalf("same peer re-upsert should succeed: %v", err)
	}
}

func TestMemoryStoreInitialQualityScore(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	peer, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "new-peer", Username: "new-peer", Addresses: []string{"10.0.0.1:1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if peer.QualityScore != InitialQualityScore {
		t.Fatalf("got quality_score %d, want %d", peer.QualityScore, InitialQualityScore)
	}

	peer2, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "new-peer", Username: "new-peer", Addresses: []string{"10.0.0.1:2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if peer2.QualityScore != InitialQualityScore {
		t.Fatalf("re-upsert should preserve quality_score, got %d", peer2.QualityScore)
	}
}

func TestMemoryStoreGetBestPeers(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("peer-%d", i)
		_, err := s.Upsert(ctx, model.RegisterRequest{
			ID: id, Username: id, Addresses: []string{"10.0.0.1:1"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	all, _ := s.GetBestPeers(ctx, 10)
	if len(all) != 5 {
		t.Fatalf("got %d peers, want 5", len(all))
	}

	top3, _ := s.GetBestPeers(ctx, 3)
	if len(top3) != 3 {
		t.Fatalf("got %d peers, want 3", len(top3))
	}

	zero, _ := s.GetBestPeers(ctx, 0)
	if len(zero) != 0 {
		t.Fatalf("expected empty, got %d", len(zero))
	}

	neg, _ := s.GetBestPeers(ctx, -1)
	if len(neg) != 0 {
		t.Fatalf("expected empty for negative n, got %d", len(neg))
	}
}

func TestMemoryStoreGetBestPeersOrdered(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("peer-%d", i)
		_, err := s.Upsert(ctx, model.RegisterRequest{
			ID: id, Username: id, Addresses: []string{"10.0.0.1:1"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	s.mu.Lock()
	p1 := s.peers["peer-0"]
	p1.QualityScore = 3000
	s.peers["peer-0"] = p1
	p2 := s.peers["peer-1"]
	p2.QualityScore = 1000
	s.peers["peer-1"] = p2
	p3 := s.peers["peer-2"]
	p3.QualityScore = 2000
	s.peers["peer-2"] = p3
	s.mu.Unlock()

	top, _ := s.GetBestPeers(ctx, 3)
	if len(top) != 3 {
		t.Fatalf("got %d peers", len(top))
	}
	if top[0].ID != "peer-0" || top[1].ID != "peer-2" || top[2].ID != "peer-1" {
		t.Fatalf("wrong order: %s %s %s", top[0].ID, top[1].ID, top[2].ID)
	}
}

func TestMemoryStoreExpiry(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID:         "bob",
		Username:   "bob",
		Addresses:  []string{"10.0.0.1:1"},
		TTLSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1100 * time.Millisecond)
	if n := s.PurgeExpired(ctx); n != 1 {
		t.Fatalf("purged %d, want 1", n)
	}
	if _, err := s.Get(ctx, "bob"); err != ErrNotFound {
		t.Fatalf("expected not found after expiry, got %v", err)
	}
}

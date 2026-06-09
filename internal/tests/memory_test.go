package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/store"
)

func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "alice", Addresses: []string{"192.168.1.10:9000"},
		Keys: []model.Key{{Login: "login-alice", Signature: "sig-alice"}},
	})
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
	if len(peer.Keys) != 1 {
		t.Fatalf("got %d keys, want 1", len(peer.Keys))
	}

	peers, err := s.List(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("list: %v len=%d", err, len(peers))
	}

	if err := s.Delete(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "alice"); err != store.ErrNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestGetByKey(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{{Login: "login-1", Signature: "sig-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	peer, err := s.GetByKey(ctx, "login-1", "sig-1")
	if err != nil {
		t.Fatal(err)
	}
	if peer.ID != "node-1" {
		t.Fatalf("got id %q", peer.ID)
	}

	if _, err := s.GetByKey(ctx, "login-1", "wrong-sig"); err != store.ErrNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := s.GetByKey(ctx, "unknown", "sig-1"); err != store.ErrNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestGetByKeyEmptyLogin(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	if _, err := s.GetByKey(ctx, "", "sig"); err != store.ErrInvalidKey {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
	if _, err := s.GetByKey(ctx, "login", ""); err != store.ErrInvalidKey {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestKeyUniqueAcrossPeers(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{{Login: "same-login", Signature: "same-sig"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "node-2", Addresses: []string{"10.0.0.2:1"},
		Keys: []model.Key{{Login: "same-login", Signature: "same-sig"}},
	})
	if err != store.ErrKeyTaken {
		t.Fatalf("expected ErrKeyTaken, got %v", err)
	}
}

func TestKeyReUpsertSamePeer(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{{Login: "login-1", Signature: "sig-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:2"},
		Keys: []model.Key{{Login: "login-1", Signature: "sig-1"}},
	})
	if err != nil {
		t.Fatalf("same peer re-upsert with same key should succeed: %v", err)
	}

	peer, err := s.Get(ctx, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(peer.Addresses) != 1 || peer.Addresses[0] != "10.0.0.1:2" {
		t.Fatalf("addresses not updated: %v", peer.Addresses)
	}
}

func TestMultipleKeysPerPeer(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{
			{Login: "login-A", Signature: "sig-A"},
			{Login: "login-B", Signature: "sig-B"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	peer, err := s.GetByKey(ctx, "login-A", "sig-A")
	if err != nil {
		t.Fatal(err)
	}
	if peer.ID != "node-1" {
		t.Fatalf("got id %q", peer.ID)
	}

	peer, err = s.GetByKey(ctx, "login-B", "sig-B")
	if err != nil {
		t.Fatal(err)
	}
	if peer.ID != "node-1" {
		t.Fatalf("got id %q", peer.ID)
	}
}

func TestKeysReplacedOnReUpsert(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{{Login: "old-login", Signature: "old-sig"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{{Login: "new-login", Signature: "new-sig"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.GetByKey(ctx, "old-login", "old-sig"); err != store.ErrNotFound {
		t.Fatalf("old key should be gone, got %v", err)
	}

	if _, err := s.GetByKey(ctx, "new-login", "new-sig"); err != nil {
		t.Fatalf("new key should resolve: %v", err)
	}
}

func TestKeyCollisionDifferentSignatures(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{{Login: "same-login", Signature: "sig-A"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "node-2", Addresses: []string{"10.0.0.2:1"},
		Keys: []model.Key{{Login: "same-login", Signature: "sig-B"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	peer, err := s.GetByKey(ctx, "same-login", "sig-A")
	if err != nil {
		t.Fatal(err)
	}
	if peer.ID != "node-1" {
		t.Fatalf("got %q, want node-1", peer.ID)
	}

	peer, err = s.GetByKey(ctx, "same-login", "sig-B")
	if err != nil {
		t.Fatal(err)
	}
	if peer.ID != "node-2" {
		t.Fatalf("got %q, want node-2", peer.ID)
	}
}

func TestRegisterWithoutKeys(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
	})
	if err != store.ErrInvalidPeer {
		t.Fatalf("expected ErrInvalidPeer, got %v", err)
	}
}

func TestRegisterWithEmptyKeys(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{},
	})
	if err != store.ErrInvalidPeer {
		t.Fatalf("expected ErrInvalidPeer, got %v", err)
	}
}

func TestRegisterWithBlankKeyFields(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{{Login: "", Signature: ""}},
	})
	if err != store.ErrInvalidKey {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestInitialQualityScore(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	peer, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "new-peer", Addresses: []string{"10.0.0.1:1"},
		Keys: []model.Key{{Login: "login", Signature: "sig"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if peer.QualityScore != store.InitialQualityScore {
		t.Fatalf("got %d, want %d", peer.QualityScore, store.InitialQualityScore)
	}

	peer2, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "new-peer", Addresses: []string{"10.0.0.1:2"},
		Keys: []model.Key{{Login: "login", Signature: "sig"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if peer2.QualityScore != store.InitialQualityScore {
		t.Fatalf("re-upsert should preserve score, got %d", peer2.QualityScore)
	}
}

func TestGetBestPeersCount(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("peer-%d", i)
		_, err := s.Upsert(ctx, model.RegisterRequest{
			ID: id, Addresses: []string{"10.0.0.1:1"},
			Keys: []model.Key{{Login: fmt.Sprintf("login-%d", i), Signature: fmt.Sprintf("sig-%d", i)}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	all, _ := s.GetBestPeers(ctx, 10)
	if len(all) != 5 {
		t.Fatalf("got %d, want 5", len(all))
	}

	top3, _ := s.GetBestPeers(ctx, 3)
	if len(top3) != 3 {
		t.Fatalf("got %d, want 3", len(top3))
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

func TestGetBestPeersOrdered(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("peer-%d", i)
		_, err := s.Upsert(ctx, model.RegisterRequest{
			ID: id, Addresses: []string{"10.0.0.1:1"},
			Keys: []model.Key{{Login: fmt.Sprintf("login-%d", i), Signature: fmt.Sprintf("sig-%d", i)}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := s.SetPeerScore("peer-0", 3000); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPeerScore("peer-1", 1000); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPeerScore("peer-2", 2000); err != nil {
		t.Fatal(err)
	}

	top, _ := s.GetBestPeers(ctx, 3)
	if len(top) != 3 {
		t.Fatalf("got %d, want 3", len(top))
	}
	if top[0].ID != "peer-0" || top[1].ID != "peer-2" || top[2].ID != "peer-1" {
		t.Fatalf("wrong order: %s %s %s", top[0].ID, top[1].ID, top[2].ID)
	}
}

func TestExpiry(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "bob", Addresses: []string{"10.0.0.1:1"},
		Keys:       []model.Key{{Login: "login-bob", Signature: "sig-bob"}},
		TTLSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1100 * time.Millisecond)
	if n := s.PurgeExpired(ctx); n != 1 {
		t.Fatalf("purged %d, want 1", n)
	}
	if _, err := s.Get(ctx, "bob"); err != store.ErrNotFound {
		t.Fatalf("expected not found after expiry, got %v", err)
	}
}

func TestExpiredPeerKeysCleaned(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "bob", Addresses: []string{"10.0.0.1:1"},
		Keys:       []model.Key{{Login: "login-bob", Signature: "sig-bob"}},
		TTLSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1100 * time.Millisecond)
	s.PurgeExpired(ctx)

	if _, err := s.GetByKey(ctx, "login-bob", "sig-bob"); err != store.ErrNotFound {
		t.Fatalf("expected not found for expired peer's key, got %v", err)
	}
}

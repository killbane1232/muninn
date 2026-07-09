package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/store"
)

func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	err := s.Upsert(ctx, model.RegisterRequest{
		ID: "alice", Login: "login-alice", SignatureKey: "sig-alice",
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

	err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "login-1", SignatureKey: "sig-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	peers, err := s.GetByKey(ctx, "login-1", "sig-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].ID != "node-1" {
		t.Fatalf("got %+v", peers)
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

	err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "same-login", SignatureKey: "same-sig",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Upsert(ctx, model.RegisterRequest{
		ID: "node-2", Login: "same-login", SignatureKey: "same-sig",
	})
	if err != nil {
		t.Fatalf("expected ok, got %v", err)
	}

	peers, err := s.GetByKey(ctx, "same-login", "same-sig")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}
}

func TestKeyReUpsertSamePeer(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "login-1", SignatureKey: "sig-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "login-1", SignatureKey: "sig-1",
	})
	if err != nil {
		t.Fatalf("same peer re-upsert with same key should succeed: %v", err)
	}

	_, err = s.Get(ctx, "node-1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultipleKeysPerPeer(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "login-A", SignatureKey: "sig-A",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "login-B", SignatureKey: "sig-B",
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.GetByKey(ctx, "login-A", "sig-A"); err != store.ErrNotFound {
		t.Fatal("got old key")
	}

	if _, err := s.GetByKey(ctx, "login-B", "sig-B"); err != store.ErrNotFound {
		t.Fatal("got new sign")
	}
	peers, err := s.GetByKey(ctx, "login-B", "sig-A")
	if err != nil {
		t.Fatal("new key should resolve")
	}
	if len(peers) != 1 || peers[0].ID != "node-1" {
		t.Fatalf("got %+v", peers)
	}
}

func TestKeyCollisionDifferentSignatures(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	err := s.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "same-login", SignatureKey: "sig-A",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Upsert(ctx, model.RegisterRequest{
		ID: "node-2", Login: "same-login", SignatureKey: "sig-B",
	})
	if err != nil {
		t.Fatal(err)
	}

	peers, err := s.GetByKey(ctx, "same-login", "sig-A")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].ID != "node-1" {
		t.Fatalf("got %+v", peers)
	}

	peers, err = s.GetByKey(ctx, "same-login", "sig-B")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].ID != "node-2" {
		t.Fatalf("got %+v", peers)
	}
}

func TestGetBestPeersCount(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("peer-%d", i)
		err := s.Upsert(ctx, model.RegisterRequest{
			ID: id, Login: fmt.Sprintf("login-%d", i), SignatureKey: fmt.Sprintf("sig-%d", i),
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
		err := s.Upsert(ctx, model.RegisterRequest{
			ID: id, Login: fmt.Sprintf("login-%d", i), SignatureKey: fmt.Sprintf("sig-%d", i),
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

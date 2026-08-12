package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/killbane1232/muninn/internal/model"
	_ "modernc.org/sqlite"
)

func newTestSQLiteStore(t *testing.T) *dbStore {
	t.Helper()
	st, err := NewDB(driverSQLite, "file:"+filepath.Join(t.TempDir(), "muninn.db"))
	if err != nil {
		t.Fatal(err)
	}
	db := st.(*dbStore)
	t.Cleanup(func() { _ = db.db.Close() })
	return db
}

func TestDBRefreshRequiresExistingMatchingPeer(t *testing.T) {
	ctx := context.Background()
	st := newTestSQLiteStore(t)
	req := model.RefreshRequest{ID: "node-1", Login: "login-1", SignatureKey: "sig-1"}

	if err := st.Refresh(ctx, req); !errors.Is(err, ErrNotFound) {
		t.Fatalf("refresh missing peer: got %v, want %v", err, ErrNotFound)
	}
	if err := st.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "login-1", SignatureKey: "sig-1", TTLSeconds: 120,
	}); err != nil {
		t.Fatal(err)
	}
	wrongKey := req
	wrongKey.SignatureKey = "wrong-signature"
	if err := st.Refresh(ctx, wrongKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("refresh wrong identity: got %v, want %v", err, ErrNotFound)
	}
	if err := st.Refresh(ctx, req); err != nil {
		t.Fatalf("refresh existing peer: %v", err)
	}
}

func TestDBUpsertUpdatesPeerIdentity(t *testing.T) {
	ctx := context.Background()
	st := newTestSQLiteStore(t)

	if err := st.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "old-login", EncryptionKey: "old-enc", SignatureKey: "old-sig",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Upsert(ctx, model.RegisterRequest{
		ID: "node-1", Login: "new-login", EncryptionKey: "new-enc", SignatureKey: "new-sig",
	}); err != nil {
		t.Fatal(err)
	}

	peer, err := st.Get(ctx, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if peer.Login != "new-login" || peer.EncryptionKey != "new-enc" || peer.SignatureKey != "new-sig" {
		t.Fatalf("identity was not updated: %+v", peer)
	}
	if _, err := st.GetByKey(ctx, "old-login", "old-sig"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old key still resolves: %v", err)
	}
	peers, err := st.GetByKey(ctx, "new-login", "new-sig")
	if err != nil || len(peers) != 1 || peers[0].ID != "node-1" {
		t.Fatalf("new key lookup: peers=%+v err=%v", peers, err)
	}
}

func TestSQLiteLoginMigrationBackfillsLegacyPeerKey(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaVersionTable("sqlite")); err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.ID == "009" {
			break
		}
		if _, err := db.Exec(migration.SQLite); err != nil {
			t.Fatalf("apply legacy migration %s: %v", migration.ID, err)
		}
		if _, err := db.Exec(
			`INSERT INTO schema_version (id, author, description, applied_at, checksum) VALUES ($1, $2, $3, 0, $4)`,
			migration.ID, migration.Author, migration.Description, checksum(migration),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO peers (id, key, encryption_key, signature_key, last_seen, ttl_seconds, peer_flag) VALUES ($1, $2, $3, $4, unixepoch(), 120, 'thin')`,
		"node-1", "legacy-login:legacy-sig", "enc", "legacy-sig",
	); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := NewDB(driverSQLite, dsn)
	if err != nil {
		t.Fatal(err)
	}
	dbStore := st.(*dbStore)
	t.Cleanup(func() { _ = dbStore.db.Close() })
	peers, err := dbStore.GetByKey(context.Background(), "legacy-login", "legacy-sig")
	if err != nil || len(peers) != 1 || peers[0].ID != "node-1" {
		t.Fatalf("migrated key lookup: peers=%+v err=%v", peers, err)
	}
}

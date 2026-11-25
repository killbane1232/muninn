package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/killbane1232/muninn/internal/model"
)

func newTestPostgresStore(t *testing.T) *dbStore {
	t.Helper()
	dsn := os.Getenv("MUNINN_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set MUNINN_TEST_POSTGRES_DSN to run PostgreSQL store tests")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = admin.Close() })
	schema := pgx.Identifier{fmt.Sprintf("muninn_test_%d", time.Now().UnixNano())}.Sanitize()
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
			t.Errorf("clean test schema: %v", err)
		}
	})
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = db.Close() })
	if err := runMigrations(db, "pgx"); err != nil {
		t.Fatal(err)
	}
	return &dbStore{db: db, driver: driverPostgres}
}

func TestDBUpsertFakeFlag(t *testing.T) {
	for name, open := range map[string]func(*testing.T) *dbStore{
		"sqlite":   newTestSQLiteStore,
		"postgres": newTestPostgresStore,
	} {
		t.Run(name, func(t *testing.T) {
			st := open(t)
			ctx := context.Background()
			fake, real := true, false
			for _, initial := range []struct {
				name string
				flag *bool
			}{
				{"omitted", nil}, {"false", &real}, {"true", &fake},
			} {
				t.Run(initial.name, func(t *testing.T) {
					want := initial.flag != nil && *initial.flag
					for _, flag := range []*bool{initial.flag, nil, &fake, nil, &real, nil} {
						if flag != nil {
							want = *flag
						}
						if err := st.Upsert(ctx, model.RegisterRequest{
							ID: initial.name, Login: "alice", SignatureKey: "sig", Fake: flag,
						}); err != nil {
							t.Fatal(err)
						}
						peer, err := st.Get(ctx, initial.name)
						if err != nil || peer.Fake != want {
							t.Fatalf("read fake flag: got %v, want %v, err=%v", peer.Fake, want, err)
						}
						var stored int
						if err := st.db.QueryRowContext(ctx, "SELECT is_fake FROM peers WHERE id = $1", initial.name).Scan(&stored); err != nil {
							t.Fatal(err)
						}
						if (want && stored != 1) || (!want && stored != 0) {
							t.Fatalf("stored fake flag = %d, want boolean %v as 0/1", stored, want)
						}
						peers, err := st.List(ctx)
						if err != nil {
							t.Fatal(err)
						}
						visible := false
						for _, p := range peers {
							visible = visible || p.ID == initial.name
						}
						if visible == want {
							t.Fatalf("fake=%v peer visibility=%v", want, visible)
						}
					}
				})
			}
		})
	}
}

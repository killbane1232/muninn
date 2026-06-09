package store_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
	"github.com/killbane1232/muninn/internal/store"
)

type testKeys struct {
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
	b64  string
}

func newTestKeys(t *testing.T) testKeys {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return testKeys{pub: pub, priv: priv, b64: sign.PublicKeyBase64(pub)}
}

func TestChunkQualityScoring(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	senderKeys := newTestKeys(t)
	receiverKeys := newTestKeys(t)

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"10.0.0.1:9000"},
		Keys:         []model.Key{{Login: "login-sender", Signature: "sig-sender"}},
		SignatureKey: senderKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "seeder-1", Addresses: []string{"10.0.0.2:9000"},
		Keys:         []model.Key{{Login: "login-seeder", Signature: "sig-seeder"}},
		SignatureKey: receiverKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}

	const fileID = "file-abc"
	const hash = "deadbeef"

	expectedMsg := sign.ExpectedPayload(fileID, 0, hash)
	if err := s.SetChunkHash(ctx, fileID, 0, model.RegisterChunkRequest{
		SenderID: "sender", Hash: "DEADbeef",
		Signature: sign.Sign(senderKeys.priv, expectedMsg),
	}); err != nil {
		t.Fatal(err)
	}

	reportedMsg := sign.ReportedPayload(fileID, 0, hash, "seeder-1")
	ok, err := s.ReportChunk(ctx, "seeder-1", model.ChunkReportRequest{
		ReporterID: "seeder-1", FileID: fileID, ChunkIndex: 0, Hash: hash,
		Signature: sign.Sign(receiverKeys.priv, reportedMsg),
	})
	if err != nil || !ok.Valid || ok.Delta != store.QualityPointsValid {
		t.Fatalf("valid report: %+v err=%v", ok, err)
	}
	wantAfterValid := store.InitialQualityScore + store.QualityPointsValid
	if ok.Peer.QualityScore != wantAfterValid || ok.Peer.Quality.ValidReports != 1 {
		t.Fatalf("peer after valid: %+v", ok.Peer)
	}

	badMsg := sign.ReportedPayload(fileID, 0, "badhash", "seeder-1")
	bad, err := s.ReportChunk(ctx, "seeder-1", model.ChunkReportRequest{
		ReporterID: "seeder-1", FileID: fileID, ChunkIndex: 0, Hash: "badhash",
		Signature: sign.Sign(receiverKeys.priv, badMsg),
	})
	if err != nil || bad.Valid || bad.Delta != store.QualityPointsInvalid {
		t.Fatalf("invalid report: %+v err=%v", bad, err)
	}
	wantAfterInvalid := store.InitialQualityScore
	if bad.Peer.QualityScore != wantAfterInvalid || bad.Peer.Quality.InvalidReports != 1 {
		t.Fatalf("peer after invalid: %+v", bad.Peer)
	}

	peer, _ := s.Get(ctx, "seeder-1")
	if peer.QualityScore != wantAfterInvalid {
		t.Fatalf("persisted score %d, want %d", peer.QualityScore, wantAfterInvalid)
	}
}

func TestReportChunkUnknown(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()
	receiverKeys := newTestKeys(t)

	_, _ = s.Upsert(ctx, model.RegisterRequest{
		ID: "p", Addresses: []string{"1:1"},
		Keys:         []model.Key{{Login: "login-p", Signature: "sig-p"}},
		SignatureKey: receiverKeys.b64,
	})

	msg := sign.ReportedPayload("f", 0, "aa", "p")
	_, err := s.ReportChunk(ctx, "p", model.ChunkReportRequest{
		ReporterID: "p", FileID: "f", ChunkIndex: 0, Hash: "aa",
		Signature: sign.Sign(receiverKeys.priv, msg),
	})
	if err != store.ErrChunkNotFound {
		t.Fatalf("got %v", err)
	}
}

func TestChunkInvalidSignature(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()
	keys := newTestKeys(t)

	_, _ = s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"1:1"},
		Keys:         []model.Key{{Login: "login-sender", Signature: "sig-sender"}},
		SignatureKey: keys.b64,
	})

	err := s.SetChunkHash(ctx, "f", 0, model.RegisterChunkRequest{
		SenderID: "sender", Hash: "aa", Signature: "invalid",
	})
	if err != store.ErrInvalidSignature {
		t.Fatalf("got %v", err)
	}
}

package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
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
	s := NewMemory()

	senderKeys := newTestKeys(t)
	receiverKeys := newTestKeys(t)

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID:           "sender",
		Username:     "sender",
		Addresses:    []string{"10.0.0.1:9000"},
		SignatureKey: senderKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID:           "seeder-1",
		Username:     "seeder-1",
		Addresses:    []string{"10.0.0.2:9000"},
		SignatureKey: receiverKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}

	const fileID = "file-abc"
	const hash = "deadbeef"

	expectedMsg := sign.ExpectedPayload(fileID, 0, hash)
	if err := s.SetChunkHash(ctx, fileID, 0, model.RegisterChunkRequest{
		SenderID:  "sender",
		Hash:      "DEADbeef",
		Signature: sign.Sign(senderKeys.priv, expectedMsg),
	}); err != nil {
		t.Fatal(err)
	}

	reportedMsg := sign.ReportedPayload(fileID, 0, hash, "seeder-1")
	ok, err := s.ReportChunk(ctx, "seeder-1", model.ChunkReportRequest{
		ReporterID: "seeder-1",
		FileID:     fileID,
		ChunkIndex: 0,
		Hash:       hash,
		Signature:  sign.Sign(receiverKeys.priv, reportedMsg),
	})
	if err != nil || !ok.Valid || ok.Delta != QualityPointsValid {
		t.Fatalf("valid report: %+v err=%v", ok, err)
	}
	wantAfterValid := InitialQualityScore + QualityPointsValid
	if ok.Peer.QualityScore != wantAfterValid || ok.Peer.Quality.ValidReports != 1 {
		t.Fatalf("peer after valid: %+v", ok.Peer)
	}

	badMsg := sign.ReportedPayload(fileID, 0, "badhash", "seeder-1")
	bad, err := s.ReportChunk(ctx, "seeder-1", model.ChunkReportRequest{
		ReporterID: "seeder-1",
		FileID:     fileID,
		ChunkIndex: 0,
		Hash:       "badhash",
		Signature:  sign.Sign(receiverKeys.priv, badMsg),
	})
	if err != nil || bad.Valid || bad.Delta != QualityPointsInvalid {
		t.Fatalf("invalid report: %+v err=%v", bad, err)
	}
	wantAfterInvalid := InitialQualityScore
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
	s := NewMemory()
	receiverKeys := newTestKeys(t)

	_, _ = s.Upsert(ctx, model.RegisterRequest{
		ID: "p", Username: "p", Addresses: []string{"1:1"}, SignatureKey: receiverKeys.b64,
	})

	msg := sign.ReportedPayload("f", 0, "aa", "p")
	_, err := s.ReportChunk(ctx, "p", model.ChunkReportRequest{
		ReporterID: "p",
		FileID:     "f",
		ChunkIndex: 0,
		Hash:       "aa",
		Signature:  sign.Sign(receiverKeys.priv, msg),
	})
	if err != ErrChunkNotFound {
		t.Fatalf("got %v", err)
	}
}

func TestChunkInvalidSignature(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()
	keys := newTestKeys(t)

	_, _ = s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Username: "sender", Addresses: []string{"1:1"}, SignatureKey: keys.b64,
	})

	err := s.SetChunkHash(ctx, "f", 0, model.RegisterChunkRequest{
		SenderID: "sender", Hash: "aa", Signature: "invalid",
	})
	if err != ErrInvalidSignature {
		t.Fatalf("got %v", err)
	}
}

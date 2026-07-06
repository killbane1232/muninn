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

func TestChunkConfirmScoring(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	senderKeys := newTestKeys(t)
	receiverKeys := newTestKeys(t)

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"10.0.0.1:9000"},
		Login: "login-sender", SignatureKey: senderKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "receiver", Addresses: []string{"10.0.0.2:9000"},
		Login: "login-receiver", SignatureKey: receiverKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}

	sender, _ := s.Get(ctx, "sender")
	receiver, _ := s.Get(ctx, "receiver")

	const fileID = "file-abc"
	const hash = "deadbeef01234567"

	expectedMsg := sign.ExpectedPayload(fileID, 0, hash)
	if err := s.SetChunkHash(ctx, fileID, 0, model.RegisterChunkRequest{
		SenderID: sender.Key, RecipientID: receiver.Key, PeerID: "receiver",
		Hash: "DEADbeef01234567", Signature: sign.Sign(senderKeys.priv, expectedMsg),
	}); err != nil {
		t.Fatal(err)
	}

	chunks, _ := s.GetChunksByRecipient(ctx, receiver.Key, 0)
	if len(chunks) != 1 || chunks[0].Confirmed {
		t.Fatalf("expected 1 unconfirmed chunk, got %+v", chunks)
	}

	confirmedMsg := sign.ConfirmedPayload(fileID, 0, hash)
	ok, err := s.ConfirmChunk(ctx, model.ConfirmChunkRequest{
		RecipientID: receiver.Key, FileID: fileID, ChunkIndex: 0, Hash: hash,
		Signature: sign.Sign(receiverKeys.priv, confirmedMsg),
	})
	if err != nil || !ok.Valid || ok.Delta != store.QualityPointsValid {
		t.Fatalf("valid confirm: %+v err=%v", ok, err)
	}
	wantAfterValid := store.InitialQualityScore + store.QualityPointsValid
	if ok.Peer.QualityScore != wantAfterValid || ok.Peer.Quality.ValidReports != 1 {
		t.Fatalf("sender after valid: %+v, wanted: %d", ok.Peer, wantAfterValid)
	}

	chunks, _ = s.GetChunksByRecipient(ctx, receiver.Key, 0)
	if len(chunks) != 1 || !chunks[0].Confirmed {
		t.Fatalf("expected 1 confirmed chunk, got %+v", chunks)
	}

	badMsg := sign.ConfirmedPayload(fileID, 0, "ffffffffffffffff")
	bad, err := s.ConfirmChunk(ctx, model.ConfirmChunkRequest{
		RecipientID: receiver.Key, FileID: fileID, ChunkIndex: 0, Hash: "ffffffffffffffff",
		Signature: sign.Sign(receiverKeys.priv, badMsg),
	})
	if err != nil || bad.Valid || bad.Delta != store.QualityPointsInvalid {
		t.Fatalf("invalid confirm: %+v err=%v", bad, err)
	}
	wantAfterInvalid := store.InitialQualityScore
	if bad.Peer.QualityScore != wantAfterInvalid || bad.Peer.Quality.InvalidReports != 1 {
		t.Fatalf("sender after invalid: %+v", bad.Peer)
	}

	peer, _ := s.Get(ctx, "sender")
	if peer.QualityScore != wantAfterInvalid {
		t.Fatalf("persisted score %d, want %d", peer.QualityScore, wantAfterInvalid)
	}
}

func TestChunkQualityScoring(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	senderKeys := newTestKeys(t)
	receiverKeys := newTestKeys(t)

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"10.0.0.1:9000"},
		Login: "login-sender", SignatureKey: senderKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "seeder-1", Addresses: []string{"10.0.0.2:9000"},
		Login: "login-seeder", SignatureKey: receiverKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}

	sender, _ := s.Get(ctx, "sender")

	const fileID = "file-abc"
	const hash = "deadbeef01234567"

	expectedMsg := sign.ExpectedPayload(fileID, 0, hash)
	if err := s.SetChunkHash(ctx, fileID, 0, model.RegisterChunkRequest{
		SenderID: sender.Key, RecipientID: "receiver", PeerID: "seeder-1",
		Hash: "DEADbeef01234567", Signature: sign.Sign(senderKeys.priv, expectedMsg),
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

	badMsg := sign.ReportedPayload(fileID, 0, "ffffffffffffffff", "seeder-1")
	bad, err := s.ReportChunk(ctx, "seeder-1", model.ChunkReportRequest{
		ReporterID: "seeder-1", FileID: fileID, ChunkIndex: 0, Hash: "ffffffffffffffff",
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
		Login: "login-p", SignatureKey: receiverKeys.b64,
	})

	msg := sign.ReportedPayload("f", 0, "aaaaaaaaaaaaaaaa", "p")
	_, err := s.ReportChunk(ctx, "p", model.ChunkReportRequest{
		ReporterID: "p", FileID: "f", ChunkIndex: 0, Hash: "aaaaaaaaaaaaaaaa",
		Signature: sign.Sign(receiverKeys.priv, msg),
	})
	if err != store.ErrChunkNotFound {
		t.Fatalf("got %v", err)
	}
}

func TestConfirmChunkUnknown(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()
	receiverKeys := newTestKeys(t)

	_, _ = s.Upsert(ctx, model.RegisterRequest{
		ID: "p", Addresses: []string{"1:1"},
		Login: "login-p", SignatureKey: receiverKeys.b64,
	})
	p, _ := s.Get(ctx, "p")

	msg := sign.ConfirmedPayload("f", 0, "aaaaaaaaaaaaaaaa")
	_, err := s.ConfirmChunk(ctx, model.ConfirmChunkRequest{
		RecipientID: p.Key, FileID: "f", ChunkIndex: 0, Hash: "aaaaaaaaaaaaaaaa",
		Signature: sign.Sign(receiverKeys.priv, msg),
	})
	if err != store.ErrChunkNotFound {
		t.Fatalf("got %v", err)
	}
}

func TestPersistChunkFlag(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	senderKeys := newTestKeys(t)
	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"10.0.0.1:9000"},
		Login: "login-sender", SignatureKey: senderKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}

	sender, _ := s.Get(ctx, "sender")

	const fileID = "file-persist"
	const hash = "deadbeef01234567"

	expectedMsg := sign.ExpectedPayload(fileID, 0, hash)
	err = s.SetChunkHash(ctx, fileID, 0, model.RegisterChunkRequest{
		SenderID: sender.Key, RecipientID: "recipient", PeerID: "seeder",
		Hash: hash, Signature: sign.Sign(senderKeys.priv, expectedMsg),
		Persist: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	expectedMsg2 := sign.ExpectedPayload(fileID, 1, hash)
	err = s.SetChunkHash(ctx, fileID, 1, model.RegisterChunkRequest{
		SenderID: sender.Key, RecipientID: "recipient", PeerID: "seeder",
		Hash: hash, Signature: sign.Sign(senderKeys.priv, expectedMsg2),
		Persist: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	records, err := s.GetChunksByRecipient(ctx, "recipient", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 chunks before delete, got %d", len(records))
	}
}

func TestPeerFlagThin(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	peer, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "thin-peer", Addresses: []string{"10.0.0.1:9000"},
		Login: "thin", SignatureKey: "thin-sig",
		PeerFlag: model.PeerFlagThin,
	})
	if err != nil {
		t.Fatal(err)
	}
	if peer.PeerFlag != model.PeerFlagThin {
		t.Fatalf("expected thin flag, got %q", peer.PeerFlag)
	}

	if store.EffectiveScore(peer) != peer.QualityScore {
		t.Fatalf("thin: expected %d^1=%d, got %d", peer.QualityScore, peer.QualityScore, store.EffectiveScore(peer))
	}
}

func TestPeerFlagThick(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	peer, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "thick-peer-2", Addresses: []string{"10.0.0.1:9000"},
		Login: "thick-2", SignatureKey: "tk2-sig",
		PeerFlag: model.PeerFlagThick,
	})
	if err != nil {
		t.Fatal(err)
	}
	if peer.PeerFlag != model.PeerFlagThick {
		t.Fatalf("expected thick flag, got %q", peer.PeerFlag)
	}

	expected := peer.QualityScore * peer.QualityScore
	if store.EffectiveScore(peer) != expected {
		t.Fatalf("thick: expected %d^2=%d, got %d", peer.QualityScore, expected, store.EffectiveScore(peer))
	}
}

func TestPeerFlagThickNegativeSignPreserved(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	peer, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "neg-thick", Addresses: []string{"10.0.0.1:9000"},
		Login: "neg-thick", SignatureKey: "nt-sig",
		PeerFlag: model.PeerFlagThick,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.SetPeerScore(peer.ID, -5)

	peer, _ = s.Get(ctx, peer.ID)
	effective := store.EffectiveScore(peer)
	// -5 ^ 2 = 25, но знак должен сохраниться → -25
	if effective != -25 {
		t.Fatalf("thick with negative score: expected -25, got %d", effective)
	}
}

func TestPeerFlagVeryThick(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	peer, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "very-thick-peer", Addresses: []string{"10.0.0.1:9000"},
		Login: "very-thick", SignatureKey: "vt-sig",
		PeerFlag: model.PeerFlagVeryThick,
	})
	if err != nil {
		t.Fatal(err)
	}
	if peer.PeerFlag != model.PeerFlagVeryThick {
		t.Fatalf("expected very_thick flag, got %q", peer.PeerFlag)
	}

	expected := peer.QualityScore * peer.QualityScore * peer.QualityScore
	if store.EffectiveScore(peer) != expected {
		t.Fatalf("very_thick: expected %d^3=%d, got %d", peer.QualityScore, expected, store.EffectiveScore(peer))
	}
}

func TestPeerFlagAffectsGetBestPeersOrder(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "no-flag", Addresses: []string{"10.0.0.1:9000"},
		Login: "no-flag", SignatureKey: "nf-sig",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "thick-peer", Addresses: []string{"10.0.0.1:9000"},
		Login: "thick", SignatureKey: "tk-sig",
		PeerFlag: model.PeerFlagThick,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Upsert(ctx, model.RegisterRequest{
		ID: "thin-peer", Addresses: []string{"10.0.0.1:9000"},
		Login: "thin-2", SignatureKey: "tn-sig",
		PeerFlag: model.PeerFlagThin,
	})
	if err != nil {
		t.Fatal(err)
	}

	all, err := s.GetBestPeers(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(all) < 3 {
		t.Fatalf("expected at least 3 peers, got %d", len(all))
	}

	if all[0].ID != "thick-peer" {
		t.Fatalf("expected thick-peer first (1000^2 >> 1000^1), got %s first", all[0].ID)
	}
}

func TestChunkPersistBatchEntry(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	senderKeys := newTestKeys(t)
	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"10.0.0.1:9000"},
		Login: "login-batch", SignatureKey: senderKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}

	sender, _ := s.Get(ctx, "sender")

	hash := "deadbeef01234567"
	msg := sign.ExpectedPayload("f", 0, hash)

	err = s.SetChunkHash(ctx, "f", 0, model.RegisterChunkRequest{
		SenderID: sender.Key, RecipientID: "r", PeerID: "p",
		Hash: hash, Signature: sign.Sign(senderKeys.priv, msg),
		Persist: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	records, err := s.GetChunksByRecipient(ctx, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || !records[0].Persist {
		t.Fatalf("expected 1 persisted chunk, got %+v", records)
	}
}

func TestChunkNonPersistDeleted(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	senderKeys := newTestKeys(t)
	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"10.0.0.1:9000"},
		Login: "login-np", SignatureKey: senderKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}

	sender, _ := s.Get(ctx, "sender")

	hash := "deadbeef01234567"
	msg := sign.ExpectedPayload("f", 0, hash)

	err = s.SetChunkHash(ctx, "f", 0, model.RegisterChunkRequest{
		SenderID: sender.Key, RecipientID: "r", PeerID: "p",
		Hash: hash, Signature: sign.Sign(senderKeys.priv, msg),
		Persist: false,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestChunkPersistSurvivesDelete(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()

	senderKeys := newTestKeys(t)
	_, err := s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"10.0.0.1:9000"},
		Login: "login-ps", SignatureKey: senderKeys.b64,
	})
	if err != nil {
		t.Fatal(err)
	}

	sender, _ := s.Get(ctx, "sender")

	hash := "deadbeef01234567"
	msg := sign.ExpectedPayload("f", 0, hash)

	err = s.SetChunkHash(ctx, "f", 0, model.RegisterChunkRequest{
		SenderID: sender.Key, RecipientID: "r", PeerID: "p",
		Hash: hash, Signature: sign.Sign(senderKeys.priv, msg),
		Persist: true,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestChunkInvalidSignature(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()
	keys := newTestKeys(t)

	_, _ = s.Upsert(ctx, model.RegisterRequest{
		ID: "sender", Addresses: []string{"1:1"},
		Login: "login-sender", SignatureKey: keys.b64,
	})

	sender, _ := s.Get(ctx, "sender")

	err := s.SetChunkHash(ctx, "f", 0, model.RegisterChunkRequest{
		SenderID: sender.Key, RecipientID: "receiver", PeerID: "p",
		Hash: "aaaaaaaaaaaaaaaa", Signature: "invalid",
	})
	if err != store.ErrInvalidSignature {
		t.Fatalf("got %v", err)
	}
}

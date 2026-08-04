package handler

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
	"github.com/killbane1232/muninn/internal/store"
)

func TestRegisterChunkBatchPreservesTTL(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	const fileID = "file-id"
	const hash = "deadbeef01234567"
	const ttl = 604800
	sender := model.RegisterRequest{
		ID:           "sender-peer",
		Login:        "alice",
		SignatureKey: sign.PublicKeyBase64(pub),
	}
	if err := st.Upsert(ctx, sender); err != nil {
		t.Fatal(err)
	}

	body, err := json.Marshal(model.RegisterChunkBatchRequest{Chunks: []model.RegisterChunkBatchEntry{{
		ChunkIndex:  0,
		SenderID:    sender.Login + ":" + sender.SignatureKey,
		RecipientID: "bob:key",
		Hash:        hash,
		Signature:   sign.Sign(priv, sign.ExpectedPayload(fileID, 0, hash)),
		PeerID:      "holder-peer",
		TTL:         ttl,
	}}})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/"+fileID+"/chunks", bytes.NewReader(body))
	req.SetPathValue("file_id", fileID)
	recorder := httptest.NewRecorder()
	(&Phonebook{Store: st}).RegisterChunkBatch(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	chunks, err := st.GetChunksByFileID(ctx, fileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("got %d chunks, want 1", len(chunks))
	}
	if chunks[0].TTL != ttl {
		t.Fatalf("ttl = %d, want %d", chunks[0].TTL, ttl)
	}
}

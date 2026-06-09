package store_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/killbane1232/muninn/internal/sign"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	msg := sign.ExpectedPayload("file-1", 0, "abc123")
	sig := sign.Sign(priv, msg)

	if err := sign.Verify(sign.PublicKeyBase64(pub), sig, msg); err != nil {
		t.Fatal(err)
	}
	if err := sign.Verify(sign.PublicKeyBase64(pub), sig, []byte("tampered")); err != sign.ErrInvalidSignature {
		t.Fatalf("got %v", err)
	}
}

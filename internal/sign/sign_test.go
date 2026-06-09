package sign

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	msg := ExpectedPayload("file-1", 0, "abc123")
	sig := Sign(priv, msg)

	if err := Verify(PublicKeyBase64(pub), sig, msg); err != nil {
		t.Fatal(err)
	}
	if err := Verify(PublicKeyBase64(pub), sig, []byte("tampered")); err != ErrInvalidSignature {
		t.Fatalf("got %v", err)
	}
}

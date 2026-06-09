package sign

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidKey       = errors.New("invalid signing key")
	ErrInvalidSignature = errors.New("invalid signature")
)

// ExpectedPayload — сообщение для подписи отправителя (эталон чанка).
func ExpectedPayload(fileID string, chunkIndex int, hash string) []byte {
	return []byte(fmt.Sprintf("muninn/expected/v1\n%s\n%d\n%s", fileID, chunkIndex, hash))
}

// ReportedPayload — сообщение для подписи получателя (отчёт о чанке).
func ReportedPayload(fileID string, chunkIndex int, hash, sourcePeerID string) []byte {
	return []byte(fmt.Sprintf("muninn/reported/v1\n%s\n%d\n%s\n%s", fileID, chunkIndex, hash, sourcePeerID))
}

// Verify проверяет Ed25519-подпись по публичному ключу (Base64, 32 байта).
func Verify(publicKeyB64, signatureB64 string, message []byte) error {
	pub, err := decodePublicKey(publicKeyB64)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(signatureB64))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return ErrInvalidSignature
	}
	if !ed25519.Verify(pub, message, sig) {
		return ErrInvalidSignature
	}
	return nil
}

// Sign создаёт подпись (Base64) для тестов и клиентских SDK.
func Sign(privateKey ed25519.PrivateKey, message []byte) string {
	return base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, message))
}

// PublicKeyBase64 кодирует публичный ключ Ed25519 в Base64.
func PublicKeyBase64(pub ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(pub)
}

func decodePublicKey(b64 string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if err != nil {
		return nil, ErrInvalidKey
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, ErrInvalidKey
	}
	return ed25519.PublicKey(raw), nil
}

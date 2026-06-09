package store

import (
	"errors"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
)

func verifyPeerSignature(peer model.Peer, signatureB64 string, message []byte) error {
	if peer.SignatureKey == "" {
		return ErrNoSigningKey
	}
	if err := sign.Verify(peer.SignatureKey, signatureB64, message); err != nil {
		if errors.Is(err, sign.ErrInvalidSignature) || errors.Is(err, sign.ErrInvalidKey) {
			return ErrInvalidSignature
		}
		return err
	}
	return nil
}

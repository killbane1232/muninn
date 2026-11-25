package store

import (
	"context"
	"errors"
	"strings"

	"github.com/killbane1232/muninn/internal/model"
)

// PeerResolver is used only to verify endpoint-based reports across servers.
// User keys already contain their signing public key and are self-certifying.
type PeerResolver interface {
	Get(context.Context, string) (model.Peer, error)
}
type resolverKey struct{}

func WithPeerResolver(ctx context.Context, resolver PeerResolver) context.Context {
	return context.WithValue(ctx, resolverKey{}, resolver)
}

func signingPeer(ctx context.Context, local Store, id string) (model.Peer, error) {
	p, err := local.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		if resolver, ok := ctx.Value(resolverKey{}).(PeerResolver); ok {
			return resolver.Get(ctx, id)
		}
	}
	return p, err
}
func signingUser(ctx context.Context, local Store, key string) (model.Peer, error) {
	login, signature, ok := strings.Cut(key, ":")
	if !ok || login == "" || signature == "" {
		return model.Peer{}, ErrInvalidKey
	}
	peers, err := local.GetByKey(ctx, login, signature)
	if err == nil && len(peers) > 0 {
		return peers[0], nil
	}
	if errors.Is(err, ErrNotFound) && ctx.Value(resolverKey{}) != nil {
		// The signature still has to verify against this exact public key. Do not
		// create a local directory record for a user registered on another server.
		return model.Peer{Login: login, SignatureKey: signature}, nil
	}
	return model.Peer{}, err
}

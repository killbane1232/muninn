package store

import (
	"context"
	"errors"

	"github.com/killbane1232/muninn/internal/model"
)

var (
	ErrNotFound           = errors.New("peer not found")
	ErrInvalidPeer        = errors.New("invalid peer data")
	ErrInvalidKey         = errors.New("invalid key")
	ErrKeyTaken           = errors.New("login+signature pair already belongs to another peer")
	ErrInvalidChunk       = errors.New("invalid chunk data")
	ErrChunkNotFound      = errors.New("chunk hash not registered")
	ErrInvalidSignature   = errors.New("invalid signature")
	ErrNoSigningKey       = errors.New("peer has no signature key")
	ErrNoPendingMessages  = errors.New("no pending messages for recipient")
)

const (
	InitialQualityScore  = 1000
	QualityPointsValid   = 1
	QualityPointsInvalid = -1
)

// EffectiveScore — quality_score, возведённый в степень согласно PeerFlag.
// Для чётных степеней знак исходного числа сохраняется.
func EffectiveScore(peer model.Peer) int {
	exp := peerFlagExponent(peer.PeerFlag)
	score := peer.QualityScore
	result := 1
	for i := 0; i < exp; i++ {
		result *= score
	}
	if exp%2 == 0 && score < 0 && result > 0 {
		result = -result
	}
	return result
}

func peerFlagExponent(flag model.PeerFlag) int {
	switch flag {
	case model.PeerFlagThin:
		return 1
	case model.PeerFlagThick:
		return 2
	case model.PeerFlagVeryThick:
		return 3
	default:
		return 1
	}
}

// Store — хранилище телефонной книги.
type Store interface {
	Upsert(ctx context.Context, req model.RegisterRequest) (model.Peer, error)
	Get(ctx context.Context, id string) (model.Peer, error)
	GetByKey(ctx context.Context, login string, signature string) (model.Peer, error)
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]model.Peer, error)
	Heartbeat(ctx context.Context, id string, ttlSeconds int) (model.Peer, error)
	SetChunkHash(ctx context.Context, fileID string, chunkIndex int, req model.RegisterChunkRequest) error
	GetBestPeers(ctx context.Context, n int) ([]model.Peer, error)
	ReportChunk(ctx context.Context, sourcePeerID string, req model.ChunkReportRequest) (model.ChunkReportResult, error)
	ConfirmChunk(ctx context.Context, req model.ConfirmChunkRequest) (model.ConfirmChunkResult, error)
	GetChunksByRecipient(ctx context.Context, recipientID string) ([]model.ChunkRecord, error)

	GetBestThickPeers(ctx context.Context, n int) ([]model.Peer, error)

	SetSignal(ctx context.Context, peerID string, sig model.Signal) error
	PollSignals(ctx context.Context, peerID string) ([]model.Signal, error)
}

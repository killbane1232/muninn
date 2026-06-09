package store

import (
	"context"
	"errors"

	"github.com/killbane1232/muninn/internal/model"
)

var (
	ErrNotFound        = errors.New("peer not found")
	ErrInvalidPeer     = errors.New("invalid peer data")
	ErrAlreadyExists   = errors.New("peer already exists")
	ErrUsernameTaken   = errors.New("username already taken")
	ErrInvalidChunk    = errors.New("invalid chunk data")
	ErrChunkNotFound     = errors.New("chunk hash not registered")
	ErrInvalidSignature  = errors.New("invalid signature")
	ErrNoSigningKey      = errors.New("peer has no signature key")
)

const (
	InitialQualityScore  = 1000
	QualityPointsValid   = 1
	QualityPointsInvalid = -1
)

// Store — хранилище телефонной книги.
type Store interface {
	Upsert(ctx context.Context, req model.RegisterRequest) (model.Peer, error)
	Get(ctx context.Context, id string) (model.Peer, error)
	GetByUsername(ctx context.Context, username string) (model.Peer, error)
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]model.Peer, error)
	Heartbeat(ctx context.Context, id string, ttlSeconds int) (model.Peer, error)
	PurgeExpired(ctx context.Context) int
	SetChunkHash(ctx context.Context, fileID string, chunkIndex int, req model.RegisterChunkRequest) error
	GetBestPeers(ctx context.Context, n int) ([]model.Peer, error)
	ReportChunk(ctx context.Context, sourcePeerID string, req model.ChunkReportRequest) (model.ChunkReportResult, error)
}

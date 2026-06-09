package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
)

const usernameKeyLen = 12

const defaultTTL = 300

// MemoryStore — потокобезопасное in-memory хранилище.
type MemoryStore struct {
	mu        sync.RWMutex
	peers     map[string]model.Peer
	usernames map[string]string // username -> peer ID
	chunks    map[string]string // fileID#index -> expected hash
}

func NewMemory() *MemoryStore {
	return &MemoryStore{
		peers:     make(map[string]model.Peer),
		usernames: make(map[string]string),
		chunks:    make(map[string]string),
	}
}

func (s *MemoryStore) Upsert(_ context.Context, req model.RegisterRequest) (model.Peer, error) {
	id := strings.TrimSpace(req.ID)
	if id == "" {
		return model.Peer{}, ErrInvalidPeer
	}
	if len(req.Addresses) == 0 {
		return model.Peer{}, ErrInvalidPeer
	}

	ttl := req.TTLSeconds
	if ttl <= 0 {
		ttl = defaultTTL
	}

	now := time.Now().UTC()
	peer := model.Peer{
		ID:            id,
		Addresses:     copyStrings(req.Addresses),
		PublicKey:     strings.TrimSpace(req.PublicKey),
		EncryptionKey: strings.TrimSpace(req.EncryptionKey),
		SignatureKey:  strings.TrimSpace(req.SignatureKey),
		Metadata:      copyMetadata(req.Metadata),
		LastSeen:      now,
		TTLSeconds:    ttl,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	username, err := s.resolveUsername(req)
	if err != nil {
		return model.Peer{}, err
	}
	peer.Username = username

	if existing, ok := s.peers[id]; ok {
		if existing.Username != username {
			delete(s.usernames, existing.Username)
		}
		peer.QualityScore = existing.QualityScore
		peer.Quality = existing.Quality
		if peer.EncryptionKey == "" {
			peer.EncryptionKey = existing.EncryptionKey
		}
		if peer.SignatureKey == "" {
			peer.SignatureKey = existing.SignatureKey
		}
	} else {
		if owner, taken := s.usernames[username]; taken && owner != id {
			return model.Peer{}, ErrUsernameTaken
		}
		peer.QualityScore = InitialQualityScore
	}

	s.peers[id] = peer
	s.usernames[username] = id
	return peer, nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (model.Peer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peer, ok := s.peers[id]
	if !ok || s.isExpired(peer, time.Now().UTC()) {
		return model.Peer{}, ErrNotFound
	}
	return peer, nil
}

func (s *MemoryStore) GetByUsername(_ context.Context, username string) (model.Peer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.usernames[username]
	if !ok {
		return model.Peer{}, ErrNotFound
	}
	peer, ok := s.peers[id]
	if !ok || s.isExpired(peer, time.Now().UTC()) {
		return model.Peer{}, ErrNotFound
	}
	return peer, nil
}

func (s *MemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	peer, ok := s.peers[id]
	if !ok {
		return ErrNotFound
	}
	delete(s.usernames, peer.Username)
	delete(s.peers, id)
	return nil
}

func (s *MemoryStore) List(_ context.Context) ([]model.Peer, error) {
	now := time.Now().UTC()
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]model.Peer, 0, len(s.peers))
	for _, peer := range s.peers {
		if !s.isExpired(peer, now) {
			out = append(out, peer)
		}
	}
	return out, nil
}

func (s *MemoryStore) Heartbeat(_ context.Context, id string, ttlSeconds int) (model.Peer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	peer, ok := s.peers[id]
	if !ok {
		return model.Peer{}, ErrNotFound
	}

	if ttlSeconds > 0 {
		peer.TTLSeconds = ttlSeconds
	}
	peer.LastSeen = time.Now().UTC()
	s.peers[id] = peer
	return peer, nil
}

func (s *MemoryStore) GetBestPeers(_ context.Context, n int) ([]model.Peer, error) {
	if n <= 0 {
		return []model.Peer{}, nil
	}

	now := time.Now().UTC()
	s.mu.RLock()
	defer s.mu.RUnlock()

	active := make([]model.Peer, 0, len(s.peers))
	for _, peer := range s.peers {
		if !s.isExpired(peer, now) {
			active = append(active, peer)
		}
	}

	sort.Slice(active, func(i, j int) bool {
		return active[i].QualityScore > active[j].QualityScore
	})

	if n > len(active) {
		n = len(active)
	}
	return active[:n], nil
}

func (s *MemoryStore) PurgeExpired(_ context.Context) int {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for id, peer := range s.peers {
		if s.isExpired(peer, now) {
			delete(s.peers, id)
			removed++
		}
	}
	return removed
}

func (s *MemoryStore) isExpired(peer model.Peer, now time.Time) bool {
	ttl := peer.TTLSeconds
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return now.Sub(peer.LastSeen) > time.Duration(ttl)*time.Second
}

func copyStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, a := range in {
		a = strings.TrimSpace(a)
		if a != "" {
			out = append(out, a)
		}
	}
	return out
}

func (s *MemoryStore) SetChunkHash(_ context.Context, fileID string, chunkIndex int, req model.RegisterChunkRequest) error {
	fileID = strings.TrimSpace(fileID)
	senderID := strings.TrimSpace(req.SenderID)
	hash := normalizeHash(req.Hash)
	if fileID == "" || senderID == "" || hash == "" || chunkIndex < 0 || strings.TrimSpace(req.Signature) == "" {
		return ErrInvalidChunk
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sender, ok := s.peers[senderID]
	if !ok {
		return ErrNotFound
	}
	msg := sign.ExpectedPayload(fileID, chunkIndex, hash)
	if err := verifyPeerSignature(sender, req.Signature, msg); err != nil {
		return err
	}

	s.chunks[chunkKey(fileID, chunkIndex)] = hash
	return nil
}

func (s *MemoryStore) ReportChunk(_ context.Context, sourcePeerID string, req model.ChunkReportRequest) (model.ChunkReportResult, error) {
	sourcePeerID = strings.TrimSpace(sourcePeerID)
	reporterID := strings.TrimSpace(req.ReporterID)
	fileID := strings.TrimSpace(req.FileID)
	reported := normalizeHash(req.Hash)

	if sourcePeerID == "" || reporterID == "" || fileID == "" || reported == "" || req.ChunkIndex < 0 ||
		strings.TrimSpace(req.Signature) == "" {
		return model.ChunkReportResult{}, ErrInvalidChunk
	}

	key := chunkKey(fileID, req.ChunkIndex)

	s.mu.Lock()
	defer s.mu.Unlock()

	reporter, ok := s.peers[reporterID]
	if !ok {
		return model.ChunkReportResult{}, ErrNotFound
	}
	msg := sign.ReportedPayload(fileID, req.ChunkIndex, reported, sourcePeerID)
	if err := verifyPeerSignature(reporter, req.Signature, msg); err != nil {
		return model.ChunkReportResult{}, err
	}

	expected, ok := s.chunks[key]
	if !ok {
		return model.ChunkReportResult{}, ErrChunkNotFound
	}

	peer, ok := s.peers[sourcePeerID]
	if !ok {
		return model.ChunkReportResult{}, ErrNotFound
	}

	valid := reported == expected
	delta := QualityPointsInvalid
	if valid {
		delta = QualityPointsValid
		peer.Quality.ValidReports++
	} else {
		peer.Quality.InvalidReports++
	}
	peer.QualityScore += delta
	s.peers[sourcePeerID] = peer

	return model.ChunkReportResult{
		Valid:        valid,
		ExpectedHash: expected,
		ReportedHash: reported,
		Delta:        delta,
		Peer:         peer,
	}, nil
}

func copyMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (s *MemoryStore) resolveUsername(req model.RegisterRequest) (string, error) {
	if u := strings.TrimSpace(req.Username); u != "" {
		return u, nil
	}

	key := strings.TrimSpace(req.SignatureKey)
	if key == "" {
		key = strings.TrimSpace(req.EncryptionKey)
	}
	if key == "" {
		key = strings.TrimSpace(req.PublicKey)
	}
	if key == "" {
		return "", ErrInvalidPeer
	}

	b64 := strings.TrimSpace(key)
	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		decoded = []byte(b64)
	}
	h := sha256.Sum256(decoded)
	username := fmt.Sprintf("%x", h[:usernameKeyLen/2])

	if _, taken := s.usernames[username]; taken {
		if _, exists := s.peers[req.ID]; !exists {
			return "", ErrUsernameTaken
		}
	}

	return username, nil
}

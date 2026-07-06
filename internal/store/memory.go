package store

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
)

const defaultTTL = 300

// MemoryStore — потокобезопасное in-memory хранилище.
type MemoryStore struct {
	mu                sync.RWMutex
	peers             map[string]model.Peer
	logins            map[string][]string
	chunks            []chunkRecordEntry
	signals           map[string][]model.Signal
}

type chunkRecordEntry struct {
	key         string
	Hash        string
	SenderID    string
	RecipientID string
	PeerID      string
	Persist     bool
	Confirmed   bool
	Readed      bool
	CreatedAt   int64
	UpdatedAt   int64
	TTL         int
}

func NewMemory() *MemoryStore {
	return &MemoryStore{
		peers:   make(map[string]model.Peer),
		logins:  make(map[string][]string),
		signals: make(map[string][]model.Signal),
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

	key := strings.TrimSpace(req.Login)+":"+strings.TrimSpace(req.SignatureKey)

	ttl := req.TTLSeconds
	if ttl <= 0 {
		ttl = defaultTTL
	}

	now := time.Now().UTC()
	peer := model.Peer{
		ID:            id,
		Key:           key,
		Addresses:     copyStrings(req.Addresses),
		EncryptionKey: strings.TrimSpace(req.EncryptionKey),
		SignatureKey:  strings.TrimSpace(req.SignatureKey),
		Metadata:      copyMetadata(req.Metadata),
		LastSeen:      now,
		TTLSeconds:    ttl,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.peers[id]; ok {
		peer.QualityScore = existing.QualityScore
		peer.Quality = existing.Quality
		peer.PeerFlag = existing.PeerFlag
		if req.PeerFlag != "" {
			peer.PeerFlag = req.PeerFlag
		}
		peer.Fake = existing.Fake
		peer.EncryptionKey = existing.EncryptionKey
		peer.SignatureKey = existing.SignatureKey
		s.logins[existing.Key] = removeID(s.logins[existing.Key], id)
	} else {
		peer.QualityScore = InitialQualityScore
		peer.PeerFlag = req.PeerFlag
		if req.Fake != nil {
			peer.Fake = *req.Fake
		}
	}

	s.logins[key] = append(s.logins[key], id)
	s.peers[id] = peer
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

func (s *MemoryStore) GetByKey(_ context.Context, login string, signature string) ([]model.Peer, error) {
	login = strings.TrimSpace(login)
	signature = strings.TrimSpace(signature)
	if login == "" || signature == "" {
		return nil, ErrInvalidKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	ids, ok := s.logins[login + ":" + signature]
	if !ok {
		return nil, ErrNotFound
	}
	now := time.Now().UTC()
	var out []model.Peer
	for _, id := range ids {
		peer, ok := s.peers[id]
		if !ok || s.isExpired(peer, now) {
			continue
		}
		out = append(out, peer)
	}
	if out == nil {
		return nil, ErrNotFound
	}
	return out, nil
}

func (s *MemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	peer, ok := s.peers[id]
	if !ok {
		return ErrNotFound
	}
	s.logins[peer.Key] = removeID(s.logins[peer.Key], id)
	delete(s.peers, id)
	return nil
}

func (s *MemoryStore) List(_ context.Context) ([]model.Peer, error) {
	now := time.Now().UTC()
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]model.Peer, 0, len(s.peers))
	for _, peer := range s.peers {
		if !s.isExpired(peer, now) && !peer.Fake {
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
		if !s.isExpired(peer, now) && !peer.Fake {
			active = append(active, peer)
		}
	}

	sort.Slice(active, func(i, j int) bool {
		return EffectiveScore(active[i]) > EffectiveScore(active[j])
	})

	if n > len(active) {
		n = len(active)
	}
	return active[:n], nil
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
	senderKey := strings.TrimSpace(req.SenderID)
	recipientKey := strings.TrimSpace(req.RecipientID)
	peerID := strings.TrimSpace(req.PeerID)
	hash := normalizeHash(req.Hash)
	if fileID == "" || senderKey == "" || recipientKey == "" || peerID == "" || hash == "" || !validHashFormat(hash) || chunkIndex < 0 || strings.TrimSpace(req.Signature) == "" {
		return ErrInvalidChunk
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sender, ok := s.peerByKey(senderKey)
	if !ok {
		return ErrNotFound
	}
	msg := sign.ExpectedPayload(fileID, chunkIndex, hash)
	if err := verifyPeerSignature(sender, req.Signature, msg); err != nil {
		return err
	}

	chunkKey := chunkKey(fileID, chunkIndex)
	chunkTTL := req.TTL
	if chunkTTL <= 0 {
		chunkTTL = defaultChunkTTL
	}
	now := time.Now().Unix()
	s.chunks = append(s.chunks, chunkRecordEntry{
		key:         chunkKey,
		Hash:        hash,
		SenderID:    senderKey,
		RecipientID: recipientKey,
		PeerID:      peerID,
		Persist:     req.Persist,
		Confirmed:   false,
		CreatedAt:   now,
		UpdatedAt:   now,
		TTL:         chunkTTL,
	})
	return nil
}

func (s *MemoryStore) ReportChunk(_ context.Context, sourcePeerID string, req model.ChunkReportRequest) (model.ChunkReportResult, error) {
	sourcePeerID = strings.TrimSpace(sourcePeerID)
	reporterID := strings.TrimSpace(req.ReporterID)
	fileID := strings.TrimSpace(req.FileID)
	reported := normalizeHash(req.Hash)

	if sourcePeerID == "" || reporterID == "" || fileID == "" || reported == "" || !validHashFormat(reported) || req.ChunkIndex < 0 ||
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

	var expected string
	for _, entry := range s.chunks {
		if entry.key == key {
			expected = entry.Hash
			break
		}
	}
	if expected == "" {
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

func (s *MemoryStore) ConfirmChunk(_ context.Context, req model.ConfirmChunkRequest) (model.ConfirmChunkResult, error) {
	recipientKey := strings.TrimSpace(req.RecipientID)
	fileID := strings.TrimSpace(req.FileID)
	confirmed := normalizeHash(req.Hash)

	if recipientKey == "" || fileID == "" || confirmed == "" || !validHashFormat(confirmed) || req.ChunkIndex < 0 ||
		strings.TrimSpace(req.Signature) == "" {
		return model.ConfirmChunkResult{}, ErrInvalidChunk
	}

	key := chunkKey(fileID, req.ChunkIndex)

	s.mu.Lock()
	defer s.mu.Unlock()

	recipient, ok := s.peerByKey(recipientKey)
	if !ok {
		return model.ConfirmChunkResult{}, ErrNotFound
	}
	msg := sign.ConfirmedPayload(fileID, req.ChunkIndex, confirmed)
	if err := verifyPeerSignature(recipient, req.Signature, msg); err != nil {
		return model.ConfirmChunkResult{}, err
	}

	var entry *chunkRecordEntry
	for i := range s.chunks {
		if s.chunks[i].key == key && s.chunks[i].RecipientID == recipientKey {
			entry = &s.chunks[i]
			break
		}
	}
	if entry == nil {
		return model.ConfirmChunkResult{}, ErrChunkNotFound
	}

	senderPeer, ok := s.peerByKey(entry.SenderID)
	if !ok {
		return model.ConfirmChunkResult{}, ErrNotFound
	}

	valid := confirmed == entry.Hash
	delta := QualityPointsInvalid
	if valid {
		delta = QualityPointsValid
		senderPeer.Quality.ValidReports++
		entry.Confirmed = true
		entry.UpdatedAt = time.Now().Unix()
	} else {
		senderPeer.Quality.InvalidReports++
	}
	senderPeer.QualityScore += delta
	s.peers[senderPeer.ID] = senderPeer

	return model.ConfirmChunkResult{
		Valid:         valid,
		ExpectedHash:  entry.Hash,
		ConfirmedHash: confirmed,
		Delta:         delta,
		Peer:          senderPeer,
	}, nil
}

func (s *MemoryStore) ReadChunk(_ context.Context, req model.ReadChunkRequest) (model.ReadChunkResult, error) {
	recipientKey := strings.TrimSpace(req.RecipientID)
	fileID := strings.TrimSpace(req.FileID)

	if fileID == "" || strings.TrimSpace(req.Signature) == "" {
		return model.ReadChunkResult{}, ErrInvalidChunk
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	recipient, ok := s.peerByKey(recipientKey)
	if !ok {
		return model.ReadChunkResult{}, ErrNotFound
	}

	msg := sign.ReadPayload(fileID)
	if err := verifyPeerSignature(recipient, req.Signature, msg); err != nil {
		return model.ReadChunkResult{}, err
	}

	marked := false
	now := time.Now().Unix()
	for i := range s.chunks {
		if s.chunks[i].RecipientID == recipientKey {
			fid, _ := parseChunkKey(s.chunks[i].key)
			if fid == fileID {
				s.chunks[i].Readed = true
				s.chunks[i].UpdatedAt = now
				marked = true
			}
		}
	}

	if !marked {
		return model.ReadChunkResult{}, ErrChunkNotFound
	}

	return model.ReadChunkResult{Valid: true}, nil
}

func (s *MemoryStore) GetChunksByRecipient(_ context.Context, recipientID string, dateFrom int64) ([]model.ChunkRecord, error) {
	recipientID = strings.TrimSpace(recipientID)
	if recipientID == "" {
		return nil, ErrInvalidChunk
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var records []model.ChunkRecord
	for _, entry := range s.chunks {
		if entry.RecipientID != recipientID || entry.UpdatedAt < dateFrom {
			continue
		}
		fileID, idx := parseChunkKey(entry.key)
		records = append(records, model.ChunkRecord{
			FileID:      fileID,
			ChunkIndex:  idx,
			SenderID:    entry.SenderID,
			RecipientID: entry.RecipientID,
			Hash:        entry.Hash,
			PeerID:      entry.PeerID,
			Persist:     entry.Persist,
			Confirmed:   entry.Confirmed,
			Readed:      entry.Readed,
			CreatedAt:   entry.CreatedAt,
			UpdatedAt:   entry.UpdatedAt,
			TTL:         entry.TTL,
		})
	}
	if records == nil {
		records = []model.ChunkRecord{}
	}
	return records, nil
}

func (s *MemoryStore) GetChunksByFileID(_ context.Context, fileID string) ([]model.ChunkRecord, error) {
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return nil, ErrInvalidChunk
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var records []model.ChunkRecord
	for _, entry := range s.chunks {
		fid, idx := parseChunkKey(entry.key)
		if fid != fileID {
			continue
		}
		records = append(records, model.ChunkRecord{
			FileID:      fileID,
			ChunkIndex:  idx,
			SenderID:    entry.SenderID,
			RecipientID: entry.RecipientID,
			Hash:        entry.Hash,
			PeerID:      entry.PeerID,
			Persist:     entry.Persist,
			Confirmed:   entry.Confirmed,
			Readed:      entry.Readed,
			CreatedAt:   entry.CreatedAt,
			UpdatedAt:   entry.UpdatedAt,
			TTL:         entry.TTL,
		})
	}
	if records == nil {
		records = []model.ChunkRecord{}
	}
	return records, nil
}

func parseChunkKey(key string) (string, int) {
	parts := strings.SplitN(key, "#", 2)
	if len(parts) != 2 {
		return "", -1
	}
	idx, _ := strconv.Atoi(parts[1])
	return parts[0], idx
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

func (s *MemoryStore) SetSignal(_ context.Context, peerID string, sig model.Signal) error {
	peerID = strings.TrimSpace(peerID)
	if peerID == "" || sig.From == "" || sig.Type == "" {
		return ErrInvalidChunk
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.peers[peerID]; !ok {
		return ErrNotFound
	}
	s.signals[peerID] = append(s.signals[peerID], sig)
	return nil
}

func (s *MemoryStore) PollSignals(_ context.Context, peerID string) ([]model.Signal, error) {
	peerID = strings.TrimSpace(peerID)
	if peerID == "" {
		return nil, ErrInvalidChunk
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sigs := s.signals[peerID]
	delete(s.signals, peerID)
	if sigs == nil {
		return []model.Signal{}, nil
	}
	return sigs, nil
}

func (s *MemoryStore) GetBestThickPeers(_ context.Context, n int) ([]model.Peer, error) {
	if n <= 0 {
		return []model.Peer{}, nil
	}

	now := time.Now().UTC()
	s.mu.RLock()
	defer s.mu.RUnlock()

	active := make([]model.Peer, 0, len(s.peers))
	for _, peer := range s.peers {
		if s.isExpired(peer, now) || peer.Fake {
			continue
		}
		if peer.PeerFlag == model.PeerFlagThick || peer.PeerFlag == model.PeerFlagVeryThick {
			active = append(active, peer)
		}
	}

	sort.Slice(active, func(i, j int) bool {
		return EffectiveScore(active[i]) > EffectiveScore(active[j])
	})

	if n > len(active) {
		n = len(active)
	}
	return active[:n], nil
}

func (s *MemoryStore) DeleteExpiredChunks(_ context.Context) (int64, error) {
	now := time.Now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()

	kept := s.chunks[:0]
	var deleted int64
	for _, entry := range s.chunks {
		if !entry.Persist && entry.CreatedAt+int64(entry.TTL) < now {
			deleted++
			continue
		}
		kept = append(kept, entry)
	}
	s.chunks = kept
	return deleted, nil
}

// SetPeerScore sets the quality score for a peer. Exported for testing.
func (s *MemoryStore) SetPeerScore(id string, score int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.peers[id]
	if !ok {
		return ErrNotFound
	}
	p.QualityScore = score
	s.peers[id] = p
	return nil
}

func removeID(ids []string, id string) []string {
	for i, v := range ids {
		if v == id {
			return append(ids[:i], ids[i+1:]...)
		}
	}
	return ids
}

func (s *MemoryStore) peerByKey(key string) (model.Peer, bool) {
	for _, peer := range s.peers {
		if peer.Key == key && !s.isExpired(peer, time.Now().UTC()) {
			return peer, true
		}
	}
	return model.Peer{}, false
}

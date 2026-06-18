package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"errors"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
)

func (s *dbStore) Upsert(ctx context.Context, req model.RegisterRequest) (model.Peer, error) {
	id := strings.TrimSpace(req.ID)
	if id == "" {
		return model.Peer{}, ErrInvalidPeer
	}
	if len(req.Addresses) == 0 {
		return model.Peer{}, ErrInvalidPeer
	}
	if len(req.Keys) == 0 {
		return model.Peer{}, ErrInvalidPeer
	}

	keys := copyKeys(req.Keys)
	if len(keys) == 0 {
		return model.Peer{}, ErrInvalidKey
	}

	ttl := req.TTLSeconds
	if ttl <= 0 {
		ttl = defaultTTL
	}

	now := time.Now().UTC()
	nowUnix := now.Unix()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Peer{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var existingQualityScore int
	var existingQualityValid, existingQualityInvalid int
	var existingEncKey, existingSigKey string

	err = tx.QueryRowContext(ctx,
		`SELECT quality_score, quality_valid, quality_invalid, encryption_key, signature_key FROM peers WHERE id = $1`, id,
	).Scan(&existingQualityScore, &existingQualityValid, &existingQualityInvalid, &existingEncKey, &existingSigKey)

	isNew := false
	if err == sql.ErrNoRows {
		isNew = true
		existingQualityScore = InitialQualityScore
	} else if err != nil {
		return model.Peer{}, fmt.Errorf("select peer: %w", err)
	}

	encKey := strings.TrimSpace(req.EncryptionKey)
	sigKey := strings.TrimSpace(req.SignatureKey)
	if encKey == "" {
		encKey = existingEncKey
	}
	if sigKey == "" {
		sigKey = existingSigKey
	}

	peerFlag := string(req.PeerFlag)

	if isNew {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO peers (id, addresses, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid, peer_flag)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			id, jsonString(req.Addresses),
			encKey, sigKey, jsonString(req.Metadata), nowUnix, ttl,
			InitialQualityScore, 0, 0, peerFlag,
		)
	} else {
		_, err = tx.ExecContext(ctx,
			`UPDATE peers SET
				addresses = $1,
				encryption_key = CASE WHEN $2 = '' THEN encryption_key ELSE $2 END,
				signature_key = CASE WHEN $3 = '' THEN signature_key ELSE $3 END,
				metadata = $4,
				last_seen = $5,
				ttl_seconds = $6,
				peer_flag = CASE WHEN $8 = '' THEN peer_flag ELSE $8 END
			 WHERE id = $7`,
			jsonString(req.Addresses), strings.TrimSpace(req.EncryptionKey), 
			strings.TrimSpace(req.SignatureKey),
			jsonString(req.Metadata), nowUnix, ttl, id, peerFlag,
		)
	}
	if err != nil {
		return model.Peer{}, fmt.Errorf("upsert peer: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM peer_keys WHERE peer_id = $1`, id); err != nil {
		return model.Peer{}, fmt.Errorf("delete keys: %w", err)
	}

	for _, k := range keys {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO peer_keys (login, signature, peer_id) VALUES ($1, $2, $3)
			 ON CONFLICT(login, signature) DO NOTHING`,
			k.Login, k.Signature, id,
		); err != nil {
			return model.Peer{}, fmt.Errorf("insert key: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return model.Peer{}, fmt.Errorf("commit: %w", err)
	}

	return s.getPeerByID(ctx, id)
}

func (s *dbStore) Get(ctx context.Context, id string) (model.Peer, error) {
	return s.getPeerByID(ctx, strings.TrimSpace(id))
}

func (s *dbStore) getPeerByID(ctx context.Context, id string) (model.Peer, error) {
	if id == "" {
		return model.Peer{}, ErrInvalidPeer
	}

	var peer model.Peer
	var addressesJSON, metadataJSON string
	var lastSeenUnix int64
	var qValid, qInvalid int
	var peerFlag string

	err := s.db.QueryRowContext(ctx,
		`SELECT id, addresses, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid, peer_flag FROM peers WHERE id = $1`, id,
	).Scan(
		&peer.ID, &addressesJSON, &peer.EncryptionKey,
		&peer.SignatureKey, &metadataJSON, &lastSeenUnix, &peer.TTLSeconds,
		&peer.QualityScore, &qValid, &qInvalid, &peerFlag,
	)
	peer.PeerFlag = model.PeerFlag(peerFlag)
	if err == sql.ErrNoRows {
		return model.Peer{}, ErrNotFound
	}
	if err != nil {
		return model.Peer{}, fmt.Errorf("scan peer: %w", err)
	}

	if err := json.Unmarshal([]byte(addressesJSON), &peer.Addresses); err != nil {
		peer.Addresses = strings.FieldsFunc(addressesJSON, func(r rune) bool { return r == ',' })
	}
	if err := json.Unmarshal([]byte(metadataJSON), &peer.Metadata); err != nil {
		peer.Metadata = nil
	}
	if peer.Metadata == nil {
		peer.Metadata = make(map[string]string)
	}

	peer.LastSeen = time.Unix(lastSeenUnix, 0).UTC()
	peer.Quality = model.QualityStats{ValidReports: qValid, InvalidReports: qInvalid}

	if s.isExpired(peer, time.Now().UTC()) {
		return model.Peer{}, ErrNotFound
	}

	peer.Keys = s.getPeerKeys(ctx, id)
	return peer, nil
}

func (s *dbStore) getPeerKeys(ctx context.Context, peerID string) []model.Key {
	rows, err := s.db.QueryContext(ctx,
		`SELECT login, signature FROM peer_keys WHERE peer_id = $1`, peerID,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var keys []model.Key
	for rows.Next() {
		var k model.Key
		if err := rows.Scan(&k.Login, &k.Signature); err == nil {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s *dbStore) GetByKey(ctx context.Context, login, signature string) (model.Peer, error) {
	login = strings.TrimSpace(login)
	signature = strings.TrimSpace(signature)
	if login == "" || signature == "" {
		return model.Peer{}, ErrInvalidKey
	}

	var peerID string
	err := s.db.QueryRowContext(ctx,
		`SELECT peer_id FROM peer_keys WHERE login = $1 AND signature = $2`, login, signature,
	).Scan(&peerID)
	if err == sql.ErrNoRows {
		return model.Peer{}, ErrNotFound
	}
	if err != nil {
		return model.Peer{}, fmt.Errorf("get peer_id by key: %w", err)
	}

	return s.getPeerByID(ctx, peerID)
}

func (s *dbStore) Delete(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrInvalidPeer
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `DELETE FROM peers WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete peer: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}

	_, _ = tx.ExecContext(ctx, `DELETE FROM peer_keys WHERE peer_id = $1`, id)
	return tx.Commit()
}

func (s *dbStore) List(ctx context.Context) ([]model.Peer, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM peers`)
	if err != nil {
		return nil, fmt.Errorf("list peers: %w", err)
	}
	defer rows.Close()

	now := time.Now().UTC()
	var out []model.Peer
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		peer, err := s.getPeerByID(ctx, id)
		if err == nil && !s.isExpired(peer, now) {
			out = append(out, peer)
		}
	}
	if out == nil {
		out = []model.Peer{}
	}
	return out, nil
}

func (s *dbStore) Heartbeat(ctx context.Context, id string, ttlSeconds int) (model.Peer, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Peer{}, ErrInvalidPeer
	}

	nowUnix := time.Now().UTC().Unix()

	result, err := s.db.ExecContext(ctx,
		`UPDATE peers SET last_seen = $1, ttl_seconds = CASE WHEN $2 > 0 THEN $2 ELSE ttl_seconds END WHERE id = $3`,
		nowUnix, ttlSeconds, id,
	)
	if err != nil {
		return model.Peer{}, fmt.Errorf("heartbeat: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return model.Peer{}, ErrNotFound
	}
	return s.getPeerByID(ctx, id)
}

func (s *dbStore) GetBestPeers(ctx context.Context, n int) ([]model.Peer, error) {
	if n <= 0 {
		return []model.Peer{}, nil
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, quality_score, peer_flag FROM peers`,
	)
	if err != nil {
		return nil, fmt.Errorf("get best peers: %w", err)
	}
	defer rows.Close()

	now := time.Now().UTC()
	type scoredPeer struct {
		peer  model.Peer
		score int
	}
	var candidates []scoredPeer

	for rows.Next() {
		var id, flag string
		var qualityScore int
		if err := rows.Scan(&id, &qualityScore, &flag); err != nil {
			continue
		}
		peer, err := s.getPeerByID(ctx, id)
		if err != nil || s.isExpired(peer, now) {
			continue
		}
		peer.PeerFlag = model.PeerFlag(flag)
		effective := EffectiveScore(peer)
		candidates = append(candidates, scoredPeer{peer: peer, score: effective})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	if n > len(candidates) {
		n = len(candidates)
	}
	out := make([]model.Peer, n)
	for i := 0; i < n; i++ {
		out[i] = candidates[i].peer
	}
	return out, nil
}

func (s *dbStore) isExpired(peer model.Peer, now time.Time) bool {
	ttl := peer.TTLSeconds
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return now.Sub(peer.LastSeen) > time.Duration(ttl)*time.Second
}

func (s *dbStore) SetPeerScore(id string, score int) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrInvalidPeer
	}

	ctx := context.Background()

	result, err := s.db.ExecContext(ctx, `UPDATE peers SET quality_score = $1 WHERE id = $2`, score, id)
	if err != nil {
		return fmt.Errorf("set peer score: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

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

func (s *dbStore) GetBestThickPeers(ctx context.Context, n int) ([]model.Peer, error) {
	if n <= 0 {
		return []model.Peer{}, nil
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM peers WHERE peer_flag IN ('thick', 'very_thick')`,
	)
	if err != nil {
		return nil, fmt.Errorf("get best thick peers: %w", err)
	}
	defer rows.Close()

	now := time.Now().UTC()
	var candidates []model.Peer

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		peer, err := s.getPeerByID(ctx, id)
		if err != nil || s.isExpired(peer, now) {
			continue
		}
		candidates = append(candidates, peer)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return EffectiveScore(candidates[i]) > EffectiveScore(candidates[j])
	})

	if n > len(candidates) {
		n = len(candidates)
	}
	if candidates == nil {
		return []model.Peer{}, nil
	}
	return candidates[:n], nil
}
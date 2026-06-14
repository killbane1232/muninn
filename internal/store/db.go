package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
)

const driverSQLite = "sqlite"
const driverPostgres = "postgres"

var driverMap = map[string]string{
	driverSQLite:  "sqlite",
	driverPostgres: "pgx",
}

var defaultDSN = map[string]string{
	driverSQLite:  "file:muninn.db?cache=shared&_journal_mode=WAL",
	driverPostgres: "postgres://localhost:5432/muninn?sslmode=disable",
}

type dbStore struct {
	db     *sql.DB
	driver string
}

func NewDB(driver, dsn string) (Store, error) {
	sqlDriver, ok := driverMap[driver]
	if !ok {
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}

	if dsn == "" {
		dsn = defaultDSN[driver]
	}

	db, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", driver, err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping %s: %w", driver, err)
	}

	if err := runMigrations(db, sqlDriver); err != nil {
		return nil, fmt.Errorf("migrations %s: %w", driver, err)
	}

	return &dbStore{db: db, driver: driver}, nil
}

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
			`INSERT INTO peers (id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid, peer_flag)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			id, jsonString(req.Addresses), strings.TrimSpace(req.PublicKey),
			encKey, sigKey, jsonString(req.Metadata), nowUnix, ttl,
			InitialQualityScore, 0, 0, peerFlag,
		)
	} else {
		_, err = tx.ExecContext(ctx,
			`UPDATE peers SET
				addresses = $1,
				public_key = $2,
				encryption_key = CASE WHEN $3 = '' THEN encryption_key ELSE $3 END,
				signature_key = CASE WHEN $4 = '' THEN signature_key ELSE $4 END,
				metadata = $5,
				last_seen = $6,
				ttl_seconds = $7,
				peer_flag = CASE WHEN $9 = '' THEN peer_flag ELSE $9 END
			 WHERE id = $8`,
			jsonString(req.Addresses), strings.TrimSpace(req.PublicKey),
			strings.TrimSpace(req.EncryptionKey), strings.TrimSpace(req.SignatureKey),
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
		`SELECT id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid, peer_flag FROM peers WHERE id = $1`, id,
	).Scan(
		&peer.ID, &addressesJSON, &peer.PublicKey, &peer.EncryptionKey,
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

func (s *dbStore) PurgeExpired(ctx context.Context) int {
	now := time.Now().UTC()
	cutoff := now.Unix()
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM peers WHERE last_seen + ttl_seconds < $1`, cutoff,
	)
	if err != nil {
		return 0
	}
	n, _ := result.RowsAffected()
	return int(n)
}

func (s *dbStore) SetChunkHash(ctx context.Context, fileID string, chunkIndex int, req model.RegisterChunkRequest) error {
	fileID = strings.TrimSpace(fileID)
	senderID := strings.TrimSpace(req.SenderID)
	recipientID := strings.TrimSpace(req.RecipientID)
	peerID := strings.TrimSpace(req.PeerID)
	hash := normalizeHash(req.Hash)
	if fileID == "" || senderID == "" || recipientID == "" || peerID == "" || hash == "" || !validHashFormat(hash) || chunkIndex < 0 || strings.TrimSpace(req.Signature) == "" {
		return ErrInvalidChunk
	}

	sender, err := s.getPeerByID(ctx, senderID)
	if err != nil {
		return ErrNotFound
	}

	msg := sign.ExpectedPayload(fileID, chunkIndex, hash)
	if err := verifyPeerSignature(sender, req.Signature, msg); err != nil {
		return err
	}

	persist := 0
	if req.Persist {
		persist = 1
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO chunks (file_id, chunk_index, expected_hash, sender_id, recipient_id, holder_peer_id, persist, confirmed) VALUES ($1, $2, $3, $4, $5, $6, $7, 0)`,
		fileID, chunkIndex, hash, senderID, recipientID, peerID, persist,
	)
	if err != nil {
		return fmt.Errorf("set chunk hash: %w", err)
	}
	return nil
}

func (s *dbStore) GetChunksByRecipient(ctx context.Context, recipientID string) ([]model.ChunkRecord, error) {
	recipientID = strings.TrimSpace(recipientID)
	if recipientID == "" {
		return nil, ErrInvalidChunk
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT file_id, chunk_index, expected_hash, sender_id, recipient_id, holder_peer_id, persist, confirmed FROM chunks WHERE recipient_id = $1`, recipientID,
	)
	if err != nil {
		return nil, fmt.Errorf("get chunks by recipient: %w", err)
	}
	defer rows.Close()

	var records []model.ChunkRecord
	for rows.Next() {
		var rec model.ChunkRecord
		var peerID string
		var persist, confirmed int
		if err := rows.Scan(&rec.FileID, &rec.ChunkIndex, &rec.Hash, &rec.SenderID, &rec.RecipientID, &peerID, &persist, &confirmed); err != nil {
			return nil, fmt.Errorf("scan chunk: %w", err)
		}
		rec.PeerID = peerID
		rec.Persist = persist != 0
		rec.Confirmed = confirmed != 0
		records = append(records, rec)
	}
	if records == nil {
		records = []model.ChunkRecord{}
	}
	return records, nil
}

func (s *dbStore) ReportChunk(ctx context.Context, sourcePeerID string, req model.ChunkReportRequest) (model.ChunkReportResult, error) {
	sourcePeerID = strings.TrimSpace(sourcePeerID)
	reporterID := strings.TrimSpace(req.ReporterID)
	fileID := strings.TrimSpace(req.FileID)
	reported := normalizeHash(req.Hash)

	if sourcePeerID == "" || reporterID == "" || fileID == "" || reported == "" || !validHashFormat(reported) || req.ChunkIndex < 0 ||
		strings.TrimSpace(req.Signature) == "" {
		return model.ChunkReportResult{}, ErrInvalidChunk
	}

	reporter, err := s.getPeerByID(ctx, reporterID)
	if err != nil {
		return model.ChunkReportResult{}, ErrNotFound
	}

	msg := sign.ReportedPayload(fileID, req.ChunkIndex, reported, sourcePeerID)
	if err := verifyPeerSignature(reporter, req.Signature, msg); err != nil {
		return model.ChunkReportResult{}, err
	}

	var expected string
	err = s.db.QueryRowContext(ctx,
		`SELECT expected_hash FROM chunks WHERE file_id = $1 AND chunk_index = $2`, fileID, req.ChunkIndex,
	).Scan(&expected)
	if err == sql.ErrNoRows {
		return model.ChunkReportResult{}, ErrChunkNotFound
	}
	if err != nil {
		return model.ChunkReportResult{}, fmt.Errorf("get chunk: %w", err)
	}

	valid := reported == expected
	delta := QualityPointsInvalid
	qValidInc := 0
	qInvalidInc := 0
	if valid {
		delta = QualityPointsValid
		qValidInc = 1
	} else {
		qInvalidInc = 1
	}

	if _, err := s.db.ExecContext(ctx,
		`UPDATE peers SET quality_score = quality_score + $1, quality_valid = quality_valid + $2, quality_invalid = quality_invalid + $3 WHERE id = $4`,
		delta, qValidInc, qInvalidInc, sourcePeerID,
	); err != nil {
		return model.ChunkReportResult{}, fmt.Errorf("update quality: %w", err)
	}

	peer, err := s.getPeerByID(ctx, sourcePeerID)
	if err != nil {
		return model.ChunkReportResult{}, fmt.Errorf("get updated peer: %w", err)
	}

	return model.ChunkReportResult{
		Valid:        valid,
		ExpectedHash: expected,
		ReportedHash: reported,
		Delta:        delta,
		Peer:         peer,
	}, nil
}

func (s *dbStore) ConfirmChunk(ctx context.Context, req model.ConfirmChunkRequest) (model.ConfirmChunkResult, error) {
	recipientID := strings.TrimSpace(req.RecipientID)
	fileID := strings.TrimSpace(req.FileID)
	confirmed := normalizeHash(req.Hash)

	if recipientID == "" || fileID == "" || confirmed == "" || !validHashFormat(confirmed) || req.ChunkIndex < 0 ||
		strings.TrimSpace(req.Signature) == "" {
		return model.ConfirmChunkResult{}, ErrInvalidChunk
	}

	recipient, err := s.getPeerByID(ctx, recipientID)
	if err != nil {
		return model.ConfirmChunkResult{}, ErrNotFound
	}

	msg := sign.ConfirmedPayload(fileID, req.ChunkIndex, confirmed)
	if err := verifyPeerSignature(recipient, req.Signature, msg); err != nil {
		return model.ConfirmChunkResult{}, err
	}

	var expectedHash, senderID string
	err = s.db.QueryRowContext(ctx,
		`SELECT expected_hash, sender_id FROM chunks WHERE file_id = $1 AND chunk_index = $2 AND recipient_id = $3`, fileID, req.ChunkIndex, recipientID,
	).Scan(&expectedHash, &senderID)
	if err == sql.ErrNoRows {
		return model.ConfirmChunkResult{}, ErrChunkNotFound
	}
	if err != nil {
		return model.ConfirmChunkResult{}, fmt.Errorf("get chunk: %w", err)
	}

	valid := confirmed == expectedHash
	delta := QualityPointsInvalid
	qValidInc := 0
	qInvalidInc := 0
	if valid {
		delta = QualityPointsValid
		qValidInc = 1
	} else {
		qInvalidInc = 1
	}

	if _, err := s.db.ExecContext(ctx,
		`UPDATE peers SET quality_score = quality_score + $1, quality_valid = quality_valid + $2, quality_invalid = quality_invalid + $3 WHERE id = $4`,
		delta, qValidInc, qInvalidInc, senderID,
	); err != nil {
		return model.ConfirmChunkResult{}, fmt.Errorf("update quality: %w", err)
	}

	if valid {
		if _, err := s.db.ExecContext(ctx,
			`UPDATE chunks SET confirmed = 1 WHERE file_id = $1 AND chunk_index = $2 AND recipient_id = $3`, fileID, req.ChunkIndex, recipientID,
		); err != nil {
			return model.ConfirmChunkResult{}, fmt.Errorf("update confirmed: %w", err)
		}
	}

	sender, err := s.getPeerByID(ctx, senderID)
	if err != nil {
		return model.ConfirmChunkResult{}, fmt.Errorf("get updated peer: %w", err)
	}

	return model.ConfirmChunkResult{
		Valid:         valid,
		ExpectedHash:  expectedHash,
		ConfirmedHash: confirmed,
		Delta:         delta,
		Peer:          sender,
	}, nil
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

func (s *dbStore) SetSignal(ctx context.Context, peerID string, sig model.Signal) error {
	peerID = strings.TrimSpace(peerID)
	if peerID == "" || sig.From == "" || sig.Type == "" {
		return ErrInvalidChunk
	}
	if _, err := s.getPeerByID(ctx, peerID); err != nil {
		return ErrNotFound
	}
	now := time.Now().UTC().Unix()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO signals (peer_id, sig_from, sig_type, sig_data, created_at) VALUES ($1, $2, $3, $4, $5)`,
		peerID, sig.From, sig.Type, sig.Data, now,
	)
	return err
}

func (s *dbStore) PollSignals(ctx context.Context, peerID string) ([]model.Signal, error) {
	peerID = strings.TrimSpace(peerID)
	if peerID == "" {
		return nil, ErrInvalidChunk
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT sig_from, sig_type, sig_data FROM signals WHERE peer_id = $1`, peerID,
	)
	if err != nil {
		return nil, fmt.Errorf("query signals: %w", err)
	}
	defer rows.Close()

	var sigs []model.Signal
	for rows.Next() {
		var sig model.Signal
		if err := rows.Scan(&sig.From, &sig.Type, &sig.Data); err != nil {
			return nil, fmt.Errorf("scan signal: %w", err)
		}
		sigs = append(sigs, sig)
	}

	s.db.ExecContext(ctx, `DELETE FROM signals WHERE peer_id = $1`, peerID)

	if sigs == nil {
		return []model.Signal{}, nil
	}
	return sigs, nil
}

func (s *dbStore) Close() error {
	return s.db.Close()
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

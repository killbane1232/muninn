package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
)

const driverSQLite = "sqlite"
const driverPostgres = "postgres"
const driverClickhouse = "clickhouse"

type dbStore struct {
	db     *sql.DB
	driver string
}

type dbInit struct {
	driver    string
	dsn       string
	tableDDL  []string
}

var dbInits = map[string]dbInit{
	driverSQLite: {
		driver: "sqlite",
		dsn:    "file:muninn.db?cache=shared&_journal_mode=WAL",
		tableDDL: []string{
			`CREATE TABLE IF NOT EXISTS peers (
				id TEXT PRIMARY KEY,
				addresses TEXT NOT NULL,
				public_key TEXT NOT NULL DEFAULT '',
				encryption_key TEXT NOT NULL DEFAULT '',
				signature_key TEXT NOT NULL DEFAULT '',
				metadata TEXT NOT NULL DEFAULT '{}',
				last_seen INTEGER NOT NULL,
				ttl_seconds INTEGER NOT NULL DEFAULT 300,
				quality_score INTEGER NOT NULL DEFAULT 1000,
				quality_valid INTEGER NOT NULL DEFAULT 0,
				quality_invalid INTEGER NOT NULL DEFAULT 0
			)`,
			`CREATE TABLE IF NOT EXISTS peer_keys (
				login TEXT NOT NULL,
				signature TEXT NOT NULL,
				peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
				PRIMARY KEY (login, signature)
			)`,
			`CREATE TABLE IF NOT EXISTS chunks (
				file_id TEXT NOT NULL,
				chunk_index INTEGER NOT NULL,
				expected_hash TEXT NOT NULL,
				PRIMARY KEY (file_id, chunk_index)
			)`,
		},
	},
	driverPostgres: {
		driver: "pgx",
		dsn:    "postgres://localhost:5432/muninn?sslmode=disable",
		tableDDL: []string{
			`CREATE TABLE IF NOT EXISTS peers (
				id TEXT PRIMARY KEY,
				addresses TEXT NOT NULL,
				public_key TEXT NOT NULL DEFAULT '',
				encryption_key TEXT NOT NULL DEFAULT '',
				signature_key TEXT NOT NULL DEFAULT '',
				metadata TEXT NOT NULL DEFAULT '{}',
				last_seen BIGINT NOT NULL,
				ttl_seconds INTEGER NOT NULL DEFAULT 300,
				quality_score INTEGER NOT NULL DEFAULT 1000,
				quality_valid INTEGER NOT NULL DEFAULT 0,
				quality_invalid INTEGER NOT NULL DEFAULT 0
			)`,
			`CREATE TABLE IF NOT EXISTS peer_keys (
				login TEXT NOT NULL,
				signature TEXT NOT NULL,
				peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
				PRIMARY KEY (login, signature)
			)`,
			`CREATE TABLE IF NOT EXISTS chunks (
				file_id TEXT NOT NULL,
				chunk_index INTEGER NOT NULL,
				expected_hash TEXT NOT NULL,
				PRIMARY KEY (file_id, chunk_index)
			)`,
		},
	},
	driverClickhouse: {
		driver: "clickhouse",
		dsn:    "clickhouse://localhost:9000/muninn?dial_timeout=5s",
		tableDDL: []string{
			`CREATE TABLE IF NOT EXISTS peers (
				id String,
				addresses String,
				public_key String,
				encryption_key String,
				signature_key String,
				metadata String,
				last_seen Int64,
				ttl_seconds Int32,
				quality_score Int32,
				quality_valid Int32,
				quality_invalid Int32
			) ENGINE = ReplacingMergeTree(last_seen)
			ORDER BY id`,
			`CREATE TABLE IF NOT EXISTS peer_keys (
				login String,
				signature String,
				peer_id String,
				last_seen Int64
			) ENGINE = ReplacingMergeTree(last_seen)
			ORDER BY (login, signature)`,
			`CREATE TABLE IF NOT EXISTS chunks (
				file_id String,
				chunk_index Int32,
				expected_hash String
			) ENGINE = ReplacingMergeTree()
			ORDER BY (file_id, chunk_index)`,
		},
	},
}

func NewDB(driver, dsn string) (Store, error) {
	init, ok := dbInits[driver]
	if !ok {
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}

	if dsn != "" {
		init.dsn = dsn
	}

	db, err := sql.Open(init.driver, init.dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", driver, err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping %s: %w", driver, err)
	}

	s := &dbStore{db: db, driver: driver}

	for _, ddl := range init.tableDDL {
		if _, err := db.Exec(ddl); err != nil {
			return nil, fmt.Errorf("create table %s: %w", driver, err)
		}
	}

	return s, nil
}

func (s *dbStore) isClickhouse() bool {
	return s.driver == driverClickhouse
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

	if s.isClickhouse() {
		return s.upsertCH(ctx, id, keys, req, ttl, now, nowUnix)
	}

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

	if isNew {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO peers (id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			id, jsonString(req.Addresses), strings.TrimSpace(req.PublicKey),
			encKey, sigKey, jsonString(req.Metadata), nowUnix, ttl,
			InitialQualityScore, 0, 0,
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
				ttl_seconds = $7
			 WHERE id = $8`,
			jsonString(req.Addresses), strings.TrimSpace(req.PublicKey),
			strings.TrimSpace(req.EncryptionKey), strings.TrimSpace(req.SignatureKey),
			jsonString(req.Metadata), nowUnix, ttl, id,
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

func (s *dbStore) upsertCH(ctx context.Context, id string, keys []model.Key, req model.RegisterRequest, ttl int, now time.Time, nowUnix int64) (model.Peer, error) {
	existing, err := s.getPeerByID(ctx, id)
	isNew := false
	if err == ErrNotFound {
		isNew = true
	} else if err != nil {
		return model.Peer{}, err
	}

	encKey := strings.TrimSpace(req.EncryptionKey)
	sigKey := strings.TrimSpace(req.SignatureKey)
	qualityScore := InitialQualityScore
	qValid := 0
	qInvalid := 0

	if !isNew {
		qualityScore = existing.QualityScore
		qValid = existing.Quality.ValidReports
		qInvalid = existing.Quality.InvalidReports
		if encKey == "" {
			encKey = existing.EncryptionKey
		}
		if sigKey == "" {
			sigKey = existing.SignatureKey
		}
		for _, k := range existing.Keys {
			if _, err := s.db.ExecContext(ctx,
				`ALTER TABLE peer_keys DELETE WHERE login = $1 AND signature = $2`,
				k.Login, k.Signature,
			); err != nil {
				return model.Peer{}, fmt.Errorf("delete key: %w", err)
			}
		}
	} else {
		for _, k := range keys {
			var count int
			s.db.QueryRowContext(ctx,
				`SELECT count() FROM peer_keys WHERE login = $1 AND signature = $2`,
				k.Login, k.Signature,
			).Scan(&count)
			if count > 0 {
				return model.Peer{}, ErrKeyTaken
			}
		}
	}

	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO peers (id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		id, jsonString(req.Addresses), strings.TrimSpace(req.PublicKey),
		encKey, sigKey, jsonString(req.Metadata), nowUnix, ttl,
		qualityScore, qValid, qInvalid,
	); err != nil {
		return model.Peer{}, fmt.Errorf("insert peer: %w", err)
	}

	for _, k := range keys {
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO peer_keys (login, signature, peer_id, last_seen) VALUES ($1, $2, $3, $4)`,
			k.Login, k.Signature, id, nowUnix,
		); err != nil {
			return model.Peer{}, fmt.Errorf("insert key: %w", err)
		}
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

	query := `SELECT id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid FROM peers WHERE id = $1`
	if s.isClickhouse() {
		query = `SELECT id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid FROM peers FINAL WHERE id = $1`
	}

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&peer.ID, &addressesJSON, &peer.PublicKey, &peer.EncryptionKey,
		&peer.SignatureKey, &metadataJSON, &lastSeenUnix, &peer.TTLSeconds,
		&peer.QualityScore, &qValid, &qInvalid,
	)
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
	query := `SELECT login, signature FROM peer_keys WHERE peer_id = $1`
	if s.isClickhouse() {
		query = `SELECT login, signature FROM peer_keys FINAL WHERE peer_id = $1`
	}

	rows, err := s.db.QueryContext(ctx, query, peerID)
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
	query := `SELECT peer_id FROM peer_keys WHERE login = $1 AND signature = $2`
	if s.isClickhouse() {
		query = `SELECT peer_id FROM peer_keys FINAL WHERE login = $1 AND signature = $2`
	}

	err := s.db.QueryRowContext(ctx, query, login, signature).Scan(&peerID)
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

	if s.isClickhouse() {
		result, err := s.db.ExecContext(ctx, `ALTER TABLE peer_keys DELETE WHERE peer_id = $1`, id)
		if err != nil {
			return fmt.Errorf("delete keys: %w", err)
		}
		_, err = s.db.ExecContext(ctx, `ALTER TABLE peers DELETE WHERE id = $1`, id)
		if err != nil {
			return fmt.Errorf("delete peer: %w", err)
		}
		_ = result
		return nil
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
	query := `SELECT id FROM peers`
	if s.isClickhouse() {
		query = `SELECT id FROM peers FINAL`
	}

	rows, err := s.db.QueryContext(ctx, query)
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

	if s.isClickhouse() {
		existing, err := s.getPeerByID(ctx, id)
		if err != nil {
			return model.Peer{}, err
		}
		ttl := existing.TTLSeconds
		if ttlSeconds > 0 {
			ttl = ttlSeconds
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO peers (id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			id, jsonString(existing.Addresses), existing.PublicKey,
			existing.EncryptionKey, existing.SignatureKey, jsonString(existing.Metadata),
			nowUnix, ttl, existing.QualityScore, existing.Quality.ValidReports, existing.Quality.InvalidReports,
		); err != nil {
			return model.Peer{}, fmt.Errorf("heartbeat insert: %w", err)
		}
		return s.getPeerByID(ctx, id)
	}

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

	query := `SELECT id FROM peers ORDER BY quality_score DESC`
	if s.isClickhouse() {
		query = `SELECT id FROM peers FINAL ORDER BY quality_score DESC`
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get best peers: %w", err)
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
			if len(out) >= n {
				break
			}
		}
	}
	if out == nil {
		out = []model.Peer{}
	}
	return out, nil
}

func (s *dbStore) PurgeExpired(ctx context.Context) int {
	now := time.Now().UTC()

	if s.isClickhouse() {
		cutoff := now.Unix()
		result, err := s.db.ExecContext(ctx,
			`ALTER TABLE peers DELETE WHERE last_seen + ttl_seconds < $1`, cutoff,
		)
		if err != nil {
			return 0
		}
		n, _ := result.RowsAffected()
		if n > 0 {
			s.db.ExecContext(ctx, `ALTER TABLE peer_keys DELETE WHERE peer_id NOT IN (SELECT id FROM peers FINAL)`)
		}
		return int(n)
	}

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
	hash := normalizeHash(req.Hash)
	if fileID == "" || senderID == "" || hash == "" || chunkIndex < 0 || strings.TrimSpace(req.Signature) == "" {
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

	if s.isClickhouse() {
		_, err = s.db.ExecContext(ctx,
			`INSERT INTO chunks (file_id, chunk_index, expected_hash) VALUES ($1, $2, $3)`,
			fileID, chunkIndex, hash,
		)
	} else {
		_, err = s.db.ExecContext(ctx,
			`INSERT INTO chunks (file_id, chunk_index, expected_hash) VALUES ($1, $2, $3)
			 ON CONFLICT(file_id, chunk_index) DO UPDATE SET expected_hash = $3`,
			fileID, chunkIndex, hash,
		)
	}
	if err != nil {
		return fmt.Errorf("set chunk hash: %w", err)
	}
	return nil
}

func (s *dbStore) ReportChunk(ctx context.Context, sourcePeerID string, req model.ChunkReportRequest) (model.ChunkReportResult, error) {
	sourcePeerID = strings.TrimSpace(sourcePeerID)
	reporterID := strings.TrimSpace(req.ReporterID)
	fileID := strings.TrimSpace(req.FileID)
	reported := normalizeHash(req.Hash)

	if sourcePeerID == "" || reporterID == "" || fileID == "" || reported == "" || req.ChunkIndex < 0 ||
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
	chunkQuery := `SELECT expected_hash FROM chunks WHERE file_id = $1 AND chunk_index = $2`
	if s.isClickhouse() {
		chunkQuery = `SELECT expected_hash FROM chunks FINAL WHERE file_id = $1 AND chunk_index = $2`
	}
	err = s.db.QueryRowContext(ctx, chunkQuery, fileID, req.ChunkIndex).Scan(&expected)
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

	if s.isClickhouse() {
		if err := s.updateQualityCH(ctx, sourcePeerID, delta, qValidInc, qInvalidInc); err != nil {
			return model.ChunkReportResult{}, err
		}
	} else {
		if _, err := s.db.ExecContext(ctx,
			`UPDATE peers SET quality_score = quality_score + $1, quality_valid = quality_valid + $2, quality_invalid = quality_invalid + $3 WHERE id = $4`,
			delta, qValidInc, qInvalidInc, sourcePeerID,
		); err != nil {
			return model.ChunkReportResult{}, fmt.Errorf("update quality: %w", err)
		}
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

func (s *dbStore) updateQualityCH(ctx context.Context, peerID string, delta, validInc, invalidInc int) error {
	peer, err := s.getPeerByID(ctx, peerID)
	if err != nil {
		return err
	}

	newScore := peer.QualityScore + delta
	newValid := peer.Quality.ValidReports + validInc
	newInvalid := peer.Quality.InvalidReports + invalidInc

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO peers (id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		peerID, jsonString(peer.Addresses), peer.PublicKey, peer.EncryptionKey,
		peer.SignatureKey, jsonString(peer.Metadata), peer.LastSeen.Unix(),
		peer.TTLSeconds, newScore, newValid, newInvalid,
	)
	return err
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

	if s.isClickhouse() {
		peer, err := s.getPeerByID(ctx, id)
		if err != nil {
			return err
		}
		_, err = s.db.ExecContext(ctx,
			`INSERT INTO peers (id, addresses, public_key, encryption_key, signature_key, metadata, last_seen, ttl_seconds, quality_score, quality_valid, quality_invalid)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			id, jsonString(peer.Addresses), peer.PublicKey, peer.EncryptionKey,
			peer.SignatureKey, jsonString(peer.Metadata), peer.LastSeen.Unix(),
			peer.TTLSeconds, score, peer.Quality.ValidReports, peer.Quality.InvalidReports,
		)
		return err
	}

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

func (s *dbStore) Close() error {
	return s.db.Close()
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

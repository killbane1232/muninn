package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/sign"
)

const driverSQLite = "sqlite"
const driverPostgres = "postgres"
const defaultChunkTTL = 86400

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

func (s *dbStore) SetChunkHash(ctx context.Context, fileID string, chunkIndex int, req model.RegisterChunkRequest) error {
	fileID = strings.TrimSpace(fileID)
	senderKey := strings.TrimSpace(req.SenderID)
	recipientKey := strings.TrimSpace(req.RecipientID)
	peerID := strings.TrimSpace(req.PeerID)
	hash := normalizeHash(req.Hash)
	log.Printf("SetChunkHash: file=%s idx=%d senderKey=%s recipientKey=%s peer=%s hash=%s persist=%v",
		fileID, chunkIndex, senderKey, recipientKey, peerID, hash, req.Persist)
	if fileID == "" || senderKey == "" || peerID == "" || hash == "" || !validHashFormat(hash) || chunkIndex < 0 || strings.TrimSpace(req.Signature) == "" {
		log.Printf("Invalid Chunk file=%s", fileID)
		return ErrInvalidChunk
	}

	senders, err := s.getPeerByKey(ctx, senderKey)
	if err != nil {
		log.Printf("peer=%s not found", senderKey)
		return ErrNotFound
	}

	msg := sign.ExpectedPayload(fileID, chunkIndex, hash)
	if err := verifyPeerSignature(senders[0], req.Signature, msg); err != nil {
		log.Printf("wrong signature on file=%s", fileID)
		return err
	}

	persist := 0
	if req.Persist {
		persist = 1
	}
	chunkTTL := req.TTL
	if chunkTTL <= 0 {
		chunkTTL = defaultChunkTTL
	}
	now := time.Now().Unix()
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO chunks (file_id, chunk_index, expected_hash, sender_id, recipient_id, holder_peer_id, persist, confirmed, created_at, updated_at, ttl) VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $8, $9)`,
		fileID, chunkIndex, hash, senderKey, recipientKey, peerID, persist, now, chunkTTL,
	)
	if err != nil {
		log.Printf("set chunk hash: %v", err)
		return fmt.Errorf("set chunk hash: %w", err)
	}
	return nil
}

func (s *dbStore) GetChunksByRecipient(ctx context.Context, recipientID string, dateFrom int64) ([]model.ChunkRecord, error) {
	recipientID = strings.TrimSpace(recipientID)
	if recipientID == "" {
		return nil, ErrInvalidChunk
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT file_id, chunk_index, expected_hash, sender_id, recipient_id, holder_peer_id, persist, confirmed, readed, created_at, updated_at, ttl FROM chunks WHERE recipient_id = $1 AND updated_at >= $2`, recipientID, dateFrom,
	)
	if err != nil {
		return nil, fmt.Errorf("get chunks by recipient: %w", err)
	}
	defer rows.Close()

	var records []model.ChunkRecord
	for rows.Next() {
		var rec model.ChunkRecord
		var peerID string
		var persist, confirmed, readed int
		if err := rows.Scan(&rec.FileID, &rec.ChunkIndex, &rec.Hash, &rec.SenderID, &rec.RecipientID, &peerID, &persist, &confirmed, &readed, &rec.CreatedAt, &rec.UpdatedAt, &rec.TTL); err != nil {
			return nil, fmt.Errorf("scan chunk: %w", err)
		}
		rec.PeerID = peerID
		rec.Persist = persist != 0
		rec.Confirmed = confirmed != 0
		rec.Readed = readed != 0
		records = append(records, rec)
	}
	if records == nil {
		records = []model.ChunkRecord{}
	}
	return records, nil
}

func (s *dbStore) GetChunksByFileID(ctx context.Context, fileID string) ([]model.ChunkRecord, error) {
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return nil, ErrInvalidChunk
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT file_id, chunk_index, expected_hash, sender_id, recipient_id, holder_peer_id, persist, confirmed, readed, created_at, updated_at, ttl FROM chunks WHERE file_id = $1`, fileID,
	)
	if err != nil {
		return nil, fmt.Errorf("get chunks by recipient: %w", err)
	}
	defer rows.Close()

	var records []model.ChunkRecord
	for rows.Next() {
		var rec model.ChunkRecord
		var peerID string
		var persist, confirmed, readed int
		if err := rows.Scan(&rec.FileID, &rec.ChunkIndex, &rec.Hash, &rec.SenderID, &rec.RecipientID, &peerID, &persist, &confirmed, &readed, &rec.CreatedAt, &rec.UpdatedAt, &rec.TTL); err != nil {
			return nil, fmt.Errorf("scan chunk: %w", err)
		}
		rec.PeerID = peerID
		rec.Persist = persist != 0
		rec.Confirmed = confirmed != 0
		rec.Readed = readed != 0
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
		log.Printf("report peer=%s not found", reporterID)
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
	recipientKey := strings.TrimSpace(req.RecipientID)
	fileID := strings.TrimSpace(req.FileID)
	confirmed := normalizeHash(req.Hash)

	if fileID == "" || confirmed == "" || !validHashFormat(confirmed) || req.ChunkIndex < 0 ||
		strings.TrimSpace(req.Signature) == "" {
		return model.ConfirmChunkResult{}, ErrInvalidChunk
	}

	recipients, err := s.getPeerByKey(ctx, recipientKey)
	if err != nil {
		log.Printf("confirm peer=%s not found", recipientKey)
		return model.ConfirmChunkResult{}, ErrNotFound
	}

	msg := sign.ConfirmedPayload(fileID, req.ChunkIndex, confirmed)
	if err := verifyPeerSignature(recipients[0], req.Signature, msg); err != nil {
		return model.ConfirmChunkResult{}, err
	}

	var expectedHash, senderKey string
	err = s.db.QueryRowContext(ctx,
		`SELECT expected_hash, sender_id FROM chunks WHERE file_id = $1 AND chunk_index = $2 AND recipient_id = $3`, fileID, req.ChunkIndex, recipientKey,
	).Scan(&expectedHash, &senderKey)
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
		`UPDATE peers SET quality_score = quality_score + $1, quality_valid = quality_valid + $2, quality_invalid = quality_invalid + $3 WHERE key = $4`,
		delta, qValidInc, qInvalidInc, senderKey,
	); err != nil {
		return model.ConfirmChunkResult{}, fmt.Errorf("update quality: %w", err)
	}

	if valid {
		var stmt string
		switch s.driver {
		case "pgx":
			stmt = `UPDATE chunks SET confirmed = 1, updated_at = extract(epoch from now()) WHERE file_id = $1 AND chunk_index = $2 AND recipient_id = $3`
		case "sqlite":
			stmt = `UPDATE chunks SET confirmed = 1, updated_at = unixepoch() WHERE file_id = $1 AND chunk_index = $2 AND recipient_id = $3`
		}
		if _, err := s.db.ExecContext(ctx, stmt, fileID, req.ChunkIndex, recipientKey); err != nil {
			return model.ConfirmChunkResult{}, fmt.Errorf("update confirmed: %w", err)
		}
	}

	senders, err := s.getPeerByKey(ctx, senderKey)
	if err != nil {
		return model.ConfirmChunkResult{}, fmt.Errorf("get updated peer: %w", err)
	}

	return model.ConfirmChunkResult{
		Valid:         valid,
		ExpectedHash:  expectedHash,
		ConfirmedHash: confirmed,
		Delta:         delta,
		Peer:          senders[0],
	}, nil
}

func (s *dbStore) ReadChunk(ctx context.Context, req model.ReadChunkRequest) (model.ReadChunkResult, error) {
	recipientKey := strings.TrimSpace(req.RecipientID)
	fileID := strings.TrimSpace(req.FileID)

	if fileID == "" || strings.TrimSpace(req.Signature) == "" {
		return model.ReadChunkResult{}, ErrInvalidChunk
	}

	recipients, err := s.getPeerByKey(ctx, recipientKey)
	if err != nil {
		log.Printf("read peer=%s not found", recipientKey)
		return model.ReadChunkResult{}, ErrNotFound
	}

	msg := sign.ReadPayload(fileID)
	if err := verifyPeerSignature(recipients[0], req.Signature, msg); err != nil {
		return model.ReadChunkResult{}, err
	}

	var readStmt string
	switch s.driver {
	case "pgx":
		readStmt = `UPDATE chunks SET readed = 1, updated_at = extract(epoch from now()) WHERE file_id = $1 AND recipient_id = $2`
	case "sqlite":
		readStmt = `UPDATE chunks SET readed = 1, updated_at = unixepoch() WHERE file_id = $1 AND recipient_id = $2`
	}
	if _, err := s.db.ExecContext(ctx, readStmt, fileID, recipientKey); err != nil {
		return model.ReadChunkResult{}, fmt.Errorf("update readed: %w", err)
	}

	return model.ReadChunkResult{Valid: true}, nil
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

func (s *dbStore) DeleteExpiredChunks(ctx context.Context) (int64, error) {
	var stmt string
	switch s.driver {
	case "sqlite":
		stmt = `DELETE FROM chunks WHERE persist = 0 AND created_at + ttl < unixepoch()`
	case "pgx":
		stmt = `DELETE FROM chunks WHERE persist = 0 AND created_at + ttl < extract(epoch from now())`
	}
	result, err := s.db.ExecContext(ctx, stmt)
	if err != nil {
		return 0, fmt.Errorf("delete expired chunks: %w", err)
	}
	return result.RowsAffected()
}

func (s *dbStore) Close() error {
	return s.db.Close()
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

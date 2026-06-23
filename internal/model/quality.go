package model

// QualityStats — счётчики проверок чанков для узла-источника.
type QualityStats struct {
	ValidReports   int `json:"valid_reports"`
	InvalidReports int `json:"invalid_reports"`
}

// RegisterChunkRequest — эталонный хэш чанка (манифест), подпись отправителя.
type RegisterChunkRequest struct {
	SenderID    string `json:"sender_id"`
	RecipientID string `json:"recipient_id"`
	Hash        string `json:"hash"`
	Signature   string `json:"signature"`
	PeerID      string `json:"peer_id"`
	Persist     bool   `json:"persist"`
	TTL         int    `json:"ttl,omitempty"`
}

type RegisterChunkBatchEntry struct {
	ChunkIndex  int    `json:"chunk_index"`
	SenderID    string `json:"sender_id"`
	RecipientID string `json:"recipient_id"`
	Hash        string `json:"hash"`
	Signature   string `json:"signature"`
	PeerID      string `json:"peer_id"`
	Persist     bool   `json:"persist"`
	TTL         int    `json:"ttl,omitempty"`
}

type RegisterChunkBatchRequest struct {
	Chunks []RegisterChunkBatchEntry `json:"chunks"`
}

type ChunkRecord struct {
	FileID      string `json:"file_id"`
	ChunkIndex  int    `json:"chunk_index"`
	SenderID    string `json:"sender_id"`
	RecipientID string `json:"recipient_id"`
	Hash        string `json:"hash"`
	PeerID      string `json:"peer_id"`
	Persist     bool   `json:"persist"`
	Confirmed   bool   `json:"confirmed"`
	Readed      bool   `json:"readed,omitempty"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
	TTL         int    `json:"ttl"`
}

// ChunkReportRequest — отчёт получателя о чанке от source peer.
type ChunkReportRequest struct {
	ReporterID string `json:"reporter_id"`
	FileID     string `json:"file_id"`
	ChunkIndex int    `json:"chunk_index"`
	Hash       string `json:"hash"`
	Signature  string `json:"signature"`
}

// ChunkReportResult — результат проверки и обновлённый узел.
type ChunkReportResult struct {
	Valid        bool   `json:"valid"`
	ExpectedHash string `json:"expected_hash"`
	ReportedHash string `json:"reported_hash"`
	Delta        int    `json:"delta"`
	Peer         Peer   `json:"peer"`
}

// ConfirmChunkRequest — подтверждение получения чанка получателем.
type ConfirmChunkRequest struct {
	RecipientID string `json:"recipient_id"`
	FileID      string `json:"file_id"`
	ChunkIndex  int    `json:"chunk_index"`
	Hash        string `json:"hash"`
	Signature   string `json:"signature"`
}

// ReadChunkRequest — подтверждение прочтения сообщения получателем.
type ReadChunkRequest struct {
	RecipientID string `json:"recipient_id"`
	FileID      string `json:"file_id"`
	Signature   string `json:"signature"`
}

type ConfirmChunkBatchEntry struct {
	FileID     string `json:"file_id"`
	ChunkIndex int    `json:"chunk_index"`
	Hash       string `json:"hash"`
	Signature  string `json:"signature"`
}

type ConfirmChunkBatchRequest struct {
	RecipientID string                   `json:"recipient_id"`
	Chunks      []ConfirmChunkBatchEntry `json:"chunks"`
}

// ConfirmChunkResult — результат подтверждения и обновлённый узел.
type ConfirmChunkResult struct {
	Valid         bool   `json:"valid"`
	ExpectedHash  string `json:"expected_hash"`
	ConfirmedHash string `json:"confirmed_hash"`
	Delta         int    `json:"delta"`
	Peer          Peer   `json:"peer"`
}

// ConfirmChunkResult — результат чтения и обновлённый узел.
type ReadChunkResult struct {
	Valid         bool   `json:"valid"`
}

// Signal — WebRTC signal для пира.
type Signal struct {
	From string `json:"from"`
	Type string `json:"type"`
	Data string `json:"data"`
}

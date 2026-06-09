package model

// QualityStats — счётчики проверок чанков для узла-источника.
type QualityStats struct {
	ValidReports   int `json:"valid_reports"`
	InvalidReports int `json:"invalid_reports"`
}

// RegisterChunkRequest — эталонный хэш чанка (манифест), подпись отправителя.
type RegisterChunkRequest struct {
	SenderID  string `json:"sender_id"`
	Hash      string `json:"hash"`
	Signature string `json:"signature"`
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

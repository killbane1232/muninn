package model

import "time"

// Peer — запись в телефонной книге P2P-узла.
type Peer struct {
	ID           string            `json:"id"`
	Username     string            `json:"username,omitempty"`
	Addresses    []string          `json:"addresses"`
	PublicKey      string            `json:"public_key,omitempty"`
	EncryptionKey  string            `json:"encryption_key,omitempty"`
	SignatureKey   string            `json:"signature_key,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	LastSeen     time.Time         `json:"last_seen"`
	TTLSeconds   int               `json:"ttl_seconds"`
	QualityScore int               `json:"quality_score"`
	Quality      QualityStats      `json:"quality"`
}

// RegisterRequest — тело запроса на регистрацию или обновление узла.
type RegisterRequest struct {
	ID          string            `json:"id"`
	Username    string            `json:"username,omitempty"`
	Addresses   []string          `json:"addresses"`
	PublicKey     string            `json:"public_key,omitempty"`
	EncryptionKey string            `json:"encryption_key,omitempty"`
	SignatureKey  string            `json:"signature_key,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	TTLSeconds  int               `json:"ttl_seconds,omitempty"`
}

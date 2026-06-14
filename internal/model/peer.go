package model

import "time"

// PeerFlag — флаг пира, влияющий на приоритет (возведение в степень quality_score).
type PeerFlag string

const (
	PeerFlagThin      PeerFlag = "thin"
	PeerFlagThick     PeerFlag = "thick"
	PeerFlagVeryThick PeerFlag = "very_thick"
)

// Key — зашифрованный логин с подписью для идентификации пира.
type Key struct {
	Login     string `json:"login"`
	Signature string `json:"signature"`
}

// Peer — запись в телефонной книге P2P-узла.
type Peer struct {
	ID            string            `json:"id"`
	Keys          []Key             `json:"keys"`
	Addresses     []string          `json:"addresses"`
	PublicKey     string            `json:"public_key,omitempty"`
	EncryptionKey string            `json:"encryption_key,omitempty"`
	SignatureKey  string            `json:"signature_key,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	LastSeen      time.Time         `json:"last_seen"`
	TTLSeconds    int               `json:"ttl_seconds"`
	QualityScore  int               `json:"quality_score"`
	Quality       QualityStats      `json:"quality"`
	PeerFlag      PeerFlag          `json:"peer_flag,omitempty"`
}

// RegisterRequest — тело запроса на регистрацию или обновление узла.
type RegisterRequest struct {
	ID            string            `json:"id"`
	Keys          []Key             `json:"keys"`
	Addresses     []string          `json:"addresses"`
	PublicKey     string            `json:"public_key,omitempty"`
	EncryptionKey string            `json:"encryption_key,omitempty"`
	SignatureKey  string            `json:"signature_key,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	TTLSeconds    int               `json:"ttl_seconds,omitempty"`
	PeerFlag      PeerFlag          `json:"peer_flag,omitempty"`
}

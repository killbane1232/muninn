package model

import "time"

// PeerFlag — флаг пира, влияющий на приоритет (возведение в степень quality_score).
type PeerFlag string

const (
	PeerFlagThin      PeerFlag = "thin"
	PeerFlagThick     PeerFlag = "thick"
	PeerFlagVeryThick PeerFlag = "very_thick"
)

// Peer — запись в телефонной книге P2P-узла.
type Peer struct {
	ID            string       `json:"id"`
	Login         string       `json:"login"`
	EncryptionKey string       `json:"encryption_key,omitempty"`
	SignatureKey  string       `json:"signature_key,omitempty"`
	LastSeen      time.Time    `json:"last_seen"`
	TTLSeconds    int          `json:"ttl_seconds"`
	QualityScore  int          `json:ignore`
	Quality       QualityStats `json:ignore`
	PeerFlag      PeerFlag     `json:"peer_flag,omitempty"`
	Fake          bool         `json:"is_fake,omitempty"`
}

func (p Peer) Key() string {
	return p.Login + ":" + p.SignatureKey
}

// RegisterRequest — тело запроса на регистрацию или обновление узла.
type RegisterRequest struct {
	ID            string   `json:"id"`
	Login         string   `json:"login"`
	EncryptionKey string   `json:"encryption_key,omitempty"`
	SignatureKey  string   `json:"signature_key,omitempty"`
	TTLSeconds    int      `json:"ttl_seconds,omitempty"`
	PeerFlag      PeerFlag `json:"peer_flag,omitempty"`
	Fake          *bool    `json:"fake,omitempty"`
}

// RefreshRequest — тело запроса на обновление узла.
type RefreshRequest struct {
	ID           string `json:"id"`
	Login        string `json:"login"`
	SignatureKey string `json:"signature_key,omitempty"`
}

CREATE TABLE IF NOT EXISTS peers (
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
);
CREATE TABLE IF NOT EXISTS peer_keys (
    login TEXT NOT NULL,
    signature TEXT NOT NULL,
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    PRIMARY KEY (login, signature)
);
CREATE TABLE IF NOT EXISTS chunks (
    file_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    expected_hash TEXT NOT NULL,
    sender_id TEXT NOT NULL DEFAULT '',
    recipient_id TEXT NOT NULL DEFAULT '',
    holder_peer_id TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS signals (
    peer_id TEXT NOT NULL,
    sig_from TEXT NOT NULL,
    sig_type TEXT NOT NULL,
    sig_data TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

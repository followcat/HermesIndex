CREATE TABLE IF NOT EXISTS sync_state (
    source TEXT NOT NULL,
    pg_id TEXT NOT NULL,
    text_hash TEXT,
    embedding_version TEXT,
    vector_id BIGINT,
    nsfw_score REAL,
    updated_at TIMESTAMPTZ DEFAULT now(),
    last_error TEXT,
    PRIMARY KEY (source, pg_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_state_updated_at ON sync_state (updated_at);

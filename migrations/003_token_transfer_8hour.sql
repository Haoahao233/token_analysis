-- Materialized 8-hour rolling aggregate per token
CREATE TABLE IF NOT EXISTS token_transfer_8hour (
  token_address varchar(42) PRIMARY KEY,
  txs_count bigint NOT NULL DEFAULT 0,
  updated_at timestamp NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_t8h_score ON token_transfer_8hour (txs_count DESC);


-- Per-token per-hour buckets limited to last 8 hours (rolling window)
CREATE TABLE IF NOT EXISTS token_transfer_8hour_points (
  token_address varchar(42) NOT NULL,
  hour timestamp NOT NULL,
  txs_count bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (token_address, hour)
);

CREATE INDEX IF NOT EXISTS idx_t8hp_hour ON token_transfer_8hour_points (hour);


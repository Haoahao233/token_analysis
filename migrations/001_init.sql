-- Hourly aggregate table for token transfer events
CREATE TABLE IF NOT EXISTS token_transfer_hourly (
  token_address varchar(42) NOT NULL,
  hour timestamp NOT NULL,
  txs_count bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (token_address, hour)
);

-- Worker offsets for incremental processing
CREATE TABLE IF NOT EXISTS worker_offsets (
  id text PRIMARY KEY,
  last_block bigint NOT NULL,
  last_log_index bigint NOT NULL,
  updated_at timestamp NOT NULL DEFAULT NOW()
);

-- Helpful indexes on source table (if not already present)
CREATE INDEX IF NOT EXISTS idx_tt_block_log ON token_transfers (block_number, log_index);
CREATE INDEX IF NOT EXISTS idx_tt_token_ts ON token_transfers (token_address, block_timestamp);
CREATE INDEX IF NOT EXISTS idx_tt_ts ON token_transfers (block_timestamp);

-- Helpful indexes on aggregate table
CREATE INDEX IF NOT EXISTS idx_tth_hour ON token_transfer_hourly (hour);
CREATE INDEX IF NOT EXISTS idx_tth_token_hour ON token_transfer_hourly (token_address, hour);


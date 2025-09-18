-- Optional covering index to speed incremental scan on token_transfers
-- Includes block_timestamp and token_address to reduce heap fetches
CREATE INDEX IF NOT EXISTS idx_tt_block_log_inc ON token_transfers (block_number, log_index) INCLUDE (block_timestamp, token_address);


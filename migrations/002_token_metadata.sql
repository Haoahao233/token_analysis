-- ERC20 metadata table
CREATE TABLE IF NOT EXISTS token_metadata (
  token_address varchar(42) PRIMARY KEY,
  name text,
  symbol text,
  decimals integer,
  total_supply numeric(78),
  first_seen_block bigint,
  updated_at timestamp NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_token_metadata_updated ON token_metadata (updated_at);


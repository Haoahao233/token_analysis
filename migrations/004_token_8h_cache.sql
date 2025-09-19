-- Cache table: one row per token with an 8-hour ring buffer
CREATE TABLE IF NOT EXISTS token_8h_cache (
  token_address varchar(42) PRIMARY KEY,
  base_hour timestamptz NOT NULL,        -- counts[8] corresponds to this hour (UTC, truncated to hour)
  counts integer[8] NOT NULL,            -- [1] oldest .. [8] newest
  sum8 bigint NOT NULL,                  -- redundant sum for fast leaderboard
  updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_t8c_sum8 ON token_8h_cache (sum8 DESC);
CREATE INDEX IF NOT EXISTS idx_tth_hour ON token_transfer_hourly (hour);

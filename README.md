Token Analytics — 8h Leaderboard and Series

1) What it provides
- Services
  - REST API
    - GET `/tokens/top?limit=100` — 8h leaderboard with token metadata
    - GET `/tokens/{token}/txs/8h` — last 8 hourly points for a token
    - GET `/tokens/{token}/metadata` — token info (name, symbol, decimals, total_supply)
  - Web UI
    - `/` — Leaderboard (single column with rank, token, bar, 8h txs)
    - `/token.html?address=0x...` — Token page (8h line chart + metadata, Etherscan link)
- Implementation
  - Analysis worker ingests `token_transfers`, writes hourly aggregates (`token_transfer_hourly`), and maintains a single 8h cache table:
    - `token_8h_cache` — per‑token ring buffer (counts integer[8], base_hour timestamptz, sum8 bigint)
      derived from `token_transfer_hourly` for the safe 8 full hours [H-7h..H]. Leaderboard and 8h series read from this cache.
  - Worker refreshes a Redis cache for the leaderboard (JSON + window times)
  - Optional metadata worker fetches ERC20 info via RPC and stores into `token_metadata`

2) Requirements
- Runtime
  - Go 1.21+
  - Postgres 13+ with these source tables populated: `blocks`, `token_transfers` (and `logs`, `transactions` if present)
  - Redis (for leaderboard cache)
  - Optional Ethereum JSON‑RPC (for metadata worker)
- Database schema
  - Apply project migrations in order (see Run → Migrations)
- Indexing
  - A covering index on `token_transfers` is included to accelerate incremental scans:
    - `CREATE INDEX idx_tt_block_log_inc ON token_transfers (block_number, log_index) INCLUDE (block_timestamp, token_address);`

3) How to run
- Migrations
  psql "$PG_DSN" -f migrations/001_init.sql
  psql "$PG_DSN" -f migrations/002_token_metadata.sql
  psql "$PG_DSN" -f migrations/005_idx_cover_next_transfers.sql
  psql "$PG_DSN" -f migrations/006_token_8h_cache.sql

- Analysis worker (aggregates + 8h roller + Redis refresh)
  BATCH_SIZE=20000 REORG_DEPTH=12 PG_DSN=postgres://user:pass@host:5432/db?sslmode=disable \
  REDIS_ADDR=127.0.0.1:6379 go run ./cmd/analysis-worker

- API gateway (serves API + web UI)
  PG_DSN=postgres://user:pass@host:5432/db?sslmode=disable REDIS_ADDR=127.0.0.1:6379 \
  API_ADDR=:8080 go run ./cmd/api-gateway
  Open http://localhost:8080/

- (Optional) Metadata worker (fill token_metadata via RPC)
  RPC_URL=http://127.0.0.1:8545 PG_DSN=postgres://user:pass@host:5432/db?sslmode=disable \
  go run ./cmd/metadata-worker

Notes
- The worker aggregates on the DB side (LIMIT → GROUP BY (token,hour) → UPSERT) and processes only up to `safe = latest − REORG_DEPTH`.
- When the checkpoint block equals `safe`, it still finishes that block by `log_index`.
- The 8h cache is updated after each batch and on a periodic ticker; first init seeds the ring buffer for the current safe hour window.
- If you backfill historical rows below the current checkpoint, recompute those hours (DELETE+INSERT for that hour) or add a background self‑heal task.

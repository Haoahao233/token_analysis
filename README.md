Overview

Token transfers analytics with a worker + API gateway.
Implements:
- Token TXs leaderboard (rolling 8h), cached in Redis with exact-SQL fallback.
- Hourly series per token backed by a materialized aggregate table.

Data source: Postgres with tables blocks, logs, token_transfers, transactions.
Optional: Erigon RPC (not required for this implementation).

Project Layout
- cmd/analysis-worker: incremental aggregator + leaderboard refresher
- cmd/api-gateway: REST API server
- internal/config: env-based config
- internal/store: Postgres store and queries
- internal/cache: Redis cache for leaderboard
- internal/aggregator: worker loop
- internal/api: HTTP handlers
- internal/models: DTOs
- internal/mock: mock store/cache for tests
- migrations: DDL for aggregates and indexes

Migrations (SQL)
Run migrations in Postgres before starting services:

  psql "$PG_DSN" -f migrations/001_init.sql

Environment
- PG_DSN: postgres://user:pass@localhost:5432/db?sslmode=disable
- REDIS_ADDR: 127.0.0.1:6379
- REDIS_DB: 0

Worker (analysis-worker)
- BATCH_SIZE: default 2000
- REORG_DEPTH: default 12
- IDLE_DELAY: default 3s
- LEADERBOARD_INTERVAL: default 60s
- LEADERBOARD_SIZE: default 100
- RPC_URL: default http://127.0.0.1:8545 (用于补齐缺失的 ERC20 元数据)
- METADATA_RETRY_INTERVAL: default 1h（元数据抓取失败后的重试间隔，避免反复触发revert）
- PROGRESS_LOG_INTERVAL: default 15s（进度日志打印间隔）

API (api-gateway)
- API_ADDR: default :8080

Run
1) Apply migrations
2) Start worker

  go run ./cmd/analysis-worker

3) Start API

  go run ./cmd/api-gateway

API Endpoints
- GET /tokens/top?limit=100
  Response: {"window": {"from":"...","to":"..."}, "tokens":[{"token_address":"0x..","name":"","symbol":"","decimals":18,"total_supply":"...","txs_count":123}]}

- GET /tokens/{token}/txs/hourly?from=ISO&to=ISO
  Response: {"token_address":"0x..","from":"...","to":"...","points":[{"hour":"...","txs_count":12}]}

- GET /tokens/{token}/metadata
  Response: {"token_address":"0x..","name":"","symbol":"","decimals":18,"total_supply":"...","first_seen_block":0,"updated_at":"..."}

Notes
- Leaderboard uses Redis ZSET key leaderboard:8h and stores window unix seconds in leaderboard:8h:{from,to}.
- Exact rolling 8h counts come from token_transfers (COUNT(*)), not distinct transactions.
- Hourly aggregates are upserted by the worker from token_transfers.block_timestamp.
- Worker 同步 hourly 时会：
  - 确保 token_metadata 存在并更新 first_seen_block（取最小值）
  - 若该 token 元数据缺失（name/symbol/decimals/total_supply 为空），将通过本地 RPC 获取并回写（不会覆盖已有非空字段）。在非合约地址或合约返回 revert 时跳过，并按间隔重试。
  - 周期性打印同步进度：checkpoint、安全区块、落后区块数、累计/最近处理速率

Metadata
- Table: token_metadata (address, name, symbol, decimals, total_supply, first_seen_block, updated_at)
- 由 worker 在缺失时自动补齐（依赖 RPC_URL），也可由外部流程补充或修正。
- 初始化地址集（可选）：
  INSERT INTO token_metadata (token_address)
  SELECT DISTINCT token_address FROM token_transfer_hourly
  ON CONFLICT (token_address) DO NOTHING;

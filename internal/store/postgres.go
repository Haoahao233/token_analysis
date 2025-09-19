package store

import (
    "context"
    "database/sql"
    "errors"
    "time"

    "github.com/example/tx-analytics/internal/models"
    "github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
    pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
    cfg, err := pgxpool.ParseConfig(dsn)
    if err != nil {
        return nil, err
    }
    pool, err := pgxpool.NewWithConfig(ctx, cfg)
    if err != nil {
        return nil, err
    }
    return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() { p.pool.Close() }

func (p *Postgres) GetCheckpoint(ctx context.Context, workerID string) (int64, int64, error) {
    var lastBlock, lastLog int64
    err := p.pool.QueryRow(ctx, `
        SELECT last_block, last_log_index
        FROM worker_offsets
        WHERE id = $1
    `, workerID).Scan(&lastBlock, &lastLog)
    if err != nil {
        // default start
        return 0, -1, nil
    }
    return lastBlock, lastLog, nil
}

func (p *Postgres) SetCheckpoint(ctx context.Context, workerID string, lastBlock int64, lastLogIdx int64) error {
    _, err := p.pool.Exec(ctx, `
        INSERT INTO worker_offsets (id, last_block, last_log_index, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (id)
        DO UPDATE SET last_block = EXCLUDED.last_block,
                      last_log_index = EXCLUDED.last_log_index,
                      updated_at = NOW()
    `, workerID, lastBlock, lastLogIdx)
    return err
}

func (p *Postgres) LatestBlockNumber(ctx context.Context) (int64, error) {
    var num int64
    err := p.pool.QueryRow(ctx, `SELECT COALESCE(MAX(number), 0) FROM blocks`).Scan(&num)
    return num, err
}

func (p *Postgres) LatestProcessedHour(ctx context.Context) (time.Time, error) {
    var ts *time.Time
    err := p.pool.QueryRow(ctx, `SELECT date_trunc('hour', MAX(hour)) FROM token_transfer_hourly`).Scan(&ts)
    if err != nil { return time.Time{}, err }
    if ts == nil { return time.Time{}, nil }
    return ts.UTC(), nil
}

func (p *Postgres) NextTransfers(ctx context.Context, lastBlock, lastLogIdx int64, limit int) ([]models.TransferRow, error) {
    rows, err := p.pool.Query(ctx, `
        SELECT token_address, block_timestamp, block_number, log_index
        FROM token_transfers
        WHERE (block_number > $1) OR (block_number = $1 AND log_index > $2)
        ORDER BY block_number ASC, log_index ASC
        LIMIT $3
    `, lastBlock, lastLogIdx, limit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var out []models.TransferRow
    for rows.Next() {
        var r models.TransferRow
        if err := rows.Scan(&r.TokenAddress, &r.BlockTimestamp, &r.BlockNumber, &r.LogIndex); err != nil {
            return nil, err
        }
        out = append(out, r)
    }
    return out, rows.Err()
}

func (p *Postgres) NextTransfersSafe(ctx context.Context, lastBlock, lastLogIdx int64, maxBlock int64, limit int) ([]models.TransferRow, error) {
    rows, err := p.pool.Query(ctx, `
        SELECT token_address, block_timestamp, block_number, log_index
        FROM token_transfers
        WHERE ((block_number > $1) OR (block_number = $1 AND log_index > $2))
          AND block_number <= $3
        ORDER BY block_number ASC, log_index ASC
        LIMIT $4
    `, lastBlock, lastLogIdx, maxBlock, limit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var out []models.TransferRow
    for rows.Next() {
        var r models.TransferRow
        if err := rows.Scan(&r.TokenAddress, &r.BlockTimestamp, &r.BlockNumber, &r.LogIndex); err != nil {
            return nil, err
        }
        out = append(out, r)
    }
    return out, rows.Err()
}

func (p *Postgres) UpsertHourly(ctx context.Context, token string, ts time.Time) error {
    hour := ts.Truncate(time.Hour)
    ct, err := p.pool.Exec(ctx, `
        INSERT INTO token_transfer_hourly (token_address, hour, txs_count)
        VALUES ($1, $2, 1)
        ON CONFLICT (token_address, hour)
        DO UPDATE SET txs_count = token_transfer_hourly.txs_count + 1
    `, token, hour)
    if err != nil {
        return err
    }
    if ct.RowsAffected() == 0 {
        return errors.New("no rows affected")
    }
    return nil
}

func (p *Postgres) UpsertHourlyBatch(ctx context.Context, tokens []string, hours []time.Time, counts []int64) error {
    if len(tokens) == 0 { return nil }
    // Build arrays for UNNEST
    // Note: pgx converts slices automatically.
    _, err := p.pool.Exec(ctx, `
        WITH upserts AS (
            SELECT unnest($1::varchar[]) AS token,
                   unnest($2::timestamp[]) AS hour,
                   unnest($3::bigint[]) AS cnt
        )
        INSERT INTO token_transfer_hourly (token_address, hour, txs_count)
        SELECT token, hour, SUM(cnt)
        FROM upserts
        GROUP BY token, hour
        ON CONFLICT (token_address, hour)
        DO UPDATE SET txs_count = token_transfer_hourly.txs_count + EXCLUDED.txs_count
    `, tokens, hours, counts)
    return err
}

func (p *Postgres) TopTokens8hExact(ctx context.Context, limit int) ([]models.TokenCount, time.Time, time.Time, error) {
    to := time.Now().UTC()
    from := to.Add(-8 * time.Hour)
    rows, err := p.pool.Query(ctx, `
        SELECT token_address, COUNT(*) AS txs_count
        FROM token_transfers
        WHERE block_timestamp >= $1 AND block_timestamp < $2
        GROUP BY token_address
        ORDER BY txs_count DESC
        LIMIT $3
    `, from, to, limit)
    if err != nil {
        return nil, from, to, err
    }
    defer rows.Close()
    var out []models.TokenCount
    for rows.Next() {
        var tc models.TokenCount
        if err := rows.Scan(&tc.TokenAddress, &tc.TxCount); err != nil {
            return nil, from, to, err
        }
        out = append(out, tc)
    }
    return out, from, to, rows.Err()
}

func (p *Postgres) TopTokens8hWithMeta(ctx context.Context, limit int) ([]models.TokenWithMeta, time.Time, time.Time, error) {
    // window anchored at 8h cursor hour
    var sec int64
    _ = p.pool.QueryRow(ctx, `SELECT last_block FROM worker_offsets WHERE id = $1`, t8hCursorID).Scan(&sec)
    to := time.Unix(sec, 0).UTC().Truncate(time.Hour)
    if to.IsZero() { to = time.Now().UTC().Truncate(time.Hour) }
    from := to.Add(-7 * time.Hour)

    rows, err := p.pool.Query(ctx, `
        SELECT c.token_address,
               COALESCE(m.name, '') AS name,
               COALESCE(m.symbol, '') AS symbol,
               COALESCE(m.decimals, 0) AS decimals,
               COALESCE(m.total_supply::text, '') AS total_supply,
               c.sum8 AS txs_count
        FROM token_8h_cache c
        LEFT JOIN token_metadata m ON m.token_address = c.token_address
        WHERE m.symbol IS NOT NULL AND m.symbol <> ''
        ORDER BY c.sum8 DESC
        LIMIT $1
    `, limit)
    if err != nil {
        return nil, from, to, err
    }
    defer rows.Close()
    var out []models.TokenWithMeta
    for rows.Next() {
        var it models.TokenWithMeta
        if err := rows.Scan(&it.TokenAddress, &it.Name, &it.Symbol, &it.Decimals, &it.TotalSupply, &it.TxCount); err != nil {
            return nil, from, to, err
        }
        out = append(out, it)
    }
    return out, from, to, rows.Err()
}

func (p *Postgres) HourlySeries(ctx context.Context, token string, from, to time.Time) ([]models.HourPoint, error) {
    // Instead of scanning arbitrary ranges, return last 8 points from cache
    rows, err := p.pool.Query(ctx, `
        WITH s AS (
            SELECT base_hour, counts FROM token_8h_cache WHERE token_address = $1
        )
        SELECT (s.base_hour - (($2 - i) * INTERVAL '1 hour'))::timestamptz AS hour,
               s.counts[i]::bigint AS txs_count
        FROM s, generate_subscripts((SELECT counts FROM s), 1) AS i
        ORDER BY hour ASC
    `, token, 8)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []models.HourPoint
    for rows.Next() {
        var hp models.HourPoint
        if err := rows.Scan(&hp.Hour, &hp.TxCount); err != nil { return nil, err }
        out = append(out, hp)
    }
    return out, rows.Err()
}

// UpsertTokenMetadata inserts or updates ERC20 metadata.
func (p *Postgres) UpsertTokenMetadata(ctx context.Context, md models.TokenMetadata) error {
    // Use NULL for unknown fields to avoid invalid casts (e.g., total_supply="").
    var (
        name sql.NullString
        symbol sql.NullString
        decimals sql.NullInt32
        totalSupply sql.NullString // numeric cast by PG from text; leave NULL if unknown
    )
    if md.Name != "" { name = sql.NullString{String: md.Name, Valid: true} }
    if md.Symbol != "" { symbol = sql.NullString{String: md.Symbol, Valid: true} }
    if md.Decimals > 0 { decimals = sql.NullInt32{Int32: md.Decimals, Valid: true} }
    if md.TotalSupply != "" { totalSupply = sql.NullString{String: md.TotalSupply, Valid: true} }

    _, err := p.pool.Exec(ctx, `
        INSERT INTO token_metadata (token_address, name, symbol, decimals, total_supply, updated_at)
        VALUES ($1, $2, $3, $4, CAST(NULLIF($5::text, '') AS numeric), NOW())
        ON CONFLICT (token_address)
        DO UPDATE SET name = COALESCE(EXCLUDED.name, token_metadata.name),
                      symbol = COALESCE(EXCLUDED.symbol, token_metadata.symbol),
                      decimals = COALESCE(EXCLUDED.decimals, token_metadata.decimals),
                      total_supply = COALESCE(EXCLUDED.total_supply, token_metadata.total_supply),
                      updated_at = NOW()
    `, md.TokenAddress, name, symbol, decimals, totalSupply)
    return err
}

func (p *Postgres) GetTokenMetadata(ctx context.Context, token string) (models.TokenMetadata, error) {
    var md models.TokenMetadata
    var totalSupply string
    err := p.pool.QueryRow(ctx, `
        SELECT token_address, COALESCE(name, ''), COALESCE(symbol, ''), COALESCE(decimals, 0),
               COALESCE(total_supply::text, ''), COALESCE(first_seen_block, 0), updated_at
        FROM token_metadata WHERE token_address = $1
    `, token).Scan(&md.TokenAddress, &md.Name, &md.Symbol, &md.Decimals, &totalSupply, &md.FirstSeenBlock, &md.UpdatedAt)
    if err != nil {
        return models.TokenMetadata{}, err
    }
    md.TotalSupply = totalSupply
    return md, nil
}

// MissingMetadataTokens returns distinct token addresses present in token_transfers but missing in token_metadata.
func (p *Postgres) MissingMetadataTokens(ctx context.Context, limit int) ([]string, error) {
    rows, err := p.pool.Query(ctx, `
        SELECT DISTINCT t.token_address
        FROM token_transfers t
        LEFT JOIN token_metadata m ON m.token_address = t.token_address
        WHERE m.token_address IS NULL
        ORDER BY t.token_address
        LIMIT $1
    `, limit)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []string
    for rows.Next() {
        var s string
        if err := rows.Scan(&s); err != nil { return nil, err }
        out = append(out, s)
    }
    return out, rows.Err()
}

func (p *Postgres) FirstSeenBlock(ctx context.Context, token string) (int64, error) {
    var n *int64
    err := p.pool.QueryRow(ctx, `SELECT MIN(block_number) FROM token_transfers WHERE token_address = $1`, token).Scan(&n)
    if err != nil { return 0, err }
    if n == nil { return 0, nil }
    return *n, nil
}

func (p *Postgres) EnsureTokenMetadataRow(ctx context.Context, token string, firstSeenBlock int64) error {
    _, err := p.pool.Exec(ctx, `
        INSERT INTO token_metadata (token_address, first_seen_block, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (token_address)
        DO UPDATE SET first_seen_block = LEAST(token_metadata.first_seen_block, EXCLUDED.first_seen_block)
    `, token, firstSeenBlock)
    return err
}

// 8-hour window state stored in worker_offsets with id 'analysis-worker:8h-hour'
const t8hCursorID = "analysis-worker:8h-hour"

func (p *Postgres) Get8hCursorHour(ctx context.Context) (time.Time, error) {
    var sec int64
    err := p.pool.QueryRow(ctx, `SELECT last_block FROM worker_offsets WHERE id = $1`, t8hCursorID).Scan(&sec)
    if err != nil {
        return time.Time{}, nil
    }
    return time.Unix(sec, 0).UTC().Truncate(time.Hour), nil
}

func (p *Postgres) Set8hCursorHour(ctx context.Context, hour time.Time) error {
    sec := hour.UTC().Truncate(time.Hour).Unix()
    _, err := p.pool.Exec(ctx, `
        INSERT INTO worker_offsets (id, last_block, last_log_index, updated_at)
        VALUES ($1, $2, 0, NOW())
        ON CONFLICT (id) DO UPDATE SET last_block = EXCLUDED.last_block, updated_at = NOW()
    `, t8hCursorID, sec)
    return err
}

func (p *Postgres) LatestSafeHour(ctx context.Context, safeBlock int64) (time.Time, error) {
    var ts time.Time
    err := p.pool.QueryRow(ctx, `
        SELECT date_trunc('hour', MAX(timestamp))
        FROM blocks WHERE number <= $1
    `, safeBlock).Scan(&ts)
    if err != nil { return time.Time{}, err }
    return ts.UTC(), nil
}

func (p *Postgres) Add8hHour(ctx context.Context, hour time.Time) error {
    _, err := p.pool.Exec(ctx, `
        INSERT INTO token_transfer_8hour (token_address, txs_count, updated_at)
        SELECT token_address, txs_count, NOW()
        FROM token_transfer_hourly
        WHERE hour = $1
        ON CONFLICT (token_address)
        DO UPDATE SET txs_count = token_transfer_8hour.txs_count + EXCLUDED.txs_count,
                      updated_at = NOW()
    `, hour.UTC().Truncate(time.Hour))
    return err
}

func (p *Postgres) Sub8hHour(ctx context.Context, hour time.Time) error {
    _, err := p.pool.Exec(ctx, `
        UPDATE token_transfer_8hour t8
        SET txs_count = GREATEST(0, t8.txs_count - h.txs_count),
            updated_at = NOW()
        FROM token_transfer_hourly h
        WHERE h.hour = $1 AND h.token_address = t8.token_address
    `, hour.UTC().Truncate(time.Hour))
    if err != nil { return err }
    // optional cleanup
    _, err = p.pool.Exec(ctx, `DELETE FROM token_transfer_8hour WHERE txs_count = 0`)
    return err
}

// legacy 8h tables removed; using token_8h_cache instead

// 8h cache (single-row-per-token with ring buffer)
func (p *Postgres) Rebuild8hCacheAtHour(ctx context.Context, hour time.Time) error {
    h := hour.UTC().Truncate(time.Hour)
    tx, err := p.pool.Begin(ctx)
    if err != nil { return err }
    defer func(){ _ = tx.Rollback(ctx) }()
    if _, err := tx.Exec(ctx, `DELETE FROM token_8h_cache`); err != nil { return err }
    // Build per-token 8-hour arrays and sums
    _, err = tx.Exec(ctx, `
        WITH win AS (
            SELECT generate_series($1::timestamptz - INTERVAL '7 hours', $1::timestamptz, INTERVAL '1 hour') AS hour
        ), tok AS (
            SELECT DISTINCT token_address FROM token_transfer_hourly
            WHERE hour BETWEEN ($1::timestamptz - INTERVAL '7 hours') AND ($1::timestamptz)
        ), grid AS (
            SELECT t.token_address, w.hour FROM tok t CROSS JOIN win w
        ), vals AS (
            SELECT g.token_address, g.hour, COALESCE(h.txs_count,0)::integer AS txs_count
            FROM grid g LEFT JOIN token_transfer_hourly h
            ON h.token_address = g.token_address AND h.hour = g.hour
        )
        INSERT INTO token_8h_cache (token_address, base_hour, counts, sum8, updated_at)
        SELECT token_address,
               $1::timestamptz AS base_hour,
               ARRAY_AGG(txs_count ORDER BY hour)::integer[] AS counts,
               SUM(txs_count)::bigint AS sum8,
               NOW()
        FROM vals
        GROUP BY token_address
    `, h)
    if err != nil { return err }
    return tx.Commit(ctx)
}

func (p *Postgres) Rotate8hCacheToHour(ctx context.Context, hour time.Time) error {
    h := hour.UTC().Truncate(time.Hour)
    tx, err := p.pool.Begin(ctx)
    if err != nil { return err }
    defer func(){ _ = tx.Rollback(ctx) }()
    // 1) rotate only rows behind target hour (idempotent for same hour)
    if _, err := tx.Exec(ctx, `
        UPDATE token_8h_cache
        SET base_hour = $1::timestamptz,
            counts = counts[2:8] || 0,
            sum8 = sum8 - counts[1],
            updated_at = NOW()
        WHERE base_hour < $1::timestamptz
    `, h); err != nil { return err }
    // 2) upsert current hour values for existing tokens: replace last slot and fix sum
    if _, err := tx.Exec(ctx, `
        UPDATE token_8h_cache c
        SET counts[8] = h.txs_count::integer,
            sum8 = (c.sum8 - COALESCE(c.counts[8],0)::bigint) + h.txs_count::bigint,
            updated_at = NOW()
        FROM token_transfer_hourly h
        WHERE h.hour = $1::timestamptz AND c.token_address = h.token_address
    `, h); err != nil { return err }
    // 3) insert rows for tokens not yet in cache
    if _, err := tx.Exec(ctx, `
        INSERT INTO token_8h_cache (token_address, base_hour, counts, sum8, updated_at)
        SELECT h.token_address, $1::timestamptz,
               ARRAY[0,0,0,0,0,0,0, h.txs_count::integer]::integer[],
               h.txs_count::bigint,
               NOW()
        FROM token_transfer_hourly h
        LEFT JOIN token_8h_cache c ON c.token_address = h.token_address
        WHERE h.hour = $1::timestamptz AND c.token_address IS NULL
    `, h); err != nil { return err }
    return tx.Commit(ctx)
}

func (p *Postgres) Has8hCacheData(ctx context.Context) (bool, error) {
    var exists bool
    err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM token_8h_cache LIMIT 1)`).Scan(&exists)
    return exists, err
}

// legacy points helpers removed

func (p *Postgres) Series8h(ctx context.Context, token string) ([]models.HourPoint, error) {
    rows, err := p.pool.Query(ctx, `
        WITH s AS (
            SELECT base_hour, counts FROM token_8h_cache WHERE token_address = $1
        )
        SELECT (s.base_hour - (($2 - i) * INTERVAL '1 hour'))::timestamptz AS hour,
               s.counts[i]::bigint AS txs_count
        FROM s, generate_subscripts((SELECT counts FROM s), 1) AS i
        ORDER BY hour ASC
    `, token, 8)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []models.HourPoint
    for rows.Next() {
        var hp models.HourPoint
        if err := rows.Scan(&hp.Hour, &hp.TxCount); err != nil { return nil, err }
        out = append(out, hp)
    }
    return out, rows.Err()
}

// legacy summary-from-points helpers removed

// AggregateHourlyNextBatch groups the next N safe rows server-side and upserts hourly counts, returning the new checkpoint and processed row count.
func (p *Postgres) AggregateHourlyNextBatch(ctx context.Context, lastBlock, lastLogIdx, maxBlock int64, limit int) (int64, int64, int64, error) {
    var newB, newIdx, processed int64
    tx, err := p.pool.Begin(ctx)
    if err != nil { return 0, 0, 0, err }
    defer func() { _ = tx.Rollback(ctx) }()
    // increase work_mem for better GROUP BY performance on large batches
    if _, err := tx.Exec(ctx, `SET LOCAL work_mem = '128MB'`); err != nil {
        return 0, 0, 0, err
    }
    // since aggregates are reproducible, we can relax durability for speed
    if _, err := tx.Exec(ctx, `SET LOCAL synchronous_commit = 'off'`); err != nil {
        return 0, 0, 0, err
    }
    err = tx.QueryRow(ctx, `
        WITH next AS (
            SELECT token_address, block_timestamp, block_number, log_index
            FROM token_transfers
            WHERE ((block_number > $1) OR (block_number = $1 AND log_index > $2))
              AND block_number <= $3
            ORDER BY block_number ASC, log_index ASC
            LIMIT $4
        ), last AS (
            SELECT block_number, log_index
            FROM next
            ORDER BY block_number DESC, log_index DESC
            LIMIT 1
        ), agg AS (
            SELECT token_address, date_trunc('hour', block_timestamp) AS hour, COUNT(*) AS cnt
            FROM next
            GROUP BY token_address, date_trunc('hour', block_timestamp)
        ), ins AS (
            INSERT INTO token_transfer_hourly (token_address, hour, txs_count)
            SELECT token_address, hour, cnt FROM agg
            ON CONFLICT (token_address, hour)
            DO UPDATE SET txs_count = token_transfer_hourly.txs_count + EXCLUDED.txs_count
            RETURNING 1
        )
        SELECT COALESCE((SELECT block_number FROM last), $1) AS new_block,
               COALESCE((SELECT log_index FROM last), $2) AS new_log,
               (SELECT COUNT(*) FROM next) AS processed
    `, lastBlock, lastLogIdx, maxBlock, limit).Scan(&newB, &newIdx, &processed)
    if err != nil { return 0, 0, 0, err }
    if err := tx.Commit(ctx); err != nil { return 0, 0, 0, err }
    return newB, newIdx, processed, nil
}

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
        SELECT t8.token_address,
               COALESCE(m.name, '') AS name,
               COALESCE(m.symbol, '') AS symbol,
               COALESCE(m.decimals, 0) AS decimals,
               COALESCE(m.total_supply::text, '') AS total_supply,
               t8.txs_count AS txs_count
        FROM token_transfer_8hour t8
        LEFT JOIN token_metadata m ON m.token_address = t8.token_address
        WHERE m.symbol IS NOT NULL AND m.symbol <> ''
        ORDER BY t8.txs_count DESC
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
    f := from.Truncate(time.Hour)
    t := to.Truncate(time.Hour)
    rows, err := p.pool.Query(ctx, `
        SELECT hour, txs_count
        FROM token_transfer_hourly
        WHERE token_address = $1
          AND hour >= $2 AND hour <= $3
        ORDER BY hour ASC
    `, token, f, t)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var out []models.HourPoint
    for rows.Next() {
        var hp models.HourPoint
        if err := rows.Scan(&hp.Hour, &hp.TxCount); err != nil {
            return nil, err
        }
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

func (p *Postgres) Rebuild8hAtHour(ctx context.Context, hour time.Time) error {
    h := hour.UTC().Truncate(time.Hour)
    _, err := p.pool.Exec(ctx, `DELETE FROM token_transfer_8hour`)
    if err != nil { return err }
    _, err = p.pool.Exec(ctx, `
        INSERT INTO token_transfer_8hour (token_address, txs_count, updated_at)
        SELECT token_address, SUM(txs_count) AS s, NOW()
        FROM token_transfer_hourly
        WHERE hour >= $1 - INTERVAL '7 hours' AND hour <= $1
        GROUP BY token_address
    `, h)
    return err
}

func (p *Postgres) Add8hPointsHour(ctx context.Context, hour time.Time) error {
    h := hour.UTC().Truncate(time.Hour)
    _, err := p.pool.Exec(ctx, `
        INSERT INTO token_transfer_8hour_points (token_address, hour, txs_count)
        SELECT token_address, hour, txs_count FROM token_transfer_hourly WHERE hour = $1
        ON CONFLICT (token_address, hour)
        DO UPDATE SET txs_count = EXCLUDED.txs_count
    `, h)
    return err
}

func (p *Postgres) Sub8hPointsHour(ctx context.Context, hour time.Time) error {
    h := hour.UTC().Truncate(time.Hour)
    _, err := p.pool.Exec(ctx, `DELETE FROM token_transfer_8hour_points WHERE hour = $1`, h)
    return err
}

func (p *Postgres) Series8h(ctx context.Context, token string) ([]models.HourPoint, error) {
    rows, err := p.pool.Query(ctx, `
        SELECT hour, txs_count
        FROM token_transfer_8hour_points
        WHERE token_address = $1
        ORDER BY hour ASC
    `, token)
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

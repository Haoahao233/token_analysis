package store

import (
    "context"
    "time"

    "github.com/example/tx-analytics/internal/models"
)

type Store interface {
    GetCheckpoint(ctx context.Context, workerID string) (lastBlock int64, lastLogIdx int64, err error)
    SetCheckpoint(ctx context.Context, workerID string, lastBlock int64, lastLogIdx int64) error
    LatestBlockNumber(ctx context.Context) (int64, error)
    LatestProcessedHour(ctx context.Context) (time.Time, error)
    NextTransfers(ctx context.Context, lastBlock, lastLogIdx int64, limit int) ([]models.TransferRow, error)
    NextTransfersSafe(ctx context.Context, lastBlock, lastLogIdx int64, maxBlock int64, limit int) ([]models.TransferRow, error)
    UpsertHourly(ctx context.Context, token string, ts time.Time) error
    UpsertHourlyBatch(ctx context.Context, tokens []string, hours []time.Time, counts []int64) error
    TopTokens8hExact(ctx context.Context, limit int) (items []models.TokenCount, winFrom, winTo time.Time, err error)
    TopTokens8hWithMeta(ctx context.Context, limit int) (items []models.TokenWithMeta, winFrom, winTo time.Time, err error)
    HourlySeries(ctx context.Context, token string, from, to time.Time) ([]models.HourPoint, error)

    // Metadata
    UpsertTokenMetadata(ctx context.Context, md models.TokenMetadata) error
    GetTokenMetadata(ctx context.Context, token string) (models.TokenMetadata, error)
    MissingMetadataTokens(ctx context.Context, limit int) ([]string, error)
    FirstSeenBlock(ctx context.Context, token string) (int64, error)

    // Ensure a token_metadata row exists and update first_seen_block with LEAST(existing, provided)
    EnsureTokenMetadataRow(ctx context.Context, token string, firstSeenBlock int64) error

    // 8-hour cache window maintenance
    Get8hCursorHour(ctx context.Context) (time.Time, error)
    Set8hCursorHour(ctx context.Context, hour time.Time) error
    LatestSafeHour(ctx context.Context, safeBlock int64) (time.Time, error)
    Rebuild8hCacheAtHour(ctx context.Context, hour time.Time) error
    Rotate8hCacheToHour(ctx context.Context, hour time.Time) error
    Has8hCacheData(ctx context.Context) (bool, error)

    // 8h series query (from cache)
    Series8h(ctx context.Context, token string) ([]models.HourPoint, error)

    // Server-side aggregation of next batch to hourly buckets; returns new checkpoint and processed row count
    AggregateHourlyNextBatch(ctx context.Context, lastBlock, lastLogIdx, maxBlock int64, limit int) (newLastBlock, newLastLogIdx int64, processed int64, err error)

}

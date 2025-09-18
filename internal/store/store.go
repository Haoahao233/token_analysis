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
    NextTransfers(ctx context.Context, lastBlock, lastLogIdx int64, limit int) ([]models.TransferRow, error)
    UpsertHourly(ctx context.Context, token string, ts time.Time) error
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
}

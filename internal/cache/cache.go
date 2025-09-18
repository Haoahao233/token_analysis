package cache

import (
    "context"
    "time"

    "github.com/example/tx-analytics/internal/models"
)

type Cache interface {
    GetLeaderboard8h(ctx context.Context, limit int) (winFrom, winTo time.Time, items []models.TokenCount, ok bool, err error)
    SetLeaderboard8h(ctx context.Context, winFrom, winTo time.Time, items []models.TokenCount) error

    GetLeaderboard8hWithMeta(ctx context.Context, limit int) (winFrom, winTo time.Time, items []models.TokenWithMeta, ok bool, err error)
    SetLeaderboard8hWithMeta(ctx context.Context, winFrom, winTo time.Time, items []models.TokenWithMeta) error
}

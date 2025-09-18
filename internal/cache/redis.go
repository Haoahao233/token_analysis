package cache

import (
    "context"
    "strconv"
    "time"

    "github.com/example/tx-analytics/internal/models"
    "github.com/redis/go-redis/v9"
)

type Redis struct {
    cli *redis.Client
}

func NewRedis(addr string, db int) *Redis {
    cli := redis.NewClient(&redis.Options{Addr: addr, DB: db})
    return &Redis{cli: cli}
}

func (r *Redis) Close() error { return r.cli.Close() }

const (
    lbKey = "leaderboard:8h"
    lbFromKey = "leaderboard:8h:from"
    lbToKey = "leaderboard:8h:to"
)

func (r *Redis) GetLeaderboard8h(ctx context.Context, limit int) (time.Time, time.Time, []models.TokenCount, bool, error) {
    // Read window metadata
    fromStr, err1 := r.cli.Get(ctx, lbFromKey).Result()
    toStr, err2 := r.cli.Get(ctx, lbToKey).Result()
    if err1 != nil || err2 != nil {
        return time.Time{}, time.Time{}, nil, false, nil
    }
    fromUnix, _ := strconv.ParseInt(fromStr, 10, 64)
    toUnix, _ := strconv.ParseInt(toStr, 10, 64)
    from := time.Unix(fromUnix, 0).UTC()
    to := time.Unix(toUnix, 0).UTC()

    z, err := r.cli.ZRevRangeWithScores(ctx, lbKey, 0, int64(limit-1)).Result()
    if err != nil {
        return time.Time{}, time.Time{}, nil, false, nil
    }
    items := make([]models.TokenCount, 0, len(z))
    for _, m := range z {
        addr, _ := m.Member.(string)
        items = append(items, models.TokenCount{TokenAddress: addr, TxCount: int64(m.Score)})
    }
    if len(items) == 0 {
        return time.Time{}, time.Time{}, nil, false, nil
    }
    return from, to, items, true, nil
}

func (r *Redis) SetLeaderboard8h(ctx context.Context, winFrom, winTo time.Time, items []models.TokenCount) error {
    pipe := r.cli.TxPipeline()
    pipe.Del(ctx, lbKey)
    if len(items) > 0 {
        zs := make([]redis.Z, 0, len(items))
        for _, it := range items {
            zs = append(zs, redis.Z{Member: it.TokenAddress, Score: float64(it.TxCount)})
        }
        pipe.ZAdd(ctx, lbKey, zs...)
    }
    pipe.Set(ctx, lbFromKey, strconv.FormatInt(winFrom.Unix(), 10), 0)
    pipe.Set(ctx, lbToKey, strconv.FormatInt(winTo.Unix(), 10), 0)
    // keep fresh for 10 minutes
    pipe.Expire(ctx, lbKey, 10*time.Minute)
    pipe.Expire(ctx, lbFromKey, 10*time.Minute)
    pipe.Expire(ctx, lbToKey, 10*time.Minute)
    _, err := pipe.Exec(ctx)
    return err
}


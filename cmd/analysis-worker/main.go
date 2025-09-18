package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"
    "time"

    "github.com/example/tx-analytics/internal/aggregator"
    "github.com/example/tx-analytics/internal/cache"
    "github.com/example/tx-analytics/internal/config"
    "github.com/example/tx-analytics/internal/store"
)

func main() {
    cfg := config.LoadWorker()
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    pg, err := store.NewPostgres(ctx, cfg.PgDSN)
    if err != nil { log.Fatalf("postgres: %v", err) }
    defer pg.Close()

    rds := cache.NewRedis(cfg.RedisAddr, cfg.RedisDB)
    defer rds.Close()

    w := &aggregator.Worker{
        Store: pg,
        Cache: rds,
        WorkerID: "analysis-worker:token_transfers",
        BatchSize: cfg.BatchSize,
        ReorgDepth: cfg.ReorgDepth,
        IdleDelay: cfg.IdleDelay,
        LeaderboardInterval: cfg.LeaderboardInterval,
        LeaderboardSize: cfg.LeaderboardSize,
        ProgressInterval: cfg.ProgressLogInterval,
    }


    if err := w.Run(ctx); err != nil && err != context.Canceled {
        log.Printf("worker stopped: %v", err)
    }
    // Give logs time to flush in some environments
    time.Sleep(100 * time.Millisecond)
}

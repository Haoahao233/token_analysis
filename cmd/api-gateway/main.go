package main

import (
    "context"
    "log"
    "net/http"
    "os/signal"
    "syscall"

    "github.com/example/tx-analytics/internal/api"
    "github.com/example/tx-analytics/internal/cache"
    "github.com/example/tx-analytics/internal/config"
    "github.com/example/tx-analytics/internal/store"
)

func main() {
    cfg := config.LoadAPI()
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    pg, err := store.NewPostgres(ctx, cfg.PgDSN)
    if err != nil { log.Fatalf("postgres: %v", err) }
    defer pg.Close()

    rds := cache.NewRedis(cfg.RedisAddr, cfg.RedisDB)
    defer rds.Close()

    h := &api.Handler{Store: pg, Cache: rds, LeaderboardSize: cfg.LeaderboardSize}
    mux := http.NewServeMux()
    h.Routes(mux)

    srv := &http.Server{Addr: cfg.Addr, Handler: mux}

    go func() {
        <-ctx.Done()
        _ = srv.Shutdown(context.Background())
    }()

    log.Printf("api listening on %s", cfg.Addr)
    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Fatalf("server error: %v", err)
    }
}


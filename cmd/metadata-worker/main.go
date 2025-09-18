package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/example/tx-analytics/internal/metadata"
    "github.com/example/tx-analytics/internal/store"
    "github.com/example/tx-analytics/internal/eth"
)

func getenv(key, def string) string { v := os.Getenv(key); if v != "" { return v }; return def }

func main() {
    rpcURL := getenv("RPC_URL", "http://127.0.0.1:8545")
    pgDSN := getenv("PG_DSN", "postgres://user:password@localhost:5432/eth?sslmode=disable")
    batch := 100

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    pg, err := store.NewPostgres(ctx, pgDSN)
    if err != nil { log.Fatalf("postgres: %v", err) }
    defer pg.Close()

    erc20 := eth.NewERC20Client(rpcURL)
    w := &metadata.Worker{Store: pg, ERC20: erc20, Batch: batch, IdleDelay: 5 * time.Second}
    if err := w.Run(ctx); err != nil && err != context.Canceled {
        log.Printf("metadata worker stopped: %v", err)
    }
}


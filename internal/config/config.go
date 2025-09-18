package config

import (
    "os"
    "strconv"
    "time"
)

func getenv(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}

func getenvInt(key string, def int) int {
    if v := os.Getenv(key); v != "" {
        if n, err := strconv.Atoi(v); err == nil {
            return n
        }
    }
    return def
}

func getenvInt64(key string, def int64) int64 {
    if v := os.Getenv(key); v != "" {
        if n, err := strconv.ParseInt(v, 10, 64); err == nil {
            return n
        }
    }
    return def
}

func getenvDur(key string, def time.Duration) time.Duration {
    if v := os.Getenv(key); v != "" {
        if d, err := time.ParseDuration(v); err == nil {
            return d
        }
    }
    return def
}

type Common struct {
    PgDSN string
    RedisAddr string
    RedisDB int
}

func LoadCommon() Common {
    return Common{
        PgDSN:     getenv("PG_DSN", "postgres://user:password@localhost:5432/eth?sslmode=disable"),
        RedisAddr: getenv("REDIS_ADDR", "127.0.0.1:6379"),
        RedisDB:   getenvInt("REDIS_DB", 0),
    }
}

type Worker struct {
    Common
    BatchSize int
    ReorgDepth int64
    IdleDelay time.Duration
    LeaderboardInterval time.Duration
    LeaderboardSize int
    RPCURL string
    MetadataRetryInterval time.Duration
    ProgressLogInterval time.Duration
}

func LoadWorker() Worker {
    c := LoadCommon()
    return Worker{
        Common: c,
        BatchSize: getenvInt("BATCH_SIZE", 2000),
        ReorgDepth: getenvInt64("REORG_DEPTH", 12),
        IdleDelay: getenvDur("IDLE_DELAY", 3*time.Second),
        LeaderboardInterval: getenvDur("LEADERBOARD_INTERVAL", 60*time.Second),
        LeaderboardSize: getenvInt("LEADERBOARD_SIZE", 100),
        RPCURL: getenv("RPC_URL", "http://127.0.0.1:8545"),
        MetadataRetryInterval: getenvDur("METADATA_RETRY_INTERVAL", time.Hour),
        ProgressLogInterval: getenvDur("PROGRESS_LOG_INTERVAL", 15*time.Second),
    }
}

type API struct {
    Common
    Addr string
    LeaderboardSize int
}

func LoadAPI() API {
    c := LoadCommon()
    return API{
        Common: c,
        Addr: getenv("API_ADDR", ":8080"),
        LeaderboardSize: getenvInt("LEADERBOARD_SIZE", 100),
    }
}

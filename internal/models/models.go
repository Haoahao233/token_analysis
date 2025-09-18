package models

import "time"

type TransferRow struct {
    TokenAddress   string
    BlockTimestamp time.Time
    BlockNumber    int64
    LogIndex       int64
}

type TokenCount struct {
    TokenAddress string `json:"token_address"`
    TxCount      int64  `json:"txs_count"`
}

// TokenWithMeta extends leaderboard item with metadata.
type TokenWithMeta struct {
    TokenAddress string `json:"token_address"`
    Name         string `json:"name"`
    Symbol       string `json:"symbol"`
    Decimals     int32  `json:"decimals"`
    TotalSupply  string `json:"total_supply"`
    TxCount      int64  `json:"txs_count"`
}

type HourPoint struct {
    Hour    time.Time `json:"hour"`
    TxCount int64     `json:"txs_count"`
}

type Window struct {
    From time.Time `json:"from"`
    To   time.Time `json:"to"`
}

// TokenMetadata represents ERC20 static-ish fields.
type TokenMetadata struct {
    TokenAddress string `json:"token_address"`
    Name         string `json:"name"`
    Symbol       string `json:"symbol"`
    Decimals     int32  `json:"decimals"`
    // total_supply can be very large; represent as string in JSON
    TotalSupply  string `json:"total_supply"`
    FirstSeenBlock int64 `json:"first_seen_block"`
    UpdatedAt    time.Time `json:"updated_at"`
}

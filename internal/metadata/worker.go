package metadata

import (
    "context"
    "log"
    "math/big"
    "strings"
    "time"

    "github.com/example/tx-analytics/internal/eth"
    "github.com/example/tx-analytics/internal/models"
    "github.com/example/tx-analytics/internal/store"
)

type Worker struct {
    Store store.Store
    ERC20 *eth.ERC20Client
    Batch int
    IdleDelay time.Duration
}

func (w *Worker) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        tokens, err := w.Store.MissingMetadataTokens(ctx, w.Batch)
        if err != nil {
            log.Printf("MissingMetadataTokens error: %v", err)
            time.Sleep(w.IdleDelay)
            continue
        }
        if len(tokens) == 0 {
            time.Sleep(w.IdleDelay)
            continue
        }
        for _, t := range tokens {
            md := w.fetchOne(ctx, t)
            if fs, err := w.Store.FirstSeenBlock(ctx, t); err == nil {
                md.FirstSeenBlock = fs
            }
            if err := w.Store.UpsertTokenMetadata(ctx, md); err != nil {
                log.Printf("UpsertTokenMetadata %s error: %v", t, err)
            }
        }
    }
}

func (w *Worker) fetchOne(ctx context.Context, token string) models.TokenMetadata {
    // Normalize address to lower-case
    token = strings.ToLower(token)
    var name, symbol string
    var dec uint8
    var supply *big.Int
    var err error

    // Use short contexts to avoid long hangs
    cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
    defer cancel()

    if name, err = w.ERC20.Name(cctx, token); err != nil { name = "" }
    if symbol, err = w.ERC20.Symbol(cctx, token); err != nil { symbol = "" }
    if dec, err = w.ERC20.Decimals(cctx, token); err != nil { dec = 0 }
    if supply, err = w.ERC20.TotalSupply(cctx, token); err != nil { supply = big.NewInt(0) }

    return models.TokenMetadata{
        TokenAddress: token,
        Name: name,
        Symbol: symbol,
        Decimals: int32(dec),
        TotalSupply: supply.String(),
        FirstSeenBlock: 0,
        UpdatedAt: time.Now().UTC(),
    }
}

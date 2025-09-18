package aggregator

import (
    "context"
    "log"
    "time"

    "github.com/example/tx-analytics/internal/cache"
    "github.com/example/tx-analytics/internal/eth"
    "github.com/example/tx-analytics/internal/models"
    "github.com/example/tx-analytics/internal/store"
)

type Worker struct {
    Store  store.Store
    Cache  cache.Cache
    WorkerID string

    BatchSize int
    ReorgDepth int64
    IdleDelay time.Duration
    LeaderboardInterval time.Duration
    LeaderboardSize int

    ERC20 *eth.ERC20Client

    // avoid hammering problematic tokens repeatedly
    MetaRetryInterval time.Duration
    lastMetaAttempt   map[string]time.Time

    // progress logging
    ProgressInterval time.Duration
}

func (w *Worker) Run(ctx context.Context) error {
    log.Printf("worker starting: reorgDepth=%d batchSize=%d", w.ReorgDepth, w.BatchSize)
    ticker := time.NewTicker(w.LeaderboardInterval)
    defer ticker.Stop()
    progTicker := time.NewTicker(w.ProgressInterval)
    defer progTicker.Stop()
    if w.lastMetaAttempt == nil { w.lastMetaAttempt = make(map[string]time.Time) }
    started := time.Now()
    var totalProcessed int64
    var processedSinceReport int64
    var lastCpBlock int64
    var lastReportAt time.Time
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        latest, err := w.Store.LatestBlockNumber(ctx)
        if err != nil {
            log.Printf("LatestBlockNumber error: %v", err)
            time.Sleep(w.IdleDelay)
            continue
        }
        safe := latest - w.ReorgDepth
        lastB, lastIdx, err := w.Store.GetCheckpoint(ctx, w.WorkerID)
        if err != nil {
            log.Printf("GetCheckpoint error: %v", err)
            time.Sleep(w.IdleDelay)
            continue
        }

        // No safe blocks to finalize yet
        if lastB >= safe {
            select {
            case <-ticker.C:
                w.refreshLeaderboard(ctx)
            default:
            }
            time.Sleep(w.IdleDelay)
            continue
        }

        rows, err := w.Store.NextTransfers(ctx, lastB, lastIdx, w.BatchSize)
        if err != nil {
            log.Printf("NextTransfers error: %v", err)
            time.Sleep(w.IdleDelay)
            continue
        }
        if len(rows) == 0 {
            select {
            case <-ticker.C:
                w.refreshLeaderboard(ctx)
            default:
            }
            time.Sleep(w.IdleDelay)
            continue
        }

        var maxB, maxIdx int64
        // track minimal first seen block per token in this batch
        minBlockByToken := map[string]int64{}
        for _, r := range rows {
            if r.BlockNumber > safe { // don't finalize unsafe
                break
            }
            if err := w.Store.UpsertHourly(ctx, r.TokenAddress, r.BlockTimestamp); err != nil {
                log.Printf("UpsertHourly error: %v", err)
                break
            }
            if mb, ok := minBlockByToken[r.TokenAddress]; !ok || r.BlockNumber < mb {
                minBlockByToken[r.TokenAddress] = r.BlockNumber
            }
            maxB, maxIdx = r.BlockNumber, r.LogIndex
            totalProcessed++
            processedSinceReport++
        }
        if maxB > 0 {
            if err := w.Store.SetCheckpoint(ctx, w.WorkerID, maxB, maxIdx); err != nil {
                log.Printf("SetCheckpoint error: %v", err)
            }
        }

        // ensure token_metadata rows exist and update first_seen_block conservatively
        for token, fsb := range minBlockByToken {
            if err := w.Store.EnsureTokenMetadataRow(ctx, token, fsb); err != nil {
                log.Printf("EnsureTokenMetadataRow %s error: %v", token, err)
            }
        }

        if w.ERC20 != nil {
            for token := range minBlockByToken {
                // backoff repeated failures
                if t, ok := w.lastMetaAttempt[token]; ok && time.Since(t) < w.MetaRetryInterval {
                    continue
                }
                w.lastMetaAttempt[token] = time.Now()

                // preflight: ensure it's a contract address
                cctx, cancelCode := context.WithTimeout(ctx, 3*time.Second)
                code, errCode := w.ERC20.GetCode(cctx, token)
                cancelCode()
                if errCode == nil && (code == "0x" || code == "0x0") {
                    continue
                }
                // read current metadata; if missing name/symbol/decimals, try to fetch
                md, err := w.Store.GetTokenMetadata(ctx, token)
                if err != nil || md.Name == "" || md.Symbol == "" || md.Decimals == 0 || md.TotalSupply == "" {
                    // short timeout per token
                    cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
                    name, sym, tsStr := md.Name, md.Symbol, md.TotalSupply
                    if name == "" { if v, err := w.ERC20.Name(cctx, token); err == nil { name = v } }
                    if sym == "" { if v, err := w.ERC20.Symbol(cctx, token); err == nil { sym = v } }
                    var dec uint8
                    if md.Decimals == 0 { if v, err := w.ERC20.Decimals(cctx, token); err == nil { dec = v } else { dec = 0 } } else { dec = uint8(md.Decimals) }
                    if tsStr == "" { if v, err := w.ERC20.TotalSupply(cctx, token); err == nil { tsStr = v.String() } }
                    cancel()
                    upd := models.TokenMetadata{
                        TokenAddress: token,
                        Name: name,
                        Symbol: sym,
                        Decimals: int32(dec),
                        TotalSupply: tsStr,
                        FirstSeenBlock: md.FirstSeenBlock,
                        UpdatedAt: time.Now().UTC(),
                    }
                    // Only upsert if at least one field is non-empty
                    if name != "" || sym != "" || dec != 0 || tsStr != "" {
                        if err := w.Store.UpsertTokenMetadata(ctx, upd); err != nil {
                            log.Printf("UpsertTokenMetadata %s error: %v", token, err)
                        }
                    }
                }
            }
        }

        select {
        case <-ticker.C:
            w.refreshLeaderboard(ctx)
        case <-progTicker.C:
            // progress report: checkpoint vs safe/latest, lag, throughput
            cpB, cpIdx, err := w.Store.GetCheckpoint(ctx, w.WorkerID)
            if err != nil {
                log.Printf("progress: checkpoint err=%v", err)
                continue
            }
            latestNow, _ := w.Store.LatestBlockNumber(ctx)
            safeNow := latestNow - w.ReorgDepth
            lag := safeNow - cpB
            if lag < 0 { lag = 0 }
            elapsed := time.Since(started).Seconds()
            rpsTotal := float64(0)
            if elapsed > 0 { rpsTotal = float64(totalProcessed)/elapsed }
            rpsRecent := float64(processedSinceReport) / w.ProgressInterval.Seconds()
            // recent block progress & ETA
            now := time.Now()
            if lastReportAt.IsZero() {
                lastReportAt = now
                lastCpBlock = cpB
            }
            dt := now.Sub(lastReportAt).Seconds()
            cpDelta := cpB - lastCpBlock
            bpsRecent := float64(0)
            if dt > 0 { bpsRecent = float64(cpDelta) / dt }
            epbRecent := float64(0)
            if cpDelta > 0 { epbRecent = float64(processedSinceReport) / float64(cpDelta) }
            eta := "n/a"
            if bpsRecent > 0 {
                sec := float64(lag) / bpsRecent
                eta = (time.Duration(sec) * time.Second).Truncate(time.Second).String()
            }
            log.Printf("progress: cp=%d:%d safe=%d latest=%d lag=%d rows_total=%d rps_total=%.1f rps_recent=%.1f bps_recent=%.2f epb_recent=%.1f eta_safe=%s", cpB, cpIdx, safeNow, latestNow, lag, totalProcessed, rpsTotal, rpsRecent, bpsRecent, epbRecent, eta)
            lastReportAt = now
            lastCpBlock = cpB
            processedSinceReport = 0
        default:
        }
    }
}

func (w *Worker) refreshLeaderboard(ctx context.Context) {
    if w.Cache == nil { return }
    items, from, to, err := func() ([]models.TokenCount, time.Time, time.Time, error) {
        it, f, t, err := w.Store.TopTokens8hExact(ctx, w.LeaderboardSize)
        if err != nil {
            return nil, time.Time{}, time.Time{}, err
        }
        return it, f, t, nil
    }()
    if err != nil {
        log.Printf("TopTokens8hExact error: %v", err)
        return
    }
    if err := w.Cache.SetLeaderboard8h(ctx, from, to, items); err != nil {
        log.Printf("SetLeaderboard8h error: %v", err)
    }
}

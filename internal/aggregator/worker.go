package aggregator

import (
	"context"
	"log"
	"time"

	"github.com/example/tx-analytics/internal/cache"
	"github.com/example/tx-analytics/internal/eth"
	"github.com/example/tx-analytics/internal/store"
)

type Worker struct {
	Store    store.Store
	WorkerID string
	Cache    cache.Cache

	BatchSize           int
	ReorgDepth          int64
	IdleDelay           time.Duration
	LeaderboardInterval time.Duration
	LeaderboardSize     int

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
	if w.lastMetaAttempt == nil {
		w.lastMetaAttempt = make(map[string]time.Time)
	}
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
				w.roll8h(ctx)
			default:
			}
			time.Sleep(w.IdleDelay)
			continue
		}

		t0 := time.Now()
		newB, newIdx, processed, err := w.Store.AggregateHourlyNextBatch(ctx, lastB, lastIdx, safe, w.BatchSize)
		if err != nil {
			log.Printf("AggregateHourlyNextBatch error: %v", err)
			time.Sleep(w.IdleDelay)
			continue
		}
		pullMs := time.Since(t0).Milliseconds()
		if processed == 0 {
			select {
			case <-ticker.C:
				w.roll8h(ctx)
			default:
			}
			time.Sleep(w.IdleDelay)
			continue
		}
		log.Printf("db-agg: rows=%d in %dms (cp=%d:%d -> %d:%d safe=%d)", processed, pullMs, lastB, lastIdx, newB, newIdx, safe)

		var maxB, maxIdx int64
		// update counters and checkpoint
		totalProcessed += processed
		processedSinceReport += processed
		maxB, maxIdx = newB, newIdx
		if maxB > 0 {
			if err := w.Store.SetCheckpoint(ctx, w.WorkerID, maxB, maxIdx); err != nil {
				log.Printf("SetCheckpoint error: %v", err)
			}
		}
		// note: server-side aggregation path doesn't expose per-token min block here.
		// If you need metadata enrichment on the hot path, we can add a lightweight distinct-token query.

		select {
		case <-ticker.C:
			// roll the 8-hour window up to safe hour
			w.roll8h(ctx)
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
			if lag < 0 {
				lag = 0
			}
			elapsed := time.Since(started).Seconds()
			rpsTotal := float64(0)
			if elapsed > 0 {
				rpsTotal = float64(totalProcessed) / elapsed
			}
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
			if dt > 0 {
				bpsRecent = float64(cpDelta) / dt
			}
			epbRecent := float64(0)
			if cpDelta > 0 {
				epbRecent = float64(processedSinceReport) / float64(cpDelta)
			}
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

func (w *Worker) roll8h(ctx context.Context) {
	// anchor to the latest processed hour (not ahead of safe)
	latest, err := w.Store.LatestBlockNumber(ctx)
	if err != nil {
		return
	}
	safe := latest - w.ReorgDepth
	safeHour, err := w.Store.LatestSafeHour(ctx, safe)
	if err != nil || safeHour.IsZero() {
		return
	}
	procHour, err := w.Store.LatestProcessedHour(ctx)
	if err != nil || procHour.IsZero() {
		return
	}
	if procHour.After(safeHour) {
		procHour = safeHour
	}

	cur, err := w.Store.Get8hCursorHour(ctx)
	if err != nil {
		return
	}
	if cur.IsZero() {
		// initialize to processed hour snapshot
		if err := w.Store.Rebuild8hAtHour(ctx, procHour); err == nil {
			log.Printf("8h init at hour=%s", procHour.Format(time.RFC3339))
			_ = w.Store.Set8hCursorHour(ctx, procHour)
		}
		return
	}
	// advance hour by hour up to safeHour
	for h := cur.Add(time.Hour); !h.After(procHour); h = h.Add(time.Hour) {
		// add new hour into points and summary
		if err := w.Store.Add8hPointsHour(ctx, h); err != nil {
			break
		}
		if err := w.Store.Add8hHour(ctx, h); err != nil {
			break
		}
		// drop hour outside window
		out := h.Add(-8 * time.Hour)
		if err := w.Store.Sub8hHour(ctx, out); err != nil {
			break
		}
		_ = w.Store.Sub8hPointsHour(ctx, out)
		_ = w.Store.Set8hCursorHour(ctx, h)
		log.Printf("8h rolled to hour=%s (dropped=%s)", h.Format(time.RFC3339), out.Format(time.RFC3339))
	}
}

func (w *Worker) refreshLeaderboard(ctx context.Context) {
	if w.Cache == nil {
		return
	}
	items, from, to, err := w.Store.TopTokens8hWithMeta(ctx, w.LeaderboardSize)
	if err != nil {
		return
	}
	_ = w.Cache.SetLeaderboard8hWithMeta(ctx, from, to, items)
}

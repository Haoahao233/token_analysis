package api

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "strconv"
    "strings"
    "time"

    "github.com/example/tx-analytics/internal/cache"
    "github.com/example/tx-analytics/internal/models"
    "github.com/example/tx-analytics/internal/store"
)

type Handler struct {
    Store store.Store
    Cache cache.Cache
    LeaderboardSize int
}

func (h *Handler) Routes(mux *http.ServeMux) {
    mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
    mux.HandleFunc("/tokens/top", h.TopTokens)
    mux.HandleFunc("/tokens/", h.tokenSubroutes)
}

func (h *Handler) TopTokens(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    limit := h.parseLimit(r, h.LeaderboardSize)
    if h.Cache != nil {
        if wf, wt, items, ok, err := h.Cache.GetLeaderboard8hWithMeta(ctx, limit); err == nil && ok {
            writeJSON(w, http.StatusOK, map[string]any{
                "window": models.Window{From: wf, To: wt},
                "tokens": items,
            })
            return
        }
    }

    items, wf, wt, err := h.Store.TopTokens8hWithMeta(ctx, limit)
    if err != nil {
        httpError(w, http.StatusInternalServerError, err)
        return
    }
    if h.Cache != nil {
        _ = h.Cache.SetLeaderboard8hWithMeta(ctx, wf, wt, items)
    }
    writeJSON(w, http.StatusOK, map[string]any{
        "window": models.Window{From: wf, To: wt},
        "tokens": items,
    })
}

func (h *Handler) tokenSubroutes(w http.ResponseWriter, r *http.Request) {
    // Expect: /tokens/{token}/txs/hourly
    parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
    if len(parts) == 4 && parts[0] == "tokens" && parts[2] == "txs" && parts[3] == "hourly" {
        h.HourlySeries(w, r, parts[1])
        return
    }
    if len(parts) == 4 && parts[0] == "tokens" && parts[2] == "txs" && parts[3] == "8h" {
        h.Series8h(w, r, parts[1])
        return
    }
    if len(parts) == 3 && parts[0] == "tokens" && parts[2] == "metadata" {
        h.TokenMetadata(w, r, parts[1])
        return
    }
    http.NotFound(w, r)
}

func (h *Handler) HourlySeries(w http.ResponseWriter, r *http.Request, token string) {
    ctx := r.Context()
    from, to := parseWindow(r, 24*time.Hour)
    pts, err := h.Store.HourlySeries(ctx, token, from, to)
    if err != nil {
        httpError(w, http.StatusInternalServerError, err)
        return
    }
    writeJSON(w, http.StatusOK, map[string]any{
        "token_address": token,
        "from": from,
        "to": to,
        "points": pts,
    })
}

// GET /tokens/{token}/txs/8h
func (h *Handler) Series8h(w http.ResponseWriter, r *http.Request, token string) {
    ctx := r.Context()
    pts, err := h.Store.Series8h(ctx, token)
    if err != nil {
        httpError(w, http.StatusInternalServerError, err)
        return
    }
    // derive time window from series if present
    var from, to time.Time
    if len(pts) > 0 {
        from = pts[0].Hour
        to = pts[len(pts)-1].Hour
    }
    writeJSON(w, http.StatusOK, map[string]any{
        "token_address": token,
        "from": from,
        "to": to,
        "points": pts,
    })
}

// GET /tokens/{token}/metadata
func (h *Handler) TokenMetadata(w http.ResponseWriter, r *http.Request, token string) {
    ctx := r.Context()
    md, err := h.Store.GetTokenMetadata(ctx, token)
    if err != nil {
        httpError(w, http.StatusNotFound, err)
        return
    }
    writeJSON(w, http.StatusOK, md)
}

func (h *Handler) parseLimit(r *http.Request, def int) int {
    v := r.URL.Query().Get("limit")
    if v == "" { return def }
    n, err := strconv.Atoi(v)
    if err != nil || n <= 0 { return def }
    if n > 1000 { n = 1000 }
    return n
}

func parseWindow(r *http.Request, def time.Duration) (time.Time, time.Time) {
    q := r.URL.Query()
    now := time.Now().UTC()
    to := parseTimeParam(q.Get("to"), now)
    from := parseTimeParam(q.Get("from"), to.Add(-def))
    // ensure order
    if from.After(to) { from, to = to, from }
    return from.Truncate(time.Hour).UTC(), to.Truncate(time.Hour).UTC()
}

// parseTimeParam accepts unix seconds or milliseconds, or RFC3339 as fallback.
func parseTimeParam(v string, fallback time.Time) time.Time {
    if v == "" { return fallback }
    if n, err := strconv.ParseInt(v, 10, 64); err == nil {
        // Heuristic: >= 1e12 -> milliseconds
        if n >= 1_000_000_000_000 { // ms
            return time.Unix(0, n*int64(time.Millisecond)).UTC()
        }
        // seconds
        return time.Unix(n, 0).UTC()
    }
    if t, err := time.Parse(time.RFC3339, v); err == nil {
        return t.UTC()
    }
    return fallback
}

func writeJSON(w http.ResponseWriter, code int, v any) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    if err := json.NewEncoder(w).Encode(v); err != nil {
        log.Printf("writeJSON error: %v", err)
    }
}

func httpError(w http.ResponseWriter, code int, err error) {
    writeJSON(w, code, map[string]string{"error": err.Error()})
}

// Support context cancellation in tests if needed
func WithTimeout(r *http.Request, d time.Duration) (*http.Request, context.CancelFunc) {
    ctx, cancel := context.WithTimeout(r.Context(), d)
    return r.WithContext(ctx), cancel
}

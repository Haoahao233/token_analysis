package mock

import (
    "context"
    "sort"
    "time"

    "github.com/example/tx-analytics/internal/models"
)

type MockStore struct {
    Transfers []models.TransferRow
    HourAgg   map[string]int64 // key: token|unixHour
    LastB     int64
    LastIdx   int64
    LatestBlk int64
    MetaMap   map[string]models.TokenMetadata
}

func NewMockStore() *MockStore {
    return &MockStore{HourAgg: map[string]int64{}, MetaMap: map[string]models.TokenMetadata{}}
}

func (m *MockStore) key(token string, ts time.Time) string {
    return token + "|" + strconv64(ts.Truncate(time.Hour).Unix())
}

func strconv64(n int64) string { return fmtInt(n) }

// small inline fmt without importing fmt to keep deps minimal
func fmtInt(n int64) string {
    // very small and naive base-10 conversion
    if n == 0 { return "0" }
    neg := n < 0
    if neg { n = -n }
    var b [20]byte
    i := len(b)
    for n > 0 {
        i--
        b[i] = byte('0' + (n % 10))
        n /= 10
    }
    if neg {
        i--
        b[i] = '-'
    }
    return string(b[i:])
}

func (m *MockStore) GetCheckpoint(ctx context.Context, workerID string) (int64, int64, error) {
    return m.LastB, m.LastIdx, nil
}

func (m *MockStore) SetCheckpoint(ctx context.Context, workerID string, lastBlock, lastLogIdx int64) error {
    m.LastB, m.LastIdx = lastBlock, lastLogIdx
    return nil
}

func (m *MockStore) LatestBlockNumber(ctx context.Context) (int64, error) { return m.LatestBlk, nil }

func (m *MockStore) LatestProcessedHour(ctx context.Context) (time.Time, error) {
    var max int64 = 0
    for k := range m.HourAgg {
        // k format: token|unixHour
        parts := 0
        for i := len(k)-1; i >= 0; i-- {
            if k[i] == '|' { parts = i; break }
        }
        if parts <= 0 { continue }
        // parse int64
        var n int64 = 0
        sign := int64(1)
        s := k[parts+1:]
        for i := 0; i < len(s); i++ {
            c := s[i]
            if c == '-' && i == 0 { sign = -1; continue }
            if c < '0' || c > '9' { n = 0; break }
            n = n*10 + int64(c-'0')
        }
        n *= sign
        if n > max { max = n }
    }
    if max == 0 { return time.Time{}, nil }
    return time.Unix(max, 0).UTC(), nil
}

func (m *MockStore) NextTransfers(ctx context.Context, lastBlock, lastLogIdx int64, limit int) ([]models.TransferRow, error) {
    // ensure sorted
    trs := make([]models.TransferRow, len(m.Transfers))
    copy(trs, m.Transfers)
    sort.Slice(trs, func(i, j int) bool {
        if trs[i].BlockNumber == trs[j].BlockNumber {
            return trs[i].LogIndex < trs[j].LogIndex
        }
        return trs[i].BlockNumber < trs[j].BlockNumber
    })
    var out []models.TransferRow
    for _, t := range trs {
        if t.BlockNumber > lastBlock || (t.BlockNumber == lastBlock && t.LogIndex > lastLogIdx) {
            out = append(out, t)
            if len(out) >= limit { break }
        }
    }
    return out, nil
}

func (m *MockStore) NextTransfersSafe(ctx context.Context, lastBlock, lastLogIdx int64, maxBlock int64, limit int) ([]models.TransferRow, error) {
    trs := make([]models.TransferRow, len(m.Transfers))
    copy(trs, m.Transfers)
    sort.Slice(trs, func(i, j int) bool {
        if trs[i].BlockNumber == trs[j].BlockNumber { return trs[i].LogIndex < trs[j].LogIndex }
        return trs[i].BlockNumber < trs[j].BlockNumber
    })
    var out []models.TransferRow
    for _, t := range trs {
        if t.BlockNumber > maxBlock { break }
        if t.BlockNumber > lastBlock || (t.BlockNumber == lastBlock && t.LogIndex > lastLogIdx) {
            out = append(out, t)
            if len(out) >= limit { break }
        }
    }
    return out, nil
}

func (m *MockStore) UpsertHourly(ctx context.Context, token string, ts time.Time) error {
    k := m.key(token, ts)
    m.HourAgg[k]++
    return nil
}

func (m *MockStore) UpsertHourlyBatch(ctx context.Context, tokens []string, hours []time.Time, counts []int64) error {
    for i := range tokens {
        k := m.key(tokens[i], hours[i])
        m.HourAgg[k] += counts[i]
    }
    return nil
}

func (m *MockStore) TopTokens8hExact(ctx context.Context, limit int) ([]models.TokenCount, time.Time, time.Time, error) {
    to := time.Now().UTC()
    from := to.Add(-8 * time.Hour)
    cnt := map[string]int64{}
    for _, t := range m.Transfers {
        if t.BlockTimestamp.Before(from) || !t.BlockTimestamp.Before(to) { continue }
        cnt[t.TokenAddress]++
    }
    items := make([]models.TokenCount, 0, len(cnt))
    for k, v := range cnt { items = append(items, models.TokenCount{TokenAddress: k, TxCount: v}) }
    sort.Slice(items, func(i, j int) bool { return items[i].TxCount > items[j].TxCount })
    if limit < len(items) { items = items[:limit] }
    return items, from, to, nil
}

func (m *MockStore) HourlySeries(ctx context.Context, token string, from, to time.Time) ([]models.HourPoint, error) {
    var out []models.HourPoint
    for ts := from.Truncate(time.Hour); !ts.After(to.Truncate(time.Hour)); ts = ts.Add(time.Hour) {
        k := m.key(token, ts)
        if c, ok := m.HourAgg[k]; ok {
            out = append(out, models.HourPoint{Hour: ts, TxCount: c})
        }
    }
    return out, nil
}

func (m *MockStore) UpsertTokenMetadata(ctx context.Context, md models.TokenMetadata) error {
    m.MetaMap[md.TokenAddress] = md
    return nil
}

func (m *MockStore) GetTokenMetadata(ctx context.Context, token string) (models.TokenMetadata, error) {
    if v, ok := m.MetaMap[token]; ok { return v, nil }
    return models.TokenMetadata{}, ErrNotFound
}

func (m *MockStore) MissingMetadataTokens(ctx context.Context, limit int) ([]string, error) {
    // From transfers, find tokens not in MetaMap
    seen := map[string]struct{}{}
    for _, t := range m.Transfers { seen[t.TokenAddress] = struct{}{} }
    out := []string{}
    for token := range seen {
        if _, ok := m.MetaMap[token]; !ok {
            out = append(out, token)
            if len(out) >= limit { break }
        }
    }
    return out, nil
}

// ErrNotFound is a sentinel error for missing records in mocks
type errNF struct{}
func (e errNF) Error() string { return "not found" }
var ErrNotFound error = errNF{}

func (m *MockStore) FirstSeenBlock(ctx context.Context, token string) (int64, error) {
    // Walk transfers to compute min block
    var min int64 = 0
    for _, t := range m.Transfers {
        if t.TokenAddress != token { continue }
        if min == 0 || t.BlockNumber < min { min = t.BlockNumber }
    }
    return min, nil
}

func (m *MockStore) EnsureTokenMetadataRow(ctx context.Context, token string, firstSeenBlock int64) error {
    md, ok := m.MetaMap[token]
    if !ok {
        md = models.TokenMetadata{TokenAddress: token, FirstSeenBlock: firstSeenBlock}
    } else {
        if md.FirstSeenBlock == 0 || firstSeenBlock < md.FirstSeenBlock {
            md.FirstSeenBlock = firstSeenBlock
        }
    }
    m.MetaMap[token] = md
    return nil
}

func (m *MockStore) Get8hCursorHour(ctx context.Context) (time.Time, error) { return time.Time{}, nil }
func (m *MockStore) Set8hCursorHour(ctx context.Context, hour time.Time) error { return nil }
func (m *MockStore) LatestSafeHour(ctx context.Context, safeBlock int64) (time.Time, error) { return time.Now().UTC().Truncate(time.Hour), nil }
func (m *MockStore) Add8hHour(ctx context.Context, hour time.Time) error { return nil }
func (m *MockStore) Sub8hHour(ctx context.Context, hour time.Time) error { return nil }
func (m *MockStore) Rebuild8hAtHour(ctx context.Context, hour time.Time) error { return nil }
func (m *MockStore) Add8hPointsHour(ctx context.Context, hour time.Time) error { return nil }
func (m *MockStore) Sub8hPointsHour(ctx context.Context, hour time.Time) error { return nil }
func (m *MockStore) Series8h(ctx context.Context, token string) ([]models.HourPoint, error) { return nil, nil }
func (m *MockStore) AggregateHourlyNextBatch(ctx context.Context, lastBlock, lastLogIdx, maxBlock int64, limit int) (int64, int64, int64, error) {
    // no-op mock returning unchanged cursor
    return lastBlock, lastLogIdx, 0, nil
}

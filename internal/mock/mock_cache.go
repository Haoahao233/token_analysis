package mock

import (
    "context"
    "time"

    "github.com/example/tx-analytics/internal/models"
)

type MockCache struct {
    From time.Time
    To   time.Time
    Items []models.TokenCount
    Set bool
}

func (m *MockCache) GetLeaderboard8h(ctx context.Context, limit int) (time.Time, time.Time, []models.TokenCount, bool, error) {
    if !m.Set || len(m.Items) == 0 { return time.Time{}, time.Time{}, nil, false, nil }
    items := m.Items
    if limit < len(items) { items = items[:limit] }
    return m.From, m.To, items, true, nil
}

func (m *MockCache) SetLeaderboard8h(ctx context.Context, from, to time.Time, items []models.TokenCount) error {
    m.From, m.To, m.Items, m.Set = from, to, items, true
    return nil
}


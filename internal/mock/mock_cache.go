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
    MetaItems []models.TokenWithMeta
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

func (m *MockCache) GetLeaderboard8hWithMeta(ctx context.Context, limit int) (time.Time, time.Time, []models.TokenWithMeta, bool, error) {
    if !m.Set || len(m.MetaItems) == 0 { return time.Time{}, time.Time{}, nil, false, nil }
    items := m.MetaItems
    if limit < len(items) { items = items[:limit] }
    return m.From, m.To, items, true, nil
}

func (m *MockCache) SetLeaderboard8hWithMeta(ctx context.Context, from, to time.Time, items []models.TokenWithMeta) error {
    m.From, m.To, m.MetaItems, m.Set = from, to, items, true
    return nil
}

package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type Repository interface {
	UpsertSummary(ctx context.Context, summary *Summary) error
	GetSummary(ctx context.Context, date time.Time, hour int, eventType string) (*Summary, error)
	GetSummariesByDateRange(ctx context.Context, from, to time.Time, eventType string) ([]*Summary, error)
	GetTopProducts(ctx context.Context, from, to time.Time, limit int) ([]*ProductStats, error)
}

type ProductStats struct {
	ProductID      string  `db:"product_id" json:"product_id"`
	EventCount     int64   `db:"event_count" json:"event_count"`
	UniqueUsers    int64   `db:"unique_users" json:"unique_users"`
	ConversionRate float64 `db:"conversion_rate" json:"conversion_rate"`
}

type repository struct {
	db     *sqlx.DB
	logger *zap.Logger
}

func NewRepository(db *sqlx.DB, logger *zap.Logger) Repository {
	return &repository{
		db:     db,
		logger: logger,
	}
}

func (r *repository) UpsertSummary(ctx context.Context, summary *Summary) error {
	query := `
		INSERT INTO analytics_summary (date, hour, event_type, total_events, unique_users, metadata, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (date, hour, event_type) 
		DO UPDATE SET
			total_events = analytics_summary.total_events + EXCLUDED.total_events,
			unique_users = EXCLUDED.unique_users,
			metadata = EXCLUDED.metadata,
			updated_at = EXCLUDED.updated_at
		RETURNING id
	`

	err := r.db.QueryRowContext(
		ctx,
		query,
		summary.Date,
		summary.Hour,
		summary.EventType,
		summary.TotalEvents,
		summary.UniqueUsers,
		summary.Metadata,
		summary.UpdatedAt,
	).Scan(&summary.ID)

	if err != nil {
		r.logger.Error("Failed to upsert summary", zap.Error(err))
		return fmt.Errorf("failed to upsert summary: %w", err)
	}

	r.logger.Debug("Summary upserted",
		zap.String("date", summary.Date.Format("2006-01-02")),
		zap.Int("hour", summary.Hour),
		zap.String("event_type", summary.EventType),
		zap.Int64("total_events", summary.TotalEvents),
	)

	return nil
}

func (r *repository) GetSummary(
	ctx context.Context,
	date time.Time,
	hour int,
	eventType string) (*Summary, error) {
	query := `
		SELECT id, date, hour, event_type, total_events, unique_users, metadata, updated_at
		FROM analytics_summary
		WHERE date = $1 AND hour = $2 AND event_type = $3
	`

	var summary Summary
	err := r.db.GetContext(ctx, &summary, query, date, hour, eventType)
	if err != nil {
		return nil, fmt.Errorf("failed to get summary: %w", err)
	}

	return &summary, nil
}

func (r *repository) GetSummariesByDateRange(
	ctx context.Context,
	from, to time.Time,
	eventType string) ([]*Summary, error) {
	query := `
		SELECT id, date, hour, event_type, total_events, unique_users, metadata, updated_at
		FROM analytics_summary
		WHERE date >= $1 AND date <= $2
	`
	args := []interface{}{from, to}

	if eventType != "" {
		query += " AND event_type = $3"
		args = append(args, eventType)
	}

	query += " ORDER BY date, hour"

	var summaries []*Summary
	err := r.db.SelectContext(ctx, &summaries, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get summaries: %w", err)
	}

	return summaries, nil
}

func (r *repository) GetTopProducts(
	ctx context.Context,
	from, to time.Time,
	limit int) ([]*ProductStats, error) {
	query := `
		WITH product_events AS (
			SELECT 
				product_id,
				event_type,
				user_id,
				COUNT(*) as event_count
			FROM events
			WHERE 
				product_id IS NOT NULL
				AND created_at >= $1 
				AND created_at <= $2
			GROUP BY product_id, event_type, user_id
		),
		product_stats AS (
			SELECT
				product_id,
				SUM(event_count) as total_events,
				COUNT(DISTINCT user_id) as unique_users
			FROM product_events
			GROUP BY product_id
		)
		SELECT 
			product_id,
			total_events as event_count,
			unique_users,
			0.0 as conversion_rate
		FROM product_stats
		ORDER BY total_events DESC
		LIMIT $3
	`

	var stats []*ProductStats
	err := r.db.SelectContext(ctx, &stats, query, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get top products: %w", err)
	}

	return stats, nil
}

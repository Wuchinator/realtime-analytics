package query

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type repository struct {
	db     *sqlx.DB
	logger *zap.Logger
}

func NewEventRepository(db *sqlx.DB, logger *zap.Logger) EventRepository {
	return &repository{
		db:     db,
		logger: logger,
	}
}

func (r *repository) GetByID(ctx context.Context, id uuid.UUID) (*Event, error) {
	query := `
		SELECT id, event_type, user_id, session_id, product_id, data, created_at, processed_at
		FROM events
		WHERE id = $1
	`

	var event Event
	err := r.db.GetContext(ctx, &event, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("event not found")
		}
		r.logger.Error("Failed to get event", zap.Error(err))
		return nil, fmt.Errorf("failed to get event: %w", err)
	}

	return &event, nil
}

func (r *repository) GetByUserID(
	ctx context.Context,
	userID uuid.UUID,
	from, to time.Time,
	limit int,
) ([]*Event, error) {
	query := `
		SELECT id, event_type, user_id, session_id, product_id, data, created_at, processed_at
		FROM events
		WHERE user_id = $1
		  AND created_at >= $2
		  AND created_at <= $3
		ORDER BY created_at DESC
		LIMIT $4
	`

	var events []*Event
	err := r.db.SelectContext(ctx, &events, query, userID, from, to, limit)
	if err != nil {
		r.logger.Error("Failed to get user events",
			zap.Error(err),
			zap.String("user_id", userID.String()),
		)
		return nil, fmt.Errorf("failed to get user events: %w", err)
	}

	return events, nil
}

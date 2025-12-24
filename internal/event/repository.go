package event

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Wuchinator/realtime-analytics/pkg/postgres"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

type Repository interface {
	Create(ctx context.Context, event *Event) error
	CreateBatch(ctx context.Context, events []*Event) error
	GetByID(ctx context.Context, id uuid.UUID) (*Event, error)
	GetByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Event, error)
	MarkAsProcessed(ctx context.Context, id uuid.UUID) error
	GetUnprocessed(ctx context.Context, limit int) ([]*Event, error)
}

type repository struct {
	db     *postgres.DB
	logger *zap.Logger
}

func NewRepository(db *postgres.DB, logger *zap.Logger) Repository {
	return &repository{
		db:     db,
		logger: logger,
	}
}

func (r *repository) Create(ctx context.Context, event *Event) error {
	if err := event.Validate(); err != nil {
		return err
	}

	query := `
		INSERT INTO events (id, event_type, user_id, session_id, product_id, data, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err := r.db.ExecContext(
		ctx,
		query,
		event.ID,
		event.EventType,
		event.UserID,
		event.SessionID,
		event.ProductID,
		event.Data,
		event.CreatedAt,
	)

	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok {
			if pqErr.Code == "23505" {
				r.logger.Warn("Duplicate event ignored",
					zap.String("event_id", event.ID.String()),
				)
				return ErrDuplicateEvent
			}
		}
		r.logger.Error("Failed to create event", zap.Error(err))
		return fmt.Errorf("failed to create event: %w", err)
	}

	r.logger.Debug("Event created",
		zap.String("event_id", event.ID.String()),
		zap.String("event_type", event.EventType),
		zap.String("user_id", event.UserID.String()),
	)

	return nil
}

func (r *repository) CreateBatch(ctx context.Context, events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Намеренно игнорирую ошибку

	stmt, err := tx.PreparexContext(ctx, `
		INSERT INTO events (id, event_type, user_id, session_id, product_id, data, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	successCount := 0
	for _, event := range events {
		if err := event.Validate(); err != nil {
			r.logger.Warn("Invalid event in batch",
				zap.String("event_id", event.ID.String()),
				zap.Error(err),
			)
			continue
		}

		_, err := stmt.ExecContext(
			ctx,
			event.ID,
			event.EventType,
			event.UserID,
			event.SessionID,
			event.ProductID,
			event.Data,
			event.CreatedAt,
		)
		if err != nil {
			r.logger.Error("Failed to insert event in batch",
				zap.String("event_id", event.ID.String()),
				zap.Error(err),
			)
			continue
		}
		successCount++
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	r.logger.Info("Batch insert completed",
		zap.Int("total", len(events)),
		zap.Int("success", successCount),
	)

	return nil
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
			return nil, ErrEventNotFound
		}
		return nil, fmt.Errorf("failed to get event: %w", err)
	}

	return &event, nil
}

func (r *repository) GetByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Event, error) {
	query := `
		SELECT id, event_type, user_id, session_id, product_id, data, created_at, processed_at
		FROM events
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`

	var events []*Event
	err := r.db.SelectContext(ctx, &events, query, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get user events: %w", err)
	}

	return events, nil
}

func (r *repository) MarkAsProcessed(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE events
		SET processed_at = NOW()
		WHERE id = $1 AND processed_at IS NULL
	`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to mark event as processed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrEventAlreadyProcessed
	}

	return nil
}

func (r *repository) GetUnprocessed(ctx context.Context, limit int) ([]*Event, error) {
	query := `
		SELECT id, event_type, user_id, session_id, product_id, data, created_at, processed_at
		FROM events
		WHERE processed_at IS NULL
		ORDER BY created_at ASC
		LIMIT $1
	`

	var events []*Event
	err := r.db.SelectContext(ctx, &events, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get unprocessed events: %w", err)
	}

	return events, nil
}

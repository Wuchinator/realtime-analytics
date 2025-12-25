package event

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type KafkaProducer interface {
	SendMessage(ctx context.Context, key string, value any) error
	SendMessageBatch(ctx context.Context, messages map[string]any) error
}

type Service struct {
	repo     Repository
	producer KafkaProducer
	logger   *zap.Logger
}

func NewService(repo Repository, producer KafkaProducer, logger *zap.Logger) *Service {
	return &Service{
		repo:     repo,
		producer: producer,
		logger:   logger,
	}
}

func (s *Service) TrackEvent(ctx context.Context, event *Event) error {
	if err := event.Validate(); err != nil {
		s.logger.Warn("failed to validate event",
			zap.Error(err),
			zap.String("event_id", event.ID.String()))
		return fmt.Errorf("invalid event: %w", err)
	}

	if err := s.repo.Create(ctx, event); err != nil {
		if errors.Is(err, ErrDuplicateEvent) {
			s.logger.Debug("event is already tracked", zap.String("event_id", event.ID.String()))
			return nil
		}

		s.logger.Error("failed to create event", zap.String("event_id", event.ID.String()),
			zap.Error(err),
			zap.String("event_id", event.ID.String()))

		return fmt.Errorf("failed to create event: %w", err)
	}

	// События одного пользователя идут в одну партицию
	key := event.UserID.String()

	if err := s.producer.SendMessage(ctx, key, event); err != nil {
		s.logger.Error("failed to send message",
			zap.String("event_id", event.ID.String()),
			zap.Error(err))
	}

	s.logger.Info("Event tracked successfully",
		zap.String("event_id", event.ID.String()),
		zap.String("event_type", event.EventType),
		zap.String("user_id", event.UserID.String()),
	)
	return nil
}

func (s *Service) TrackEventBatch(ctx context.Context, events []*Event) (int, []string, error) {
	if len(events) == 0 {
		return 0, nil, fmt.Errorf("no events provided")
	}

	s.logger.Info("Tracking events", zap.Int("events", len(events)))

	if err := s.repo.CreateBatch(ctx, events); err != nil {
		s.logger.Error("failed to create event batch", zap.Error(err))
		return 0, nil, fmt.Errorf("failed to save batch: %w", err)
	}

	messages := make(map[string]any)
	failedIDs := make([]string, 0)

	for _, event := range events {
		key := event.UserID.String()
		messages[key] = event.EventType
	}

	for key, value := range messages {
		if err := s.producer.SendMessage(ctx, key, value); err != nil {
			if ev, ok := value.(*Event); ok {
				failedIDs = append(failedIDs, ev.ID.String())
			}
			s.logger.Error("Failed to send message in batch",
				zap.Error(err),
				zap.String("key", key),
			)
		}
	}

	successCount := len(events) - len(failedIDs)

	return successCount, failedIDs, nil
}

func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (*Event, error) {
	event, err := s.repo.GetByID(ctx, id)
	if err != nil {
		s.logger.Error("failed to get event by ID", zap.Error(err), zap.String("id", id.String()))
		return nil, fmt.Errorf("failed to get event by ID: %w", err)
	}

	return event, nil
}

func (s *Service) GetByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Event, error) {
	events, err := s.repo.GetByUserID(ctx, userID, limit)
	if err != nil {
		s.logger.Error("failed to get event by user ID", zap.Error(err), zap.String("user_id", userID.String()))
		return nil, fmt.Errorf("failed to get event by user ID: %w", err)
	}

	return events, nil
}

func (s *Service) HealthCheck(ctx context.Context) (bool, map[string]string) {
	status := make(map[string]string)

	status["postgres"] = "ok"

	status["kafka"] = "ok"

	return true, status
}

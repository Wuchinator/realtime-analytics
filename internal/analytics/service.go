package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type Service struct {
	repo   Repository
	logger *zap.Logger

	// In-memory кеш
	uniqueUsers map[string]map[string]bool
}

func NewService(repo Repository, logger *zap.Logger) *Service {
	return &Service{
		repo:        repo,
		logger:      logger,
		uniqueUsers: make(map[string]map[string]bool),
	}
}

func (s *Service) ProcessEvent(ctx context.Context, eventData *EventData) error {
	date := eventData.CreatedAt.Truncate(24 * time.Hour)
	hour := eventData.CreatedAt.Hour()

	key := fmt.Sprintf("%s-%d-%s", date.Format("2006-01-02"), hour, eventData.EventType)

	if s.uniqueUsers[key] == nil {
		s.uniqueUsers[key] = make(map[string]bool)
	}
	s.uniqueUsers[key][eventData.UserID] = true

	summary := NewSummary(date, hour, eventData.EventType)
	summary.IncrementEvents(1)
	summary.SetUniqueUsers(int64(len(s.uniqueUsers[key])))

	if err := s.repo.UpsertSummary(ctx, summary); err != nil {
		return fmt.Errorf("failed to upsert summary: %w", err)
	}

	s.logger.Debug("Event processed",
		zap.String("event_id", eventData.ID),
		zap.String("event_type", eventData.EventType),
		zap.String("date", date.Format("2006-01-02")),
		zap.Int("hour", hour),
	)

	return nil
}

func (s *Service) ProcessEventBatch(ctx context.Context, events []*EventData) error {
	for _, event := range events {
		if err := s.ProcessEvent(ctx, event); err != nil {
			s.logger.Error("Failed to process event in batch",
				zap.Error(err),
				zap.String("event_id", event.ID),
			)
			continue
		}
	}
	return nil
}

// GetSummaries получает статистику за период
func (s *Service) GetSummaries(ctx context.Context, from, to time.Time, eventType string) ([]*Summary, error) {
	return s.repo.GetSummariesByDateRange(ctx, from, to, eventType)
}

func (s *Service) GetTopProducts(ctx context.Context, from, to time.Time, limit int) ([]*ProductStats, error) {
	return s.repo.GetTopProducts(ctx, from, to, limit)
}

// CreateMessageHandler создаёт handler для Kafka consumer
func (s *Service) CreateMessageHandler() func(ctx context.Context, key, value []byte) error {
	return func(ctx context.Context, key, value []byte) error {
		var eventData EventData
		if err := json.Unmarshal(value, &eventData); err != nil {
			s.logger.Error("Failed to unmarshal event",
				zap.Error(err),
				zap.String("value", string(value)),
			)
			return err
		}

		return s.ProcessEvent(ctx, &eventData)
	}
}

// CleanupOldCache желательно вытащить в отдельную горутину
func (s *Service) CleanupOldCache() {
	cutoff := time.Now().Add(-24 * time.Hour).Format("2006-01-02")

	for key := range s.uniqueUsers {
		if key < cutoff {
			delete(s.uniqueUsers, key)
		}
	}

	s.logger.Debug("Cache cleanup completed")
}

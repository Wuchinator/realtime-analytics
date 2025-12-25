package query

import (
	"context"
	"fmt"
	"time"

	"github.com/Wuchinator/realtime-analytics/internal/analytics"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type EventRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Event, error)
	GetByUserID(ctx context.Context, id uuid.UUID, from, to time.Time, limit int) ([]*Event, error)
}

type AnalyticsRepository interface {
	GetSummariesByDateRange(ctx context.Context, from, to time.Time, eventType string) ([]*analytics.Summary, error)
	GetTopProducts(ctx context.Context, from, to time.Time, limit int) ([]*analytics.ProductStats, error)
}

type Service struct {
	eventRepo     EventRepository
	analyticsRepo AnalyticsRepository
	logger        *zap.Logger
}

func NewService(
	eventRepo EventRepository,
	analyticsRepo AnalyticsRepository,
	logger *zap.Logger) *Service {
	return &Service{
		eventRepo:     eventRepo,
		analyticsRepo: analyticsRepo,
		logger:        logger,
	}
}

func (s *Service) GetEventStats(
	ctx context.Context,
	from, to time.Time,
	eventType string,
	granularity string,
) ([]*EventStat, error) {
	summaries, err := s.analyticsRepo.GetSummariesByDateRange(ctx, from, to, eventType)
	if err != nil {
		s.logger.Error("Failed to get summaries",
			zap.Error(err),
			zap.Time("from", from),
			zap.Time("to", to))
		return nil, fmt.Errorf("failed to get summaries %w", err)
	}

	stats := s.groupByGranularity(summaries, granularity)

	s.logger.Info("Event stats retrieved",
		zap.Int("count", len(stats)),
		zap.String("granularity", granularity),
	)

	return stats, nil
}

func (s *Service) GetUserActivity(
	ctx context.Context,
	userID uuid.UUID,
	from, to time.Time,
	limit int,
) ([]*Event, int64, error) {
	events, err := s.eventRepo.GetByUserID(ctx, userID, from, to, limit)
	if err != nil {
		s.logger.Error("Failed to get user activity",
			zap.Error(err),
			zap.String("user_id", userID.String()),
		)
		return nil, 0, fmt.Errorf("failed to get user activity: %w", err)
	}

	totalCount := int64(len(events))

	s.logger.Info("User activity retrieved",
		zap.String("user_id", userID.String()),
		zap.Int64("events_count", totalCount),
	)

	return events, totalCount, nil
}

func (s *Service) GetTopProducts(
	ctx context.Context,
	from, to time.Time,
	limit int,
	eventType string,
) ([]*ProductStat, error) {
	products, err := s.analyticsRepo.GetTopProducts(ctx, from, to, limit)
	if err != nil {
		s.logger.Error("Failed to get top products",
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to get top products: %w", err)
	}

	stats := make([]*ProductStat, len(products))
	for i, p := range products {
		stats[i] = &ProductStat{
			ProductID:      p.ProductID,
			EventCount:     p.EventCount,
			UniqueUsers:    p.UniqueUsers,
			ConversionRate: p.ConversionRate,
		}
	}

	s.logger.Info("Top products retrieved",
		zap.Int("count", len(stats)),
	)

	return stats, nil
}

func (s *Service) groupByGranularity(summaries []*analytics.Summary, granularity string) []*EventStat {
	grouped := make(map[string]*EventStat)

	for _, summary := range summaries {
		var key string
		var timestamp time.Time

		switch granularity {
		case "hour":
			timestamp = time.Date(
				summary.Date.Year(),
				summary.Date.Month(),
				summary.Date.Day(),
				summary.Hour,
				0, 0, 0,
				time.UTC,
			)
			key = fmt.Sprintf("%s-%s", timestamp.Format("2006-01-02-15"), summary.EventType)
		case "day":
			timestamp = summary.Date
			key = fmt.Sprintf("%s-%s", timestamp.Format("2006-01-02"), summary.EventType)
		default:
			timestamp = time.Date(
				summary.Date.Year(),
				summary.Date.Month(),
				summary.Date.Day(),
				summary.Hour,
				0, 0, 0,
				time.UTC,
			)
			key = fmt.Sprintf("%s-%s", timestamp.Format("2006-01-02-15"), summary.EventType)
		}

		if stat, exists := grouped[key]; exists {
			stat.TotalEvents += summary.TotalEvents
			if summary.UniqueUsers > stat.UniqueUsers {
				stat.UniqueUsers = summary.UniqueUsers
			}
		} else {
			grouped[key] = &EventStat{
				Timestamp:   timestamp,
				EventType:   summary.EventType,
				TotalEvents: summary.TotalEvents,
				UniqueUsers: summary.UniqueUsers,
			}
		}
	}

	stats := make([]*EventStat, 0, len(grouped))
	for _, stat := range grouped {
		stats = append(stats, stat)
	}

	return stats
}

func (s *Service) HealthCheck(ctx context.Context) (bool, map[string]string) {

	status := make(map[string]string)

	status["postgres"] = "ok"
	return true, status
}

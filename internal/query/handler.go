package query

import (
	"context"
	"encoding/json"

	pb "github.com/Wuchinator/realtime-analytics/pkg/pb/analytics"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Handler struct {
	pb.UnimplementedQueryServiceServer
	service *Service
	logger  *zap.Logger
}

func NewHandler(service *Service, logger *zap.Logger) *Handler {
	return &Handler{
		service: service,
		logger:  logger,
	}
}

func (h *Handler) GetEventStats(
	ctx context.Context,
	req *pb.GetEventStatsRequest,
) (*pb.GetEventStatsResponse, error) {

	h.logger.Debug("GetEventStats called",
		zap.Time("from", req.From.AsTime()),
		zap.Time("to", req.To.AsTime()),
		zap.String("event_type", req.EventType))

	if req.From == nil || req.To == nil {
		return nil, status.Error(codes.InvalidArgument, "from and to timestamps are required")
	}

	stats, err := h.service.GetEventStats(
		ctx,
		req.From.AsTime(),
		req.To.AsTime(),
		req.EventType,
		req.Granularity,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get stats: %v", err)
	}

	pbStats := make([]*pb.EventStats, len(stats))
	for i, stat := range stats {
		pbStats[i] = &pb.EventStats{
			Timestamp:   timestamppb.New(stat.Timestamp),
			EventType:   stat.EventType,
			TotalEvents: stat.TotalEvents,
			UniqueUsers: stat.UniqueUsers,
		}

		// Добавляем metadata если есть
		if stat.Metadata != nil {
			pbStats[i].Metadata = make(map[string]string)
			for k, v := range stat.Metadata {
				if str, ok := v.(string); ok {
					pbStats[i].Metadata[k] = str
				}
			}
		}
	}

	return &pb.GetEventStatsResponse{
		Stats:      pbStats,
		TotalCount: int64(len(stats)),
	}, nil
}

func (h *Handler) GetUserActivity(
	ctx context.Context,
	req *pb.GetUserActivityRequest,
) (*pb.GetUserActivityResponse, error) {
	h.logger.Debug("GetUserActivity called",
		zap.String("user_id", req.UserId),
	)

	// Парсим UUID
	userID, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}

	if req.From == nil || req.To == nil {
		return nil, status.Error(codes.InvalidArgument, "from and to timestamps are required")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100 // дефолт
	}

	events, totalCount, err := h.service.GetUserActivity(
		ctx,
		userID,
		req.From.AsTime(),
		req.To.AsTime(),
		limit,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get user activity: %v", err)
	}

	pbEvents := make([]*pb.UserEvent, len(events))
	for i, event := range events {
		pbEvents[i] = &pb.UserEvent{
			EventId:   event.ID.String(),
			EventType: event.EventType,
			Timestamp: timestamppb.New(event.CreatedAt),
		}

		if event.ProductID != nil {
			pbEvents[i].ProductId = event.ProductID.String()
		}

		// Парсим JSON data в metadata
		if len(event.Data) > 0 {
			var metadata map[string]interface{}
			if err := json.Unmarshal(event.Data, &metadata); err == nil {
				pbEvents[i].Metadata = make(map[string]string)
				for k, v := range metadata {
					if str, ok := v.(string); ok {
						pbEvents[i].Metadata[k] = str
					}
				}
			}
		}
	}

	return &pb.GetUserActivityResponse{
		UserId:      req.UserId,
		Events:      pbEvents,
		TotalEvents: totalCount,
	}, nil
}

func (h *Handler) GetTopProducts(
	ctx context.Context,
	req *pb.GetTopProductsRequest,
) (*pb.GetTopProductsResponse, error) {
	h.logger.Debug("GetTopProducts called",
		zap.Int32("limit", req.Limit),
	)

	if req.From == nil || req.To == nil {
		return nil, status.Error(codes.InvalidArgument, "from and to timestamps are required")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10 // дефолт
	}

	products, err := h.service.GetTopProducts(
		ctx,
		req.From.AsTime(),
		req.To.AsTime(),
		limit,
		req.EventType,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get top products: %v", err)
	}

	pbProducts := make([]*pb.ProductStats, len(products))
	for i, p := range products {
		pbProducts[i] = &pb.ProductStats{
			ProductId:      p.ProductID,
			EventCount:     p.EventCount,
			UniqueUsers:    p.UniqueUsers,
			ConversionRate: p.ConversionRate,
		}
	}

	return &pb.GetTopProductsResponse{
		Products: pbProducts,
	}, nil
}

func (h *Handler) HealthCheck(
	ctx context.Context,
	req *pb.HealthCheckRequest,
) (*pb.HealthCheckResponse, error) {
	healthy, deps := h.service.HealthCheck(ctx)

	return &pb.HealthCheckResponse{
		Healthy:      healthy,
		Status:       "ok",
		Dependencies: deps,
	}, nil
}

package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	pb "github.com/Wuchinator/realtime-analytics/pkg/pb/events"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Handler struct {
	pb.UnimplementedEventServiceServer
	service *Service
	logger  *zap.Logger
}

func NewHandler(service *Service, logger *zap.Logger) *Handler {
	return &Handler{
		service: service,
		logger:  logger,
	}
}

func (h *Handler) TrackEvent(ctx context.Context, req *pb.TrackEventRequest) (*pb.TrackEventResponse, error) {
	h.logger.Debug(
		"TrackEvent",
		zap.String("event_id", req.Event.EventId),
		zap.String("event_type", req.Event.EventType.String()),
	)

	event, err := h.protoToEvent(req.Event)
	if err != nil {
		h.logger.Error("can not to convert proto to event", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "can't to convert proto to event: %v", err)
	}

	if err := h.service.TrackEvent(ctx, event); err != nil {
		switch {
		case errors.Is(err, ErrEventNotFound), errors.Is(err, ErrInvalidSessionID), errors.Is(err, ErrInvalidUserID):
			return nil, status.Errorf(codes.InvalidArgument, "can't track event: %v", err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "can't track event: %v", err.Error())
		}
	}

	return &pb.TrackEventResponse{
		EventId: event.ID.String(),
		Message: "Event tracked successfully",
		Success: true,
	}, nil
}

func (h *Handler) TrackEventBatch(ctx context.Context, req *pb.TrackEventBatchRequest) (*pb.TrackEventBatchResponse, error) {
	h.logger.Debug("TrackEventBatch called",
		zap.Int("event_count", len(req.Events)),
	)

	if len(req.Events) == 0 {
		return nil, status.Error(codes.InvalidArgument, "events list is empty")
	}

	events := make([]*Event, 0, len(req.Events))
	for _, protoEvent := range req.Events {
		event, err := h.protoToEvent(protoEvent)
		if err != nil {
			h.logger.Warn("Invalid event in batch",
				zap.Error(err),
				zap.String("event_id", protoEvent.EventId),
			)
			continue
		}
		events = append(events, event)
	}
	successCount, failedIDs, err := h.service.TrackEventBatch(ctx, events)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to track batch: %v", err)
	}

	return &pb.TrackEventBatchResponse{
		Success:        true,
		Message:        "Batch processed",
		ProcessedCount: int32(successCount),
		FailedEventIds: failedIDs,
	}, nil
}

func (h *Handler) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	health, dependencies := h.service.HealthCheck(ctx)

	return &pb.HealthCheckResponse{
		Healthy:      health,
		Status:       "ok",
		Dependencies: dependencies,
	}, nil
}

func (h *Handler) protoToEvent(protoEvent *pb.Event) (*Event, error) {
	eventID, err := uuid.Parse(protoEvent.EventId)
	if err != nil {
		//
		eventID = uuid.New()
	}

	userID, err := uuid.Parse(protoEvent.UserId)
	if err != nil {
		return nil, ErrInvalidUserID
	}

	sessionID, err := uuid.Parse(protoEvent.SessionId)
	if err != nil {
		return nil, ErrInvalidSessionID
	}

	var productID *uuid.UUID

	if protoEvent.ProductId != "" {
		pid, err := uuid.Parse(protoEvent.ProductId)
		if err != nil {
			return nil, fmt.Errorf("can't parse product id: %v", err)
		}
		productID = &pid
	}

	eventType := h.eventTypeToString(protoEvent.EventType)

	data, err := json.Marshal(protoEvent.Metadata)
	if err != nil {
		return nil, fmt.Errorf("could not marshal metadata: %v", err)
	}

	event := &Event{
		ID:        eventID,
		EventType: eventType,
		UserID:    userID,
		SessionID: sessionID,
		ProductID: productID,
		Data:      data,
		CreatedAt: protoEvent.Timestamp.AsTime(),
	}
	return event, nil
}

func (h *Handler) eventTypeToString(eventType pb.EventType) string {

	switch eventType {
	case pb.EventType_EVENT_TYPE_PAGE_VIEW:
		return EventTypePageView
	case pb.EventType_EVENT_TYPE_PRODUCT_VIEW:
		return EventTypeProductView
	case pb.EventType_EVENT_TYPE_ADD_TO_CART:
		return EventTypeAddToCart
	case pb.EventType_EVENT_TYPE_REMOVE_FROM_CART:
		return EventTypeRemoveFromCart
	case pb.EventType_EVENT_TYPE_PURCHASE:
		return EventTypePurchase
	case pb.EventType_EVENT_TYPE_SEARCH:
		return EventTypeSearch
	default:
		return "unknown"
	}
}

func (h *Handler) eventToProto(event *Event) (*pb.Event, error) {
	var metadata map[string]string
	if err := json.Unmarshal(event.Data, &metadata); err != nil {
		metadata = make(map[string]string)
	}

	protoEvent := &pb.Event{
		EventId:   event.ID.String(),
		UserId:    event.UserID.String(),
		SessionId: event.SessionID.String(),
		Metadata:  metadata,
		Timestamp: timestamppb.New(event.CreatedAt),
	}

	if event.ProductID != nil {
		protoEvent.ProductId = event.ProductID.String()
	}

	return protoEvent, nil
}

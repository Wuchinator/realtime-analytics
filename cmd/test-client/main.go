package main

import (
	"context"
	"fmt"
	"log"
	_ "time"

	pb "github.com/Wuchinator/realtime-analytics/pkg/pb/events"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	conn, err := grpc.Dial(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewEventServiceClient(conn)

	healthResp, err := client.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	fmt.Printf("Health check: %s\n", healthResp.Status)
	fmt.Printf("Dependencies: %v\n\n", healthResp.Dependencies)

	fmt.Println("Sending single event")
	singleEvent := &pb.Event{
		EventId:   uuid.New().String(),
		EventType: pb.EventType_EVENT_TYPE_PRODUCT_VIEW,
		UserId:    uuid.New().String(),
		SessionId: uuid.New().String(),
		ProductId: uuid.New().String(),
		Metadata: map[string]string{
			"product_name": "MacBook Pro",
			"category":     "laptops",
			"price":        "2499.99",
		},
		Timestamp: timestamppb.Now(),
	}

	resp, err := client.TrackEvent(context.Background(), &pb.TrackEventRequest{
		Event: singleEvent,
	})
	if err != nil {
		log.Fatalf("Failed to track event: %v", err)
	}
	fmt.Printf("Event tracked: %s\n", resp.EventId)
	fmt.Printf("Message: %s\n\n", resp.Message)

	fmt.Println("Sending batch of events")
	userID := uuid.New().String()
	sessionID := uuid.New().String()

	events := []*pb.Event{
		{
			EventId:   uuid.New().String(),
			EventType: pb.EventType_EVENT_TYPE_PAGE_VIEW,
			UserId:    userID,
			SessionId: sessionID,
			Metadata: map[string]string{
				"page": "/home",
			},
			Timestamp: timestamppb.Now(),
		},
		{
			EventId:   uuid.New().String(),
			EventType: pb.EventType_EVENT_TYPE_SEARCH,
			UserId:    userID,
			SessionId: sessionID,
			Metadata: map[string]string{
				"query": "wireless headphones",
			},
			Timestamp: timestamppb.Now(),
		},
		{
			EventId:   uuid.New().String(),
			EventType: pb.EventType_EVENT_TYPE_PRODUCT_VIEW,
			UserId:    userID,
			SessionId: sessionID,
			ProductId: uuid.New().String(),
			Metadata: map[string]string{
				"product_name": "AirPods Pro",
				"price":        "249.99",
			},
			Timestamp: timestamppb.Now(),
		},
		{
			EventId:   uuid.New().String(),
			EventType: pb.EventType_EVENT_TYPE_ADD_TO_CART,
			UserId:    userID,
			SessionId: sessionID,
			ProductId: uuid.New().String(),
			Metadata: map[string]string{
				"product_name": "AirPods Pro",
				"quantity":     "1",
			},
			Timestamp: timestamppb.Now(),
		},
	}

	batchResp, err := client.TrackEventBatch(context.Background(), &pb.TrackEventBatchRequest{
		Events: events,
	})
	if err != nil {
		log.Fatalf("Failed to track batch: %v", err)
	}

	fmt.Printf("Batch processed: %d/%d events\n", batchResp.ProcessedCount, len(events))
	if len(batchResp.FailedEventIds) > 0 {
		fmt.Printf("Failed IDs: %v\n", batchResp.FailedEventIds)
	}

	fmt.Println("\nAll tests passed")
}

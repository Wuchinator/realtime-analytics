package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/Wuchinator/realtime-analytics/pkg/pb/analytics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	conn, err := grpc.Dial(
		"localhost:50052",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewQueryServiceClient(conn)

	healthResp, err := client.HealthCheck(context.Background(), &pb.HealthCheckRequest{})
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	fmt.Printf("Dependencies: %v\n\n", healthResp.Dependencies)

	now := time.Now()
	from := now.Add(-24 * time.Hour)

	statsResp, err := client.GetEventStats(context.Background(), &pb.GetEventStatsRequest{
		From:        timestamppb.New(from),
		To:          timestamppb.New(now),
		EventType:   "",
		Granularity: "hour",
	})
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}

	fmt.Printf("Found %d statistics records\n", statsResp.TotalCount)
	if len(statsResp.Stats) > 0 {
		fmt.Println("\n   Latest stats:")
		for i, stat := range statsResp.Stats {
			if i >= 5 {
				break
			}
			fmt.Printf("   - %s | %s: %d events, %d unique users\n",
				stat.Timestamp.AsTime().Format("2006-01-02 15:04"),
				stat.EventType,
				stat.TotalEvents,
				stat.UniqueUsers,
			)
		}
	}

	fmt.Println("\nGetting top products")
	productsResp, err := client.GetTopProducts(context.Background(), &pb.GetTopProductsRequest{
		From:  timestamppb.New(from),
		To:    timestamppb.New(now),
		Limit: 5,
	})
	if err != nil {
		log.Fatalf("Failed to get top products: %v", err)
	}

	fmt.Printf("Top %d products:\n", len(productsResp.Products))
	for i, product := range productsResp.Products {
		fmt.Printf("   %d. Product %s: %d events, %d unique users\n",
			i+1,
			product.ProductId[:8]+"...",
			product.EventCount,
			product.UniqueUsers,
		)
	}

	fmt.Println("\nAll queries completed successfully!")
}

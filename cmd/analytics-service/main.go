package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Wuchinator/realtime-analytics/internal/analytics"
	"github.com/Wuchinator/realtime-analytics/internal/config"
	"github.com/Wuchinator/realtime-analytics/pkg/kafka"
	"github.com/Wuchinator/realtime-analytics/pkg/logger"
	"github.com/Wuchinator/realtime-analytics/pkg/postgres"
	"go.uber.org/zap"
)

func main() {

	cfg, err := config.Load()
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	log, err := logger.NewLogger(cfg.LogLevel, cfg.Environment)
	if err != nil {
		panic(fmt.Sprintf("Failed to create logger: %v", err))
	}
	defer log.Sync()

	log = logger.WithService(log, "analytics-service")
	log.Info("Starting Analytics Service",
		zap.String("environment", cfg.Environment),
		zap.String("consumer_group", cfg.Kafka.Topic+"-analytics"),
	)

	db, err := postgres.New(postgres.Config{
		DSN:             cfg.Postgres.PostgresDSN(),
		MaxOpenConns:    cfg.Postgres.MaxOpenConns,
		MaxIdleConns:    cfg.Postgres.MaxIdleConns,
		ConnMaxLifetime: cfg.Postgres.ConnMaxLifetime,
	}, log)
	if err != nil {
		log.Fatal("Failed to connect to PostgreSQL", zap.Error(err))
	}
	defer db.Close()

	analyticsRepo := analytics.NewRepository(db.DB, log)
	analyticsService := analytics.NewService(analyticsRepo, log)

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:           cfg.Kafka.Brokers,
		Topics:            []string{cfg.Kafka.Topic},
		GroupID:           cfg.Kafka.Topic + "-analytics",
		AutoCommit:        true,
		CommitInterval:    1 * time.Second,
		SessionTimeout:    10 * time.Second,
		RebalanceStrategy: "sticky",
	}, analyticsService.CreateMessageHandler(), log)
	if err != nil {
		log.Fatal("Failed to create Kafka consumer", zap.Error(err))
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Error("Consumer error", zap.Error(err))
		}
	}()

	<-consumer.WaitReady()
	log.Info("Kafka consumer is ready and consuming messages")

	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				analyticsService.CleanupOldCache()
			case <-ctx.Done():
				return
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down gracefully...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	<-shutdownCtx.Done()

	log.Info("Analytics Service stopped")
}

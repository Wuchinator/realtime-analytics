package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Wuchinator/realtime-analytics/internal/config"
	"github.com/Wuchinator/realtime-analytics/internal/event"
	"github.com/Wuchinator/realtime-analytics/pkg/kafka"
	"github.com/Wuchinator/realtime-analytics/pkg/logger"
	pb "github.com/Wuchinator/realtime-analytics/pkg/pb/events"
	"github.com/Wuchinator/realtime-analytics/pkg/postgres"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {

	cfg, err := config.Load()
	if err != nil {
		panic(fmt.Sprintf("Error loading config: %v", err))
	}

	log, err := logger.NewLogger(cfg.LogLevel, cfg.Environment)
	if err != nil {
		panic(fmt.Sprintf("Error initializing logger: %v", err))
	}

	defer log.Sync()

	log = logger.WithService(log, "event-service")
	log.Info("Starting Event Service",
		zap.String("environment", cfg.Environment),
		zap.String("grpc_port", cfg.GRPCPort),
	)

	db, err := postgres.New(postgres.Config{
		DSN:             cfg.Postgres.PostgresDSN(),
		MaxOpenConns:    cfg.Postgres.MaxOpenConns,
		MaxIdleConns:    cfg.Postgres.MaxIdleConns,
		ConnMaxLifetime: cfg.Postgres.ConnMaxLifetime}, log)

	if err != nil {
		log.Fatal("Error initializing postgres client", zap.Error(err))
	}

	defer db.Close()

	kafka, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:          cfg.Kafka.Brokers,
		Topic:            cfg.Kafka.Topic,
		Retries:          cfg.Kafka.ProducerRetries,
		Timeout:          cfg.Kafka.ProducerTimeout,
		RequiredAcks:     cfg.Kafka.RequiredAcks,
		Compression:      cfg.Kafka.CompressionType,
		IdempotentWrites: cfg.Kafka.IdempotentWrites,
		MaxMessageBytes:  cfg.Kafka.MaxMessageBytes,
	}, log)

	if err != nil {
		log.Fatal("Error initializing kafka", zap.Error(err))
	}

	defer kafka.Close()

	eventRepo := event.NewRepository(db, log)
	eventService := event.NewService(eventRepo, kafka, log)
	eventHandler := event.NewHandler(eventService, log)

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		loggingInterceptor(log),
		recoveryInterceptor(log)),
	)

	pb.RegisterEventServiceServer(grpcServer, eventHandler)

	// Checker for kuber
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("event-service", grpc_health_v1.HealthCheckResponse_SERVING)

	// Reflection для grpcurl и подобных инструментов
	reflection.Register(grpcServer)

	listener, err := net.Listen("tcp", ":"+cfg.GRPCPort)

	if err != nil {
		log.Fatal("Error initializing gRPC listener", zap.Error(err))
	}

	go func() {
		log.Info("Starting gRPC server", zap.String("port", cfg.GRPCPort))
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal("Error initializing gRPC server", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down gRPC server")

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	select {
	case <-stopped:
		log.Info("gRPC server stopped")
	case <-ctx.Done():
		log.Warn("shutdown gRPC server timed out")
		grpcServer.Stop()
	}
	log.Info("gRPC server stopped")
}

func loggingInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		fields := []zap.Field{
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
		}

		if err != nil {
			fields = append(fields, zap.Error(err))
			log.Error("gRPC call failed", fields...)
		} else {
			log.Info("gRPC call", fields...)
		}

		return resp, err
	}
}

func recoveryInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("Panic recovered",
					zap.String("method", info.FullMethod),
					zap.Any("panic", r),
				)
				err = fmt.Errorf("internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

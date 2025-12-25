package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Wuchinator/realtime-analytics/internal/analytics"
	"github.com/Wuchinator/realtime-analytics/internal/config"
	"github.com/Wuchinator/realtime-analytics/internal/query"
	"github.com/Wuchinator/realtime-analytics/pkg/logger"
	pb "github.com/Wuchinator/realtime-analytics/pkg/pb/analytics"
	"github.com/Wuchinator/realtime-analytics/pkg/postgres"
	_ "github.com/lib/pq"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
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

	log = logger.WithService(log, "query-service")
	log.Info("Starting Query Service",
		zap.String("environment", cfg.Environment),
		zap.String("grpc_port", "50052"),
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

	eventRepo := query.NewEventRepository(db.DB, log)
	analyticsRepo := analytics.NewRepository(db.DB, log)
	queryService := query.NewService(eventRepo, analyticsRepo, log)
	queryHandler := query.NewHandler(queryService, log)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			loggingInterceptor(log),
			recoveryInterceptor(log),
		),
	)

	pb.RegisterQueryServiceServer(grpcServer, queryHandler)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("query-service", grpc_health_v1.HealthCheckResponse_SERVING)

	reflection.Register(grpcServer)

	listener, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatal("Failed to create listener", zap.Error(err))
	}

	go func() {
		log.Info("gRPC server starting", zap.String("port", "50053"))
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal("Failed to serve gRPC", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down gracefully")

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	select {
	case <-stopped:
		log.Info("Server stopped gracefully")
	case <-ctx.Done():
		log.Warn("Shutdown timeout, forcing stop")
		grpcServer.Stop()
	}

	log.Info("Query Service stopped")
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

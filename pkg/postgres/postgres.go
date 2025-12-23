package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type DB struct {
	*sqlx.DB
	logger *zap.Logger
}

type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func New(config Config, logger *zap.Logger) (*DB, error) {
	db, err := sqlx.Connect("postgres", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("could not connect to postgres: %w", err)
	}

	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("could not ping postgres: %w", err)
	}

	logger.Info("PostgreSQL connected",
		zap.Int("max_open_conns", config.MaxOpenConns),
		zap.Int("max_idle_conns", config.MaxIdleConns),
		zap.Duration("conn_max_lifetime", config.ConnMaxLifetime),
	)

	return &DB{
		DB:     db,
		logger: logger,
	}, nil
}

func (db *DB) Close() error {
	err := db.DB.Close()
	if err != nil {
		db.logger.Error("could not close database", zap.Error(err))
		return fmt.Errorf("could not close postgres connection: %w", err)
	}
	db.logger.Info("postgres connection closed")
	return nil
}

// Проверяет состояние БД
func (db *DB) HealthCheck(ctx context.Context) error {
	return db.PingContext(ctx)
}

// Возвращает статистику connection pool
func (db *DB) GetStats() map[string]any {
	stats := db.Stats()
	return map[string]any{
		"open_connections":    stats.OpenConnections,
		"in_use":              stats.InUse,
		"idle":                stats.Idle,
		"wait_count":          stats.WaitCount,
		"wait_duration_ms":    stats.WaitDuration.Milliseconds(),
		"max_idle_closed":     stats.MaxIdleClosed,
		"max_lifetime_closed": stats.MaxLifetimeClosed,
	}
}

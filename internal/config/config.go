package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Environment string
	LogLevel    string
	GRPCPort    string
	Postgres    PostgresConfig
	Kafka       KafkaConfig
}

type PostgresConfig struct {
	Host            string
	Port            string
	Database        string
	Username        string
	Password        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	SSLMode         string
}

type KafkaConfig struct {
	Brokers          []string
	Topic            string
	ProducerRetries  int
	ProducerTimeout  time.Duration
	RequiredAcks     int
	CompressionType  string
	MaxMessageBytes  int
	IdempotentWrites bool
}

func Load() (*Config, error) {
	_ = godotenv.Load()
	cfg := &Config{
		Environment: getEnv("ENVIRONMENT", "development"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		GRPCPort:    getEnv("EVENT_SERVICE_PORT", "50051"),
	}

	cfg.Postgres = PostgresConfig{
		Host:            getEnv("POSTGRES_HOST", "localhost"),
		Port:            getEnv("POSTGRES_PORT", "5432"),
		Database:        getEnv("POSTGRES_DB", "analytics"),
		Username:        getEnv("POSTGRES_USER", "admin"),
		Password:        getEnv("POSTGRES_PASSWORD", "password"),
		MaxOpenConns:    getEnvAsInt("POSTGRES_MAX_OPEN_CONNS", 25),
		MaxIdleConns:    getEnvAsInt("POSTGRES_MAX_IDLE_CONNS", 5),
		ConnMaxLifetime: getEnvAsDuration("POSTGRES_CONN_MAX_LIFETIME", 5*time.Minute),
		SSLMode:         getEnv("POSTGRES_SSL_MODE", "disable"),
	}

	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	cfg.Kafka = KafkaConfig{
		Brokers:          strings.Split(brokers, ","),
		Topic:            getEnv("KAFKA_TOPIC_EVENTS", "user-events"),
		ProducerRetries:  getEnvAsInt("KAFKA_PRODUCER_RETRIES", 3),
		ProducerTimeout:  getEnvAsDuration("KAFKA_PRODUCER_TIMEOUT", 10*time.Second),
		RequiredAcks:     getEnvAsInt("KAFKA_REQUIRED_ACKS", -1), // -1 = все ISR реплики
		CompressionType:  getEnv("KAFKA_COMPRESSION", "snappy"),
		IdempotentWrites: getEnvAsBool("KAFKA_IDEMPOTENT", true),
		MaxMessageBytes:  getEnvAsInt("KAFKA_MAX_MESSAGE_BYTES", 1000000), // 1MB
	}

	return cfg, nil
}

func (c *PostgresConfig) PostgresDSN() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.Username, c.Password, c.Database, c.SSLMode)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := os.Getenv(key)
	if value, err := strconv.ParseBool(valueStr); err == nil {
		return value
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	valueStr := os.Getenv(key)
	if value, err := time.ParseDuration(valueStr); err == nil {
		return value
	}
	return defaultValue
}

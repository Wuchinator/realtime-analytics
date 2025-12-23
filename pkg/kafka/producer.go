package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
	logger   *zap.Logger
}

type ProducerConfig struct {
	Brokers []string
	Topic   string
	Retries int
	Timeout time.Duration

	RequiredAcks     int
	Compression      string
	IdempotentWrites bool
	MaxMessageBytes  int
}

func NewProducer(cfg ProducerConfig, logger *zap.Logger) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = cfg.Retries
	config.Producer.Timeout = cfg.Timeout
	config.Producer.RequiredAcks = sarama.RequiredAcks(cfg.RequiredAcks)
	config.Producer.Idempotent = cfg.IdempotentWrites

	if cfg.IdempotentWrites {
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 5
		config.Net.MaxOpenRequests = 1
	}

	switch cfg.Compression {
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	config.Producer.MaxMessageBytes = cfg.MaxMessageBytes
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Version = sarama.V3_3_0_0

	producer, err := sarama.NewSyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	logger.Info("Kafka producer initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", cfg.Topic),
		zap.Bool("idempotent", cfg.IdempotentWrites),
		zap.String("compression", cfg.Compression),
	)

	return &Producer{
		producer: producer,
		topic:    cfg.Topic,
		logger:   logger,
	}, nil
}

func (p *Producer) SendMessage(ctx context.Context, key string, value any) error {
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(valueBytes),

		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("timestamp"),
				Value: []byte(time.Now().Format(time.RFC3339Nano)),
			},
		},
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		p.logger.Error("Failed to send message to Kafka",
			zap.Error(err),
			zap.String("topic", p.topic),
			zap.String("key", key),
		)
		return fmt.Errorf("failed to send message: %w", err)
	}

	p.logger.Debug("Message sent to Kafka",
		zap.String("topic", p.topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.String("key", key),
	)

	return nil
}

func (p *Producer) SendMessageBatch(
	ctx context.Context,
	messages map[string]any) error {
	for key, value := range messages {
		if err := p.SendMessage(ctx, key, value); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) Close() error {
	err := p.producer.Close()
	if err != nil {
		p.logger.Error("Failed to close Kafka producer")
		return fmt.Errorf("failed to close producer: %w", err)
	}
	p.logger.Info("Kafka producer closed")
	return nil
}

package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type MessageHandler func(ctx context.Context, key, value []byte) error

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string
	handler       MessageHandler
	logger        *zap.Logger
	ready         chan bool
}

type ConsumerConfig struct {
	Brokers           []string
	Topics            []string
	GroupID           string
	AutoCommit        bool
	CommitInterval    time.Duration
	SessionTimeout    time.Duration
	RebalanceStrategy string
}

func NewConsumer(cfg ConsumerConfig, handler MessageHandler, logger *zap.Logger) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_0_0
	config.Consumer.Return.Errors = true

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = cfg.AutoCommit
	config.Consumer.Offsets.AutoCommit.Interval = cfg.CommitInterval

	config.Consumer.Group.Session.Timeout = cfg.SessionTimeout

	config.Consumer.Group.Heartbeat.Interval = cfg.SessionTimeout / 3

	// Range - партиции распределяются последовательно (default)
	// RoundRobin - партиции распределяются равномерно
	// Sticky - сохраняет назначения при rebalance (меньше движения данных)
	switch cfg.RebalanceStrategy {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
			sarama.NewBalanceStrategySticky(),
		}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
			sarama.NewBalanceStrategyRoundRobin(),
		}
	default:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
			sarama.NewBalanceStrategyRange(),
		}
	}

	// Создание consumer group
	consumerGroup, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	logger.Info("Kafka consumer initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.Strings("topics", cfg.Topics),
		zap.String("group_id", cfg.GroupID),
	)

	return &Consumer{
		consumerGroup: consumerGroup,
		topics:        cfg.Topics,
		handler:       handler,
		logger:        logger,
		ready:         make(chan bool),
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// Consume блокируется до тех пор, пока:
			// 1. Не произойдёт rebalance
			// 2. Не закроется context
			if err := c.consumerGroup.Consume(ctx, c.topics, c); err != nil {
				c.logger.Error("Error from consumer", zap.Error(err))
			}

			// Проверяем не закрыт ли context
			if ctx.Err() != nil {
				c.logger.Info("Context cancelled, stopping consumer")
				return
			}

			c.ready = make(chan bool)
		}
	}()
	<-c.ready
	c.logger.Info("Kafka consumer started and ready")

	wg.Wait()
	return nil
}

func (c *Consumer) Close() error {
	if err := c.consumerGroup.Close(); err != nil {
		c.logger.Error("Failed to close consumer group", zap.Error(err))
		return err
	}
	c.logger.Info("Kafka consumer closed")
	return nil
}

// Setup вызывается при старте новой session (после rebalance)
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	c.logger.Info("Consumer group rebalanced")
	close(c.ready)
	return nil
}

// Cleanup вызывается в конце session
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim обрабатывает сообщения из конкретной партиции
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			c.logger.Debug("Message received",
				zap.String("topic", message.Topic),
				zap.Int32("partition", message.Partition),
				zap.Int64("offset", message.Offset),
				zap.String("key", string(message.Key)),
			)

			// Обрабатываем сообщение
			if err := c.handler(session.Context(), message.Key, message.Value); err != nil {
				c.logger.Error("Failed to process message",
					zap.Error(err),
					zap.String("topic", message.Topic),
					zap.Int32("partition", message.Partition),
					zap.Int64("offset", message.Offset),
				)

			}
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// WaitReady ждёт пока consumer будет готов
func (c *Consumer) WaitReady() <-chan bool {
	return c.ready
}

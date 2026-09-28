package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/ThalaPravin/RideMesh/pkg/config"
	"github.com/ThalaPravin/RideMesh/pkg/kafka"
)

// PaymentConsumer listens for completed trips and executes billing
type PaymentConsumer struct {
	repo     *PaymentRepository
	producer *kafka.Producer
	cfg      *config.Config
	logger   *zap.Logger
}

// NewPaymentConsumer creates a new PaymentConsumer instance
func NewPaymentConsumer(repo *PaymentRepository, producer *kafka.Producer, cfg *config.Config, logger *zap.Logger) *PaymentConsumer {
	return &PaymentConsumer{
		repo:     repo,
		producer: producer,
		cfg:      cfg,
		logger:   logger,
	}
}

// StartConsumers hooks background consumption to Uber Fx lifecycle
func StartConsumers(lc fx.Lifecycle, c *PaymentConsumer) {
	tripCompletedReader := kafka.NewReader(c.cfg, "payment-service-group", kafka.TopicTripCompleted)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			c.logger.Info("Starting Payment Service Kafka consumers...")
			go c.consumeTripCompleted(tripCompletedReader)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			c.logger.Info("Stopping Payment Service Kafka consumer...")
			return tripCompletedReader.Close()
		},
	})
}

func (c *PaymentConsumer) consumeTripCompleted(reader *kafka.Reader) {
	for {
		ctx := context.Background()
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			c.logger.Error("Error reading trip.completed message from Kafka", zap.Error(err))
			time.Sleep(1 * time.Second)
			continue
		}

		var event kafka.TripCompletedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.logger.Error("Error unmarshaling trip.completed event", zap.Error(err))
			continue
		}

		c.logger.Info("Payment Service received trip.completed event",
			zap.String("trip_id", event.Payload.TripID),
			zap.Float64("amount", event.Payload.Amount),
		)

		// Process payment and save transaction record to PostgreSQL
		p, err := c.repo.CreatePayment(ctx, event.Payload.TripID, event.Payload.UserID, event.Payload.Amount, "SUCCESS")
		if err != nil {
			c.logger.Error("Failed to record payment transaction in database", zap.Error(err))
			continue
		}

		// Emit payment.processed event to Kafka
		processedEvent := kafka.PaymentProcessedEvent{
			BaseEvent: kafka.BaseEvent{
				EventID:   uuid.New().String(),
				EventType: "PAYMENT_PROCESSED",
				Timestamp: time.Now(),
			},
			Payload: kafka.PaymentProcessedPayload{
				TransactionID: p.TransactionID,
				TripID:        p.TripID,
				UserID:        p.UserID,
				Amount:        p.Amount,
				Status:        p.Status,
			},
		}

		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := c.producer.Publish(pubCtx, kafka.TopicPaymentProcessed, p.TripID, processedEvent); err != nil {
			c.logger.Error("Failed to publish payment.processed event to Kafka", zap.Error(err))
		} else {
			c.logger.Info("Successfully processed payment and published payment.processed event",
				zap.String("transaction_id", p.TransactionID),
				zap.String("trip_id", p.TripID),
			)
		}
		cancel()
	}
}

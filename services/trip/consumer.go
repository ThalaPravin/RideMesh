package main

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/ThalaPravin/RideMesh/pkg/config"
	"github.com/ThalaPravin/RideMesh/pkg/kafka"
)

// TripConsumer manages background Kafka consumption for the Trip Service
type TripConsumer struct {
	repo   *TripRepository
	cfg    *config.Config
	logger *zap.Logger
}

// NewTripConsumer creates a new TripConsumer instance
func NewTripConsumer(repo *TripRepository, cfg *config.Config, logger *zap.Logger) *TripConsumer {
	return &TripConsumer{
		repo:   repo,
		cfg:    cfg,
		logger: logger,
	}
}

// StartConsumers launches background goroutines listening to Kafka topics
func StartConsumers(lc fx.Lifecycle, c *TripConsumer) {
	matchedReader := kafka.NewReader(c.cfg, "trip-service-matched-group", kafka.TopicTripMatched)
	paymentReader := kafka.NewReader(c.cfg, "trip-service-payment-group", kafka.TopicPaymentProcessed)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			c.logger.Info("Starting Trip Service Kafka event consumers...")
			go c.consumeTripMatched(matchedReader)
			go c.consumePaymentProcessed(paymentReader)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			c.logger.Info("Stopping Trip Service Kafka consumers...")
			_ = matchedReader.Close()
			_ = paymentReader.Close()
			return nil
		},
	})
}

func (c *TripConsumer) consumeTripMatched(reader *kafka.Reader) {
	for {
		ctx := context.Background()
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			c.logger.Error("Error reading trip.matched message", zap.Error(err))
			time.Sleep(1 * time.Second)
			continue
		}

		var event kafka.TripMatchedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.logger.Error("Error unmarshaling trip.matched event", zap.Error(err))
			continue
		}

		c.logger.Info("Trip Matched event received from Kafka",
			zap.String("trip_id", event.Payload.TripID),
			zap.String("driver_id", event.Payload.DriverID),
		)

		_, err = c.repo.AssignDriver(ctx, event.Payload.TripID, event.Payload.DriverID)
		if err != nil {
			c.logger.Error("Failed to assign driver in database", zap.Error(err))
			continue
		}

		c.logger.Info("Successfully bound driver to trip",
			zap.String("trip_id", event.Payload.TripID),
			zap.String("driver_id", event.Payload.DriverID),
		)
	}
}

func (c *TripConsumer) consumePaymentProcessed(reader *kafka.Reader) {
	for {
		ctx := context.Background()
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			c.logger.Error("Error reading payment.processed message", zap.Error(err))
			time.Sleep(1 * time.Second)
			continue
		}

		var event kafka.PaymentProcessedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.logger.Error("Error unmarshaling payment.processed event", zap.Error(err))
			continue
		}

		c.logger.Info("Payment Processed event received from Kafka",
			zap.String("trip_id", event.Payload.TripID),
			zap.String("transaction_id", event.Payload.TransactionID),
			zap.String("status", event.Payload.Status),
		)
	}
}

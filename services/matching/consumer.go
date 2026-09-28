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
	"github.com/ThalaPravin/RideMesh/pkg/redis"
)

// Fallback driver ID when Redis has no pings yet (seeded Goku driver ID)
const FallbackDriverID = "acb336c4-15f2-47f6-8080-d744173a1f3e"

// MatchingConsumer listens for trip requests and executes dispatch matching
type MatchingConsumer struct {
	redisClient *redis.Client
	producer    *kafka.Producer
	cfg         *config.Config
	logger      *zap.Logger
}

// NewMatchingConsumer creates a new MatchingConsumer instance
func NewMatchingConsumer(redisClient *redis.Client, producer *kafka.Producer, cfg *config.Config, logger *zap.Logger) *MatchingConsumer {
	return &MatchingConsumer{
		redisClient: redisClient,
		producer:    producer,
		cfg:         cfg,
		logger:      logger,
	}
}

// StartConsumers hooks background consumption to Uber Fx lifecycle
func StartConsumers(lc fx.Lifecycle, c *MatchingConsumer) {
	tripRequestedReader := kafka.NewReader(c.cfg, "matching-service-group", kafka.TopicTripRequested)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			c.logger.Info("Starting Matching Service Kafka consumers...")
			go c.consumeTripRequested(tripRequestedReader)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			c.logger.Info("Stopping Matching Service Kafka consumer...")
			return tripRequestedReader.Close()
		},
	})
}

func (c *MatchingConsumer) consumeTripRequested(reader *kafka.Reader) {
	for {
		ctx := context.Background()
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			c.logger.Error("Error reading trip.requested message from Kafka", zap.Error(err))
			time.Sleep(1 * time.Second)
			continue
		}

		var event kafka.TripRequestedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.logger.Error("Error unmarshaling trip.requested event", zap.Error(err))
			continue
		}

		c.logger.Info("Matching Service received trip.requested event",
			zap.String("trip_id", event.Payload.TripID),
			zap.Float64("pickup_lat", event.Payload.PickupLat),
			zap.Float64("pickup_lon", event.Payload.PickupLon),
		)

		// Search nearby available drivers using Redis Geospatial index
		driverIDs, err := c.redisClient.FindNearbyDrivers(ctx, event.Payload.PickupLat, event.Payload.PickupLon, 5.0)
		matchedDriverID := FallbackDriverID

		if err == nil && len(driverIDs) > 0 {
			matchedDriverID = driverIDs[0]
			c.logger.Info("Found driver via Redis GEORADIUS query", zap.String("driver_id", matchedDriverID))
		} else {
			c.logger.Info("Using fallback driver assignment", zap.String("driver_id", matchedDriverID))
		}

		// Emit trip.matched event to Kafka
		matchedEvent := kafka.TripMatchedEvent{
			BaseEvent: kafka.BaseEvent{
				EventID:   uuid.New().String(),
				EventType: "TRIP_MATCHED",
				Timestamp: time.Now(),
			},
			Payload: kafka.TripMatchedPayload{
				TripID:   event.Payload.TripID,
				DriverID: matchedDriverID,
				UserID:   event.Payload.UserID,
			},
		}

		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := c.producer.Publish(pubCtx, kafka.TopicTripMatched, event.Payload.TripID, matchedEvent); err != nil {
			c.logger.Error("Failed to publish trip.matched event to Kafka", zap.Error(err))
		} else {
			c.logger.Info("Successfully matched trip and published trip.matched event",
				zap.String("trip_id", event.Payload.TripID),
				zap.String("driver_id", matchedDriverID),
			)
		}
		cancel()
	}
}

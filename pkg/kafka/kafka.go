package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/ThalaPravin/RideMesh/pkg/config"
)

// Topic names constants
const (
	TopicTripRequested    = "trip.requested"
	TopicTripMatched      = "trip.matched"
	TopicTripCompleted    = "trip.completed"
	TopicPaymentProcessed = "payment.processed"
)

// BaseEvent represents the common metadata envelope for all Kafka events
type BaseEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
}

// TripRequestedPayload payload for trip.requested event
type TripRequestedPayload struct {
	TripID      string  `json:"trip_id"`
	UserID      string  `json:"user_id"`
	PickupLat   float64 `json:"pickup_lat"`
	PickupLon   float64 `json:"pickup_lon"`
	DestLat     float64 `json:"dest_lat"`
	DestLon     float64 `json:"dest_lon"`
	VehicleType string  `json:"vehicle_type"`
	Amount      float64 `json:"amount"`
}

// TripRequestedEvent complete message structure
type TripRequestedEvent struct {
	BaseEvent
	Payload TripRequestedPayload `json:"payload"`
}

// TripMatchedPayload payload for trip.matched event
type TripMatchedPayload struct {
	TripID   string `json:"trip_id"`
	DriverID string `json:"driver_id"`
	UserID   string `json:"user_id"`
}

// TripMatchedEvent complete message structure
type TripMatchedEvent struct {
	BaseEvent
	Payload TripMatchedPayload `json:"payload"`
}

// TripCompletedPayload payload for trip.completed event
type TripCompletedPayload struct {
	TripID   string  `json:"trip_id"`
	UserID   string  `json:"user_id"`
	DriverID string  `json:"driver_id"`
	Amount   float64 `json:"amount"`
}

// TripCompletedEvent complete message structure
type TripCompletedEvent struct {
	BaseEvent
	Payload TripCompletedPayload `json:"payload"`
}

// PaymentProcessedPayload payload for payment.processed event
type PaymentProcessedPayload struct {
	TransactionID string  `json:"transaction_id"`
	TripID        string  `json:"trip_id"`
	UserID        string  `json:"user_id"`
	Amount        float64 `json:"amount"`
	Status        string  `json:"status"` // SUCCESS, FAILED
}

// PaymentProcessedEvent complete message structure
type PaymentProcessedEvent struct {
	BaseEvent
	Payload PaymentProcessedPayload `json:"payload"`
}

// Producer manages Kafka writing operations
type Producer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// NewProducer constructs a thread-safe Kafka writer using segmentio/kafka-go
func NewProducer(lc fx.Lifecycle, cfg *config.Config, log *zap.Logger) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers),
		Balancer:     &kafka.LeastBytes{},
		Async:        true,
		RequiredAcks: kafka.RequireOne,
	}

	p := &Producer{
		writer: writer,
		logger: log,
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			log.Info("Closing Kafka Producer connection...")
			return writer.Close()
		},
	})

	return p
}

// Publish serializes data to JSON and pushes to the target topic
func (p *Producer) Publish(ctx context.Context, topic string, key string, event interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		p.logger.Error("Failed to marshal event JSON", zap.Error(err))
		return err
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Error("Failed to publish message to Kafka", zap.String("topic", topic), zap.Error(err))
		return err
	}

	p.logger.Info("Successfully published Kafka message", zap.String("topic", topic), zap.String("key", key))
	return nil
}

// NewReader constructs a Kafka consumer group reader for a specified topic
func NewReader(cfg *config.Config, groupID string, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.KafkaBrokers},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10B,  // 10B
		MaxBytes: 10MB, // 10MB
	})
}

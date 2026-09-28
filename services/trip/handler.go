package main

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ThalaPravin/RideMesh/pkg/kafka"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// TripHandler implements the gRPC TripServiceServer interface
type TripHandler struct {
	pb.UnimplementedTripServiceServer
	repo     *TripRepository
	producer *kafka.Producer
	logger   *zap.Logger
}

// NewTripHandler creates a new TripHandler instance
func NewTripHandler(repo *TripRepository, producer *kafka.Producer, logger *zap.Logger) *TripHandler {
	return &TripHandler{
		repo:     repo,
		producer: producer,
		logger:   logger,
	}
}

// CreateTrip registers a new ride request and triggers event routing via Kafka
func (h *TripHandler) CreateTrip(ctx context.Context, req *pb.CreateTripRequest) (*pb.TripResponse, error) {
	h.logger.Info("CreateTrip request received", zap.String("user_id", req.GetUserId()), zap.String("vehicle_type", req.GetVehicleType()))

	if req.GetUserId() == "" || req.GetPickup() == nil || req.GetDestination() == nil {
		return nil, status.Error(codes.InvalidArgument, "user_id, pickup, and destination are required")
	}

	amount := 350.0 // Default calculated ride fare

	t, err := h.repo.CreateTrip(
		ctx,
		req.GetUserId(),
		req.GetPickup().GetLatitude(),
		req.GetPickup().GetLongitude(),
		req.GetDestination().GetLatitude(),
		req.GetDestination().GetLongitude(),
		amount,
	)
	if err != nil {
		h.logger.Error("Failed to save trip to database", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to create trip")
	}

	// Publish trip.requested event to Kafka
	event := kafka.TripRequestedEvent{
		BaseEvent: kafka.BaseEvent{
			EventID:   uuid.New().String(),
			EventType: "TRIP_REQUESTED",
			Timestamp: time.Now(),
		},
		Payload: kafka.TripRequestedPayload{
			TripID:      t.ID,
			UserID:      t.UserID,
			PickupLat:   t.PickupLat,
			PickupLon:   t.PickupLon,
			DestLat:     t.DestLat,
			DestLon:     t.DestLon,
			VehicleType: req.GetVehicleType(),
			Amount:      t.Amount,
		},
	}

	go func() {
		pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.producer.Publish(pubCtx, kafka.TopicTripRequested, t.ID, event); err != nil {
			h.logger.Error("Async failed to publish trip.requested event", zap.Error(err))
		}
	}()

	return &pb.TripResponse{
		Id:          t.ID,
		UserId:      t.UserID,
		DriverId:    "",
		Pickup:      req.GetPickup(),
		Destination: req.GetDestination(),
		Amount:      t.Amount,
		Status:      t.Status,
		CreatedAt:   t.CreatedAt.Format(time.RFC3339),
	}, nil
}

// GetTrip retrieves current details of a trip
func (h *TripHandler) GetTrip(ctx context.Context, req *pb.GetTripRequest) (*pb.TripResponse, error) {
	h.logger.Info("GetTrip request received", zap.String("trip_id", req.GetTripId()))

	if req.GetTripId() == "" {
		return nil, status.Error(codes.InvalidArgument, "trip ID is required")
	}

	t, err := h.repo.GetTripByID(ctx, req.GetTripId())
	if err != nil {
		if errors.Is(err, ErrTripNotFound) {
			return nil, status.Error(codes.NotFound, "trip not found")
		}
		h.logger.Error("Failed to query trip by ID", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to retrieve trip")
	}

	driverID := ""
	if t.DriverID.Valid {
		driverID = t.DriverID.String
	}

	startedAt := ""
	if t.StartedAt.Valid {
		startedAt = t.StartedAt.Time.Format(time.RFC3339)
	}

	endedAt := ""
	if t.EndedAt.Valid {
		endedAt = t.EndedAt.Time.Format(time.RFC3339)
	}

	return &pb.TripResponse{
		Id:        t.ID,
		UserId:    t.UserID,
		DriverId:  driverID,
		Pickup:    &pb.Location{Latitude: t.PickupLat, Longitude: t.PickupLon},
		Destination: &pb.Location{Latitude: t.DestLat, Longitude: t.DestLon},
		Amount:    t.Amount,
		Status:    t.Status,
		CreatedAt: t.CreatedAt.Format(time.RFC3339),
		StartedAt: startedAt,
		EndedAt:   endedAt,
	}, nil
}

// UpdateTripStatus handles state updates and notifies Kafka subscribers on completion
func (h *TripHandler) UpdateTripStatus(ctx context.Context, req *pb.UpdateTripStatusRequest) (*pb.TripResponse, error) {
	h.logger.Info("UpdateTripStatus request received", zap.String("trip_id", req.GetTripId()), zap.String("status", req.GetStatus()))

	if req.GetTripId() == "" || req.GetStatus() == "" {
		return nil, status.Error(codes.InvalidArgument, "trip_id and status are required")
	}

	t, err := h.repo.UpdateTripStatus(ctx, req.GetTripId(), req.GetStatus())
	if err != nil {
		if errors.Is(err, ErrTripNotFound) {
			return nil, status.Error(codes.NotFound, "trip not found")
		}
		h.logger.Error("Failed to update trip status in database", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to update status")
	}

	driverID := ""
	if t.DriverID.Valid {
		driverID = t.DriverID.String
	}

	// If trip completed, publish trip.completed event to Kafka
	if req.GetStatus() == "COMPLETED" {
		event := kafka.TripCompletedEvent{
			BaseEvent: kafka.BaseEvent{
				EventID:   uuid.New().String(),
				EventType: "TRIP_COMPLETED",
				Timestamp: time.Now(),
			},
			Payload: kafka.TripCompletedPayload{
				TripID:   t.ID,
				UserID:   t.UserID,
				DriverID: driverID,
				Amount:   t.Amount,
			},
		}

		go func() {
			pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.producer.Publish(pubCtx, kafka.TopicTripCompleted, t.ID, event); err != nil {
				h.logger.Error("Async failed to publish trip.completed event", zap.Error(err))
			}
		}()
	}

	return &pb.TripResponse{
		Id:        t.ID,
		UserId:    t.UserID,
		DriverId:  driverID,
		Pickup:    &pb.Location{Latitude: t.PickupLat, Longitude: t.PickupLon},
		Destination: &pb.Location{Latitude: t.DestLat, Longitude: t.DestLon},
		Amount:    t.Amount,
		Status:    t.Status,
		CreatedAt: t.CreatedAt.Format(time.RFC3339),
	}, nil
}

// CancelTrip cancels an active or requested trip
func (h *TripHandler) CancelTrip(ctx context.Context, req *pb.CancelTripRequest) (*pb.TripResponse, error) {
	h.logger.Info("CancelTrip request received", zap.String("trip_id", req.GetTripId()), zap.String("reason", req.GetReason()))

	if req.GetTripId() == "" {
		return nil, status.Error(codes.InvalidArgument, "trip ID is required")
	}

	t, err := h.repo.CancelTrip(ctx, req.GetTripId())
	if err != nil {
		if errors.Is(err, ErrTripNotFound) {
			return nil, status.Error(codes.NotFound, "trip not found")
		}
		h.logger.Error("Failed to cancel trip in database", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to cancel trip")
	}

	return &pb.TripResponse{
		Id:        t.ID,
		UserId:    t.UserID,
		DriverId:  "",
		Pickup:    &pb.Location{Latitude: t.PickupLat, Longitude: t.PickupLon},
		Destination: &pb.Location{Latitude: t.DestLat, Longitude: t.DestLon},
		Amount:    t.Amount,
		Status:    t.Status,
		CreatedAt: t.CreatedAt.Format(time.RFC3339),
	}, nil
}

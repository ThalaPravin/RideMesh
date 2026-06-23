package main

import (
	"context"
	"go.uber.org/zap"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// TripHandler implements the gRPC TripServiceServer interface
type TripHandler struct {
	pb.UnimplementedTripServiceServer
	logger *zap.Logger
}

// NewTripHandler creates a new TripHandler instance
func NewTripHandler(logger *zap.Logger) *TripHandler {
	return &TripHandler{
		logger: logger,
	}
}

// CreateTrip registers a new ride request and triggers event routing
func (h *TripHandler) CreateTrip(ctx context.Context, req *pb.CreateTripRequest) (*pb.TripResponse, error) {
	h.logger.Info("CreateTrip request received", zap.String("user_id", req.GetUserId()), zap.String("vehicle_type", req.GetVehicleType()))

	// TODO: Publish trip.requested to Kafka and save to PG in Phase 3
	return &pb.TripResponse{
		Id:          "dummy-trip-uuid",
		UserId:      req.GetUserId(),
		DriverId:    "",
		Pickup:      req.GetPickup(),
		Destination: req.GetDestination(),
		Amount:      350.0,
		Status:      "REQUESTED",
		CreatedAt:   "2026-06-23T01:20:00Z",
	}, nil
}

// GetTrip retrieves current details of a trip
func (h *TripHandler) GetTrip(ctx context.Context, req *pb.GetTripRequest) (*pb.TripResponse, error) {
	h.logger.Info("GetTrip request received", zap.String("trip_id", req.GetTripId()))

	// TODO: Query from PostgreSQL in Phase 3
	return &pb.TripResponse{
		Id:        req.GetTripId(),
		UserId:    "dummy-user-uuid",
		DriverId:  "dummy-driver-uuid",
		Pickup:    &pb.Location{Latitude: 12.9716, Longitude: 77.5946},
		Destination: &pb.Location{Latitude: 12.9279, Longitude: 77.6271},
		Amount:    350.0,
		Status:    "ASSIGNED",
		CreatedAt: "2026-06-23T01:20:00Z",
	}, nil
}

// UpdateTripStatus handles state updates and notifies subscribers
func (h *TripHandler) UpdateTripStatus(ctx context.Context, req *pb.UpdateTripStatusRequest) (*pb.TripResponse, error) {
	h.logger.Info("UpdateTripStatus request received", zap.String("trip_id", req.GetTripId()), zap.String("status", req.GetStatus()))

	// TODO: Persist state changes and broadcast Kafka lifecycle events in Phase 3
	return &pb.TripResponse{
		Id:        req.GetTripId(),
		UserId:    "dummy-user-uuid",
		DriverId:  req.GetDriverId(),
		Pickup:    &pb.Location{Latitude: 12.9716, Longitude: 77.5946},
		Destination: &pb.Location{Latitude: 12.9279, Longitude: 77.6271},
		Amount:    350.0,
		Status:    req.GetStatus(),
		CreatedAt: "2026-06-23T01:20:00Z",
	}, nil
}

// CancelTrip cancels an active or requested trip
func (h *TripHandler) CancelTrip(ctx context.Context, req *pb.CancelTripRequest) (*pb.TripResponse, error) {
	h.logger.Info("CancelTrip request received", zap.String("trip_id", req.GetTripId()), zap.String("reason", req.GetReason()))

	// TODO: Verify cancellation eligibility and cancel in Phase 3
	return &pb.TripResponse{
		Id:        req.GetTripId(),
		UserId:    req.GetUserId(),
		DriverId:  "dummy-driver-uuid",
		Pickup:    &pb.Location{Latitude: 12.9716, Longitude: 77.5946},
		Destination: &pb.Location{Latitude: 12.9279, Longitude: 77.6271},
		Amount:    350.0,
		Status:    "CANCELED",
		CreatedAt: "2026-06-23T01:20:00Z",
	}, nil
}

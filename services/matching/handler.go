package main

import (
	"context"
	"go.uber.org/zap"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// MatchingHandler implements the gRPC MatchingServiceServer interface
type MatchingHandler struct {
	pb.UnimplementedMatchingServiceServer
	logger *zap.Logger
}

// NewMatchingHandler creates a new MatchingHandler instance
func NewMatchingHandler(logger *zap.Logger) *MatchingHandler {
	return &MatchingHandler{
		logger: logger,
	}
}

// UpdateLocation handles real-time coordinates reporting from drivers
func (h *MatchingHandler) UpdateLocation(ctx context.Context, req *pb.UpdateLocationRequest) (*pb.UpdateLocationResponse, error) {
	h.logger.Info("UpdateLocation request received", 
		zap.String("driver_id", req.GetDriverId()), 
		zap.Float64("latitude", req.GetLocation().GetLatitude()),
		zap.Float64("longitude", req.GetLocation().GetLongitude()),
	)

	// TODO: Save location update in Redis GEO index in Phase 4
	return &pb.UpdateLocationResponse{
		Success: true,
	}, nil
}

// GetNearbyDrivers fetches nearest online drivers within a radius
func (h *MatchingHandler) GetNearbyDrivers(ctx context.Context, req *pb.GetNearbyDriversRequest) (*pb.GetNearbyDriversResponse, error) {
	h.logger.Info("GetNearbyDrivers request received", 
		zap.Float64("latitude", req.GetLocation().GetLatitude()),
		zap.Float64("longitude", req.GetLocation().GetLongitude()),
		zap.Float64("radius_km", req.GetRadiusKm()),
	)

	// TODO: Search drivers using Redis GEOSEARCH in Phase 4
	return &pb.GetNearbyDriversResponse{
		DriverIds: []string{"dummy-driver-uuid-1", "dummy-driver-uuid-2"},
	}, nil
}

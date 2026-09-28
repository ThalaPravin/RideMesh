package main

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ThalaPravin/RideMesh/pkg/redis"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// MatchingHandler implements the gRPC MatchingServiceServer interface
type MatchingHandler struct {
	pb.UnimplementedMatchingServiceServer
	redisClient *redis.Client
	logger      *zap.Logger
}

// NewMatchingHandler creates a new MatchingHandler instance
func NewMatchingHandler(redisClient *redis.Client, logger *zap.Logger) *MatchingHandler {
	return &MatchingHandler{
		redisClient: redisClient,
		logger:      logger,
	}
}

// UpdateLocation handles real-time coordinates reporting from drivers
func (h *MatchingHandler) UpdateLocation(ctx context.Context, req *pb.UpdateLocationRequest) (*pb.UpdateLocationResponse, error) {
	h.logger.Info("UpdateLocation request received",
		zap.String("driver_id", req.GetDriverId()),
		zap.Float64("latitude", req.GetLocation().GetLatitude()),
		zap.Float64("longitude", req.GetLocation().GetLongitude()),
	)

	if req.GetDriverId() == "" || req.GetLocation() == nil {
		return nil, status.Error(codes.InvalidArgument, "driver_id and location are required")
	}

	err := h.redisClient.AddDriverLocation(
		ctx,
		req.GetDriverId(),
		req.GetLocation().GetLatitude(),
		req.GetLocation().GetLongitude(),
	)
	if err != nil {
		h.logger.Error("Failed to update driver location in Redis", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to update driver location")
	}

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

	if req.GetLocation() == nil {
		return nil, status.Error(codes.InvalidArgument, "location is required")
	}

	radiusKm := req.GetRadiusKm()
	if radiusKm <= 0 {
		radiusKm = 5.0 // Default 5 km radius search
	}

	driverIDs, err := h.redisClient.FindNearbyDrivers(
		ctx,
		req.GetLocation().GetLatitude(),
		req.GetLocation().GetLongitude(),
		radiusKm,
	)
	if err != nil {
		h.logger.Error("Failed to query nearby drivers from Redis", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to search nearby drivers")
	}

	return &pb.GetNearbyDriversResponse{
		DriverIds: driverIDs,
	}, nil
}

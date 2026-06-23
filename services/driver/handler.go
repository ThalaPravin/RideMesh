package main

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/ThalaPravin/RideMesh/proto"
)

// DriverHandler implements the gRPC DriverServiceServer interface
type DriverHandler struct {
	pb.UnimplementedDriverServiceServer
	repo   *DriverRepository
	logger *zap.Logger
}

// NewDriverHandler creates a new DriverHandler instance
func NewDriverHandler(repo *DriverRepository, logger *zap.Logger) *DriverHandler {
	return &DriverHandler{
		repo:   repo,
		logger: logger,
	}
}

// RegisterDriver handles driver registration
func (h *DriverHandler) RegisterDriver(ctx context.Context, req *pb.RegisterDriverRequest) (*pb.DriverResponse, error) {
	h.logger.Info("RegisterDriver request received", zap.String("phone_no", req.GetPhoneNo()))

	if req.GetPhoneNo() == "" || req.GetFirstName() == "" || req.GetVehicleNumber() == "" {
		return nil, status.Error(codes.InvalidArgument, "first_name, phone_no, and vehicle_number are required")
	}

	d, err := h.repo.CreateDriver(
		ctx,
		req.GetFirstName(),
		req.GetLastName(),
		req.GetPhoneNo(),
		req.GetVehicleModel(),
		req.GetVehicleNumber(),
		req.GetVehicleType(),
	)
	if err != nil {
		if errors.Is(err, ErrDriverAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, "driver with this phone or vehicle number already exists")
		}
		h.logger.Error("Failed to register driver in database", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to register driver")
	}

	return &pb.DriverResponse{
		Id:            d.ID,
		FirstName:     d.FirstName,
		LastName:      d.LastName,
		PhoneNo:       d.PhoneNo,
		VehicleModel:  d.VehicleModel,
		VehicleNumber: d.VehicleNumber,
		VehicleType:   d.VehicleType,
		IsOnline:      d.IsOnline,
		Rating:        d.Rating,
	}, nil
}

// UpdateDriverStatus toggles driver online/offline status
func (h *DriverHandler) UpdateDriverStatus(ctx context.Context, req *pb.UpdateDriverStatusRequest) (*pb.DriverResponse, error) {
	h.logger.Info("UpdateDriverStatus request received", zap.String("driver_id", req.GetDriverId()), zap.Bool("is_online", req.GetIsOnline()))

	if req.GetDriverId() == "" {
		return nil, status.Error(codes.InvalidArgument, "driver ID is required")
	}

	d, err := h.repo.UpdateDriverStatus(ctx, req.GetDriverId(), req.GetIsOnline())
	if err != nil {
		if errors.Is(err, ErrDriverNotFound) {
			return nil, status.Error(codes.NotFound, "driver not found")
		}
		h.logger.Error("Failed to update driver status in database", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to update status")
	}

	return &pb.DriverResponse{
		Id:            d.ID,
		FirstName:     d.FirstName,
		LastName:      d.LastName,
		PhoneNo:       d.PhoneNo,
		VehicleModel:  d.VehicleModel,
		VehicleNumber: d.VehicleNumber,
		VehicleType:   d.VehicleType,
		IsOnline:      d.IsOnline,
		Rating:        d.Rating,
	}, nil
}

// GetDriver retrieves driver and vehicle information
func (h *DriverHandler) GetDriver(ctx context.Context, req *pb.GetDriverRequest) (*pb.DriverResponse, error) {
	h.logger.Info("GetDriver request received", zap.String("driver_id", req.GetDriverId()))

	if req.GetDriverId() == "" {
		return nil, status.Error(codes.InvalidArgument, "driver ID is required")
	}

	d, err := h.repo.GetDriverByID(ctx, req.GetDriverId())
	if err != nil {
		if errors.Is(err, ErrDriverNotFound) {
			return nil, status.Error(codes.NotFound, "driver not found")
		}
		h.logger.Error("Failed to query driver by ID", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to retrieve profile")
	}

	return &pb.DriverResponse{
		Id:            d.ID,
		FirstName:     d.FirstName,
		LastName:      d.LastName,
		PhoneNo:       d.PhoneNo,
		VehicleModel:  d.VehicleModel,
		VehicleNumber: d.VehicleNumber,
		VehicleType:   d.VehicleType,
		IsOnline:      d.IsOnline,
		Rating:        d.Rating,
	}, nil
}

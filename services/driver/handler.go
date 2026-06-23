package main

import (
	"context"
	"go.uber.org/zap"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// DriverHandler implements the gRPC DriverServiceServer interface
type DriverHandler struct {
	pb.UnimplementedDriverServiceServer
	logger *zap.Logger
}

// NewDriverHandler creates a new DriverHandler instance
func NewDriverHandler(logger *zap.Logger) *DriverHandler {
	return &DriverHandler{
		logger: logger,
	}
}

// RegisterDriver handles driver registration
func (h *DriverHandler) RegisterDriver(ctx context.Context, req *pb.RegisterDriverRequest) (*pb.DriverResponse, error) {
	h.logger.Info("RegisterDriver request received", zap.String("phone_no", req.GetPhoneNo()))

	// TODO: Implement driver registration in Phase 2
	return &pb.DriverResponse{
		Id:            "dummy-driver-uuid",
		FirstName:     req.GetFirstName(),
		LastName:      req.GetLastName(),
		PhoneNo:       req.GetPhoneNo(),
		VehicleModel:  req.GetVehicleModel(),
		VehicleNumber: req.GetVehicleNumber(),
		VehicleType:   req.GetVehicleType(),
		IsOnline:      false,
		Rating:        5.0,
	}, nil
}

// UpdateDriverStatus toggles driver online/offline status
func (h *DriverHandler) UpdateDriverStatus(ctx context.Context, req *pb.UpdateDriverStatusRequest) (*pb.DriverResponse, error) {
	h.logger.Info("UpdateDriverStatus request received", zap.String("driver_id", req.GetDriverId()), zap.Bool("is_online", req.GetIsOnline()))

	// TODO: Implement status updates & DB persistence in Phase 2
	return &pb.DriverResponse{
		Id:            req.GetDriverId(),
		FirstName:     "Goku",
		LastName:      "Son",
		PhoneNo:       "11111111",
		VehicleModel:  "Honda CR-V",
		VehicleNumber: "MH 01A B 4321",
		VehicleType:   "SUV",
		IsOnline:      req.GetIsOnline(),
		Rating:        4.9,
	}, nil
}

// GetDriver retrieves driver and vehicle information
func (h *DriverHandler) GetDriver(ctx context.Context, req *pb.GetDriverRequest) (*pb.DriverResponse, error) {
	h.logger.Info("GetDriver request received", zap.String("driver_id", req.GetDriverId()))

	// TODO: Implement database lookup in Phase 2
	return &pb.DriverResponse{
		Id:            req.GetDriverId(),
		FirstName:     "Goku",
		LastName:      "Son",
		PhoneNo:       "11111111",
		VehicleModel:  "Honda CR-V",
		VehicleNumber: "MH 01A B 4321",
		VehicleType:   "SUV",
		IsOnline:      true,
		Rating:        4.9,
	}, nil
}

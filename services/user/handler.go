package main

import (
	"context"
	"go.uber.org/zap"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// UserHandler implements the gRPC UserServiceServer interface
type UserHandler struct {
	pb.UnimplementedUserServiceServer
	logger *zap.Logger
}

// NewUserHandler creates a new UserHandler instance
func NewUserHandler(logger *zap.Logger) *UserHandler {
	return &UserHandler{
		logger: logger,
	}
}

// Register handles user registration gRPC requests
func (h *UserHandler) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.AuthResponse, error) {
	h.logger.Info("Register user request received", zap.String("email", req.GetEmail()))
	
	// TODO: Implement user registration business logic in Phase 2
	return &pb.AuthResponse{
		AccessToken: "dummy-jwt-access-token",
		ExpiresAt:   "2026-06-23T23:59:59Z",
		User: &pb.UserResponse{
			Id:        "dummy-user-uuid",
			Email:     req.GetEmail(),
			FirstName: req.GetFirstName(),
			LastName:  req.GetLastName(),
			CreatedAt: "2026-06-23T01:20:00Z",
		},
	}, nil
}

// Login handles user login gRPC requests
func (h *UserHandler) Login(ctx context.Context, req *pb.LoginRequest) (*pb.AuthResponse, error) {
	h.logger.Info("Login user request received", zap.String("email", req.GetEmail()))
	
	// TODO: Implement login verification in Phase 2
	return &pb.AuthResponse{
		AccessToken: "dummy-jwt-access-token",
		ExpiresAt:   "2026-06-23T23:59:59Z",
		User: &pb.UserResponse{
			Id:        "dummy-user-uuid",
			Email:     req.GetEmail(),
			FirstName: "Test",
			LastName:  "User",
			CreatedAt: "2026-06-23T01:20:00Z",
		},
	}, nil
}

// GetUser retrieves user profile details
func (h *UserHandler) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.UserResponse, error) {
	h.logger.Info("GetUser request received", zap.String("user_id", req.GetUserId()))
	
	// TODO: Implement user retrieval logic in Phase 2
	return &pb.UserResponse{
		Id:        req.GetUserId(),
		Email:     "testuser@example.com",
		FirstName: "Test",
		LastName:  "User",
		CreatedAt: "2026-06-23T01:20:00Z",
	}, nil
}

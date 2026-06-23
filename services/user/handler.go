package main

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ThalaPravin/RideMesh/pkg/auth"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

const jwtSecret = "ridemesh-jwt-secret-key-12345"

// UserHandler implements the gRPC UserServiceServer interface
type UserHandler struct {
	pb.UnimplementedUserServiceServer
	repo   *UserRepository
	logger *zap.Logger
}

// NewUserHandler creates a new UserHandler instance
func NewUserHandler(repo *UserRepository, logger *zap.Logger) *UserHandler {
	return &UserHandler{
		repo:   repo,
		logger: logger,
	}
}

// Register handles user registration gRPC requests
func (h *UserHandler) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.AuthResponse, error) {
	h.logger.Info("Register user request received", zap.String("email", req.GetEmail()))

	if req.GetEmail() == "" || req.GetPassword() == "" || req.GetFirstName() == "" {
		return nil, status.Error(codes.InvalidArgument, "email, password, and first name are required")
	}

	hashedPassword, err := auth.HashPassword(req.GetPassword())
	if err != nil {
		h.logger.Error("Failed to hash password", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to process request")
	}

	u, err := h.repo.CreateUser(ctx, req.GetEmail(), hashedPassword, req.GetFirstName(), req.GetLastName())
	if err != nil {
		if errors.Is(err, ErrUserAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, "user with this email already exists")
		}
		h.logger.Error("Failed to create user in database", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to create user")
	}

	token, expiresAt, err := auth.GenerateToken(u.ID, jwtSecret)
	if err != nil {
		h.logger.Error("Failed to generate JWT token", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to sign auth token")
	}

	return &pb.AuthResponse{
		AccessToken: token,
		ExpiresAt:   expiresAt.Format(time.RFC3339),
		User: &pb.UserResponse{
			Id:        u.ID,
			Email:     u.Email,
			FirstName: u.FirstName,
			LastName:  u.LastName,
			CreatedAt: u.CreatedAt.Format(time.RFC3339),
		},
	}, nil
}

// Login handles user login gRPC requests
func (h *UserHandler) Login(ctx context.Context, req *pb.LoginRequest) (*pb.AuthResponse, error) {
	h.logger.Info("Login user request received", zap.String("email", req.GetEmail()))

	if req.GetEmail() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "email and password are required")
	}

	u, err := h.repo.GetUserByEmail(ctx, req.GetEmail())
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		h.logger.Error("Failed to query user by email", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to query credentials")
	}

	if !auth.CheckPasswordHash(req.GetPassword(), u.Password) {
		return nil, status.Error(codes.Unauthenticated, "invalid password")
	}

	token, expiresAt, err := auth.GenerateToken(u.ID, jwtSecret)
	if err != nil {
		h.logger.Error("Failed to generate JWT token", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to sign auth token")
	}

	return &pb.AuthResponse{
		AccessToken: token,
		ExpiresAt:   expiresAt.Format(time.RFC3339),
		User: &pb.UserResponse{
			Id:        u.ID,
			Email:     u.Email,
			FirstName: u.FirstName,
			LastName:  u.LastName,
			CreatedAt: u.CreatedAt.Format(time.RFC3339),
		},
	}, nil
}

// GetUser retrieves user profile details
func (h *UserHandler) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.UserResponse, error) {
	h.logger.Info("GetUser request received", zap.String("user_id", req.GetUserId()))

	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user ID is required")
	}

	u, err := h.repo.GetUserByID(ctx, req.GetUserId())
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		h.logger.Error("Failed to query user by ID", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to fetch profile")
	}

	return &pb.UserResponse{
		Id:        u.ID,
		Email:     u.Email,
		FirstName: u.FirstName,
		LastName:  u.LastName,
		CreatedAt: u.CreatedAt.Format(time.RFC3339),
	}, nil
}

package main

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/ThalaPravin/RideMesh/proto"
)

// PaymentHandler implements the gRPC PaymentServiceServer interface
type PaymentHandler struct {
	pb.UnimplementedPaymentServiceServer
	repo   *PaymentRepository
	logger *zap.Logger
}

// NewPaymentHandler creates a new PaymentHandler instance
func NewPaymentHandler(repo *PaymentRepository, logger *zap.Logger) *PaymentHandler {
	return &PaymentHandler{
		repo:   repo,
		logger: logger,
	}
}

// ProcessPayment handles billing transactions
func (h *PaymentHandler) ProcessPayment(ctx context.Context, req *pb.ProcessPaymentRequest) (*pb.ProcessPaymentResponse, error) {
	h.logger.Info("ProcessPayment request received", zap.String("trip_id", req.GetTripId()), zap.Float64("amount", req.GetAmount()))

	if req.GetTripId() == "" || req.GetUserId() == "" || req.GetAmount() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "trip_id, user_id, and valid amount are required")
	}

	p, err := h.repo.CreatePayment(ctx, req.GetTripId(), req.GetUserId(), req.GetAmount(), "SUCCESS")
	if err != nil {
		h.logger.Error("Failed to record payment transaction in database", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to process payment")
	}

	return &pb.ProcessPaymentResponse{
		TransactionId: p.TransactionID,
		Status:        p.Status,
		ProcessedAt:   p.CreatedAt.Format(time.RFC3339),
	}, nil
}

// GetPaymentStatus retrieves transaction details
func (h *PaymentHandler) GetPaymentStatus(ctx context.Context, req *pb.GetPaymentStatusRequest) (*pb.GetPaymentStatusResponse, error) {
	h.logger.Info("GetPaymentStatus request received", zap.String("transaction_id", req.GetTransactionId()))

	if req.GetTransactionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction ID is required")
	}

	p, err := h.repo.GetPaymentByTransactionID(ctx, req.GetTransactionId())
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) {
			return nil, status.Error(codes.NotFound, "transaction not found")
		}
		h.logger.Error("Failed to query payment by transaction ID", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to retrieve receipt")
	}

	return &pb.GetPaymentStatusResponse{
		TransactionId: p.TransactionID,
		TripId:        p.TripID,
		Amount:        p.Amount,
		Status:        p.Status,
		ProcessedAt:   p.CreatedAt.Format(time.RFC3339),
	}, nil
}

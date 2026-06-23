package main

import (
	"context"
	"go.uber.org/zap"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// PaymentHandler implements the gRPC PaymentServiceServer interface
type PaymentHandler struct {
	pb.UnimplementedPaymentServiceServer
	logger *zap.Logger
}

// NewPaymentHandler creates a new PaymentHandler instance
func NewPaymentHandler(logger *zap.Logger) *PaymentHandler {
	return &PaymentHandler{
		logger: logger,
	}
}

// ProcessPayment handles billing transactions
func (h *PaymentHandler) ProcessPayment(ctx context.Context, req *pb.ProcessPaymentRequest) (*pb.ProcessPaymentResponse, error) {
	h.logger.Info("ProcessPayment request received", zap.String("trip_id", req.GetTripId()), zap.Float64("amount", req.GetAmount()))

	// TODO: Integrate mock payment gateway in Phase 5
	return &pb.ProcessPaymentResponse{
		TransactionId: "dummy-txn-uuid",
		Status:        "SUCCESS",
		ProcessedAt:   "2026-06-23T01:21:00Z",
	}, nil
}

// GetPaymentStatus retrieves transaction details
func (h *PaymentHandler) GetPaymentStatus(ctx context.Context, req *pb.GetPaymentStatusRequest) (*pb.GetPaymentStatusResponse, error) {
	h.logger.Info("GetPaymentStatus request received", zap.String("transaction_id", req.GetTransactionId()))

	// TODO: Query transaction details from PostgreSQL in Phase 5
	return &pb.GetPaymentStatusResponse{
		TransactionId: req.GetTransactionId(),
		TripId:        "dummy-trip-uuid",
		Amount:        350.0,
		Status:        "SUCCESS",
		ProcessedAt:   "2026-06-23T01:21:00Z",
	}, nil
}

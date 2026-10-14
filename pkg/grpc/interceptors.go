package grpcutil

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ThalaPravin/RideMesh/pkg/metrics"
)

// UnaryServerLoggingInterceptor logs incoming gRPC requests and latency
func UnaryServerLoggingInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		statusCode := codes.OK
		if err != nil {
			statusCode = status.Code(err)
		}

		log.Info("gRPC call processed",
			zap.String("method", info.FullMethod),
			zap.String("code", statusCode.String()),
			zap.Duration("duration", duration),
		)

		return resp, err
	}
}

// UnaryServerRecoveryInterceptor catches unhandled panics and prevents server crash
func UnaryServerRecoveryInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("Recovered from unhandled panic in gRPC handler",
					zap.String("method", info.FullMethod),
					zap.Any("panic", r),
				)
				err = status.Errorf(codes.Internal, "Internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

// UnaryServerMetricsInterceptor records call metrics into Prometheus
func UnaryServerMetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start).Seconds()

		if info.FullMethod == "/proto.MatchingService/GetNearbyDrivers" {
			metrics.MatchingDuration.Observe(duration)
		}

		return resp, err
	}
}

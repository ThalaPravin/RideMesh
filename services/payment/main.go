package main

import (
	"context"
	"net"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ThalaPravin/RideMesh/pkg/config"
	"github.com/ThalaPravin/RideMesh/pkg/logger"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// StartGRPCServer starts the Payment Service gRPC server
func StartGRPCServer(lc fx.Lifecycle, log *zap.Logger, cfg *config.Config, handler *PaymentHandler) {
	grpcServer := grpc.NewServer()
	pb.RegisterPaymentServiceServer(grpcServer, handler)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			lis, err := net.Listen("tcp", ":"+cfg.Port)
			if err != nil {
				log.Error("Failed to listen on port", zap.String("port", cfg.Port), zap.Error(err))
				return err
			}

			log.Info("Starting Payment Service gRPC server", zap.String("port", cfg.Port))
			go func() {
				if err := grpcServer.Serve(lis); err != nil {
					log.Error("gRPC server failed to serve", zap.Error(err))
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("Stopping Payment Service gRPC server gracefully...")
			grpcServer.GracefulStop()
			return nil
		},
	})
}

func main() {
	app := fx.New(
		fx.Provide(
			config.LoadConfig,
			logger.NewLogger,
			NewPaymentHandler,
		),
		fx.Invoke(StartGRPCServer),
	)

	app.Run()
}

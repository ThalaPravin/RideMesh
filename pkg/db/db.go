package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/ThalaPravin/RideMesh/pkg/config"
)

// NewDBPool initializes a pgx connection pool and integrates it with Fx lifecycle
func NewDBPool(lc fx.Lifecycle, cfg *config.Config, log *zap.Logger) (*pgxpool.Pool, error) {
	log.Info("Initializing PostgreSQL connection pool...")
	
	// Configure pool parameters if needed
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Error("Failed to parse database connection URL", zap.Error(err))
		return nil, err
	}
	
	// Set reasonable defaults for production-ready pooling
	poolConfig.MaxConns = 20
	poolConfig.MinConns = 5
	poolConfig.MaxConnIdleTime = 30 * time.Minute

	var pool *pgxpool.Pool
	
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			var err error
			// Connect with context
			pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
			if err != nil {
				log.Error("Failed to create connection pool", zap.Error(err))
				return err
			}

			// Ping database to ensure availability
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := pool.Ping(pingCtx); err != nil {
				log.Error("Failed to ping PostgreSQL database", zap.Error(err))
				return err
			}

			log.Info("Connected to PostgreSQL successfully.")

			// Run auto migrations
			if err := InitSchema(ctx, pool, log); err != nil {
				log.Error("Failed to execute database migrations", zap.Error(err))
				return err
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("Closing PostgreSQL connection pool...")
			if pool != nil {
				pool.Close()
			}
			return nil
		},
	})

	return pool, nil
}

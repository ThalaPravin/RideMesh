package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/ThalaPravin/RideMesh/pkg/config"
)

const DriverLocationsKey = "drivers:locations"

// Client wraps the go-redis Client with custom geospatial helpers
type Client struct {
	rdb    *redis.Client
	logger *zap.Logger
}

// NewRedisClient creates a new Redis connection pool with Uber Fx lifecycle management
func NewRedisClient(lc fx.Lifecycle, cfg *config.Config, log *zap.Logger) (*Client, error) {
	log.Info("Initializing Redis client connection...", zap.String("url", cfg.RedisURL))

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisURL,
		Password: "", // No password set by default
		DB:       0,  // Use default DB
	})

	c := &Client{
		rdb:    rdb,
		logger: log,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			if err := rdb.Ping(pingCtx).Err(); err != nil {
				log.Error("Failed to ping Redis server", zap.Error(err))
				return err
			}

			log.Info("Connected to Redis successfully.")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("Closing Redis client connection...")
			return rdb.Close()
		},
	})

	return c, nil
}

// AddDriverLocation indexes a driver's latitude and longitude using Redis GEOADD
func (c *Client) AddDriverLocation(ctx context.Context, driverID string, lat, lon float64) error {
	location := &redis.GeoLocation{
		Name:      driverID,
		Longitude: lon,
		Latitude:  lat,
	}

	err := c.rdb.GeoAdd(ctx, DriverLocationsKey, location).Err()
	if err != nil {
		c.logger.Error("Failed to GEOADD driver location to Redis", zap.String("driver_id", driverID), zap.Error(err))
		return err
	}

	c.logger.Info("Updated driver location in Redis GEO index",
		zap.String("driver_id", driverID),
		zap.Float64("lat", lat),
		zap.Float64("lon", lon),
	)
	return nil
}

// FindNearbyDrivers searches for drivers within a given radius in kilometers using Redis GEORADIUS / GEOSEARCH
func (c *Client) FindNearbyDrivers(ctx context.Context, lat, lon, radiusKm float64) ([]string, error) {
	query := &redis.GeoRadiusQuery{
		Radius:      radiusKm,
		Unit:        "km",
		WithCoord:   true,
		WithDist:    true,
		Count:       10,
		Order:       "ASC", // Nearest drivers first
	}

	locations, err := c.rdb.GeoRadius(ctx, DriverLocationsKey, lon, lat, query).Result()
	if err != nil {
		c.logger.Error("Failed to query nearby drivers from Redis GEO index", zap.Error(err))
		return nil, err
	}

	var driverIDs []string
	for _, loc := range locations {
		driverIDs = append(driverIDs, loc.Name)
	}

	c.logger.Info("Queried nearby drivers from Redis GEO index",
		zap.Float64("lat", lat),
		zap.Float64("lon", lon),
		zap.Int("found_count", len(driverIDs)),
	)
	return driverIDs, nil
}

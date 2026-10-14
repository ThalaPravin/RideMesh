package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// Limiter implements a distributed sliding-window rate limiter powered by Redis
type Limiter struct {
	client *redis.Client
	logger *zap.Logger
}

// NewLimiter initializes a rate limiter
func NewLimiter(client *redis.Client, logger *zap.Logger) *Limiter {
	return &Limiter{
		client: client,
		logger: logger,
	}
}

// Allow checks if the given key (e.g. user_id or IP) is permitted within the rate limit
// limit: max allowed requests
// window: rolling time window (e.g. 1 minute)
func (l *Limiter) Allow(ctx context.Context, key string, limit int64, window time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	windowStart := now - window.Nanoseconds()
	redisKey := fmt.Sprintf("ratelimit:%s", key)

	// Pipeline Redis commands for atomic sliding window check
	pipe := l.client.TxPipeline()

	// 1. Remove elements older than the current window
	pipe.ZRemRangeByScore(ctx, redisKey, "0", fmt.Sprintf("%d", windowStart))

	// 2. Add current request timestamp
	pipe.ZAdd(ctx, redisKey, &redis.Z{
		Score:  float64(now),
		Member: fmt.Sprintf("%d", now),
	})

	// 3. Count remaining elements in window
	countCmd := pipe.ZCard(ctx, redisKey)

	// 4. Set TTL on the key so inactive keys expire automatically
	pipe.Expire(ctx, redisKey, window*2)

	_, err := pipe.Exec(ctx)
	if err != nil {
		l.logger.Error("Rate limiter Redis pipeline execution failed", zap.Error(err))
		// Fail open to avoid blocking traffic on Redis glitch
		return true, nil
	}

	currentRequests := countCmd.Val()
	allowed := currentRequests <= limit

	if !allowed {
		l.logger.Warn("Rate limit exceeded",
			zap.String("key", key),
			zap.Int64("current_requests", currentRequests),
			zap.Int64("limit", limit),
		)
	}

	return allowed, nil
}

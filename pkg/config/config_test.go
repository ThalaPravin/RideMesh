package config

import (
	"os"
	"testing"
)

func TestLoadConfig_Defaults(t *testing.T) {
	// Clean environment variables
	os.Unsetenv("APP_ENV")
	os.Unsetenv("PORT")
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("REDIS_URL")
	os.Unsetenv("KAFKA_BROKERS")

	cfg := LoadConfig()

	if cfg.AppEnv != "development" {
		t.Errorf("Expected default AppEnv 'development', got '%s'", cfg.AppEnv)
	}

	if cfg.Port != "50051" {
		t.Errorf("Expected default Port '50051', got '%s'", cfg.Port)
	}

	if cfg.RedisURL != "redis:6379" {
		t.Errorf("Expected default RedisURL 'redis:6379', got '%s'", cfg.RedisURL)
	}

	if cfg.KafkaBrokers != "kafka:9092" {
		t.Errorf("Expected default KafkaBrokers 'kafka:9092', got '%s'", cfg.KafkaBrokers)
	}
}

func TestLoadConfig_CustomEnv(t *testing.T) {
	os.Setenv("APP_ENV", "production")
	os.Setenv("PORT", "8080")
	os.Setenv("DATABASE_URL", "postgres://user:pass@dbhost:5432/rides")
	os.Setenv("REDIS_URL", "redishost:6380")
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	defer func() {
		os.Unsetenv("APP_ENV")
		os.Unsetenv("PORT")
		os.Unsetenv("DATABASE_URL")
		os.Unsetenv("REDIS_URL")
		os.Unsetenv("KAFKA_BROKERS")
	}()

	cfg := LoadConfig()

	if cfg.AppEnv != "production" {
		t.Errorf("Expected AppEnv 'production', got '%s'", cfg.AppEnv)
	}

	if cfg.Port != "8080" {
		t.Errorf("Expected Port '8080', got '%s'", cfg.Port)
	}

	if cfg.DatabaseURL != "postgres://user:pass@dbhost:5432/rides" {
		t.Errorf("Expected custom DatabaseURL, got '%s'", cfg.DatabaseURL)
	}

	if cfg.RedisURL != "redishost:6380" {
		t.Errorf("Expected custom RedisURL, got '%s'", cfg.RedisURL)
	}

	if cfg.KafkaBrokers != "broker1:9092,broker2:9092" {
		t.Errorf("Expected custom KafkaBrokers, got '%s'", cfg.KafkaBrokers)
	}
}

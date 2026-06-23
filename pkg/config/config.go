package config

import (
	"os"
)

// Config holds all config variables for the microservices
type Config struct {
	AppEnv       string
	Port         string
	DatabaseURL  string
	RedisURL     string
	KafkaBrokers string
}

// LoadConfig creates a new Config using environment variables and default values
func LoadConfig() *Config {
	return &Config{
		AppEnv:       getEnv("APP_ENV", "development"),
		Port:         getEnv("PORT", "50051"),
		DatabaseURL:  getEnv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/ridemesh?sslmode=disable"),
		RedisURL:     getEnv("REDIS_URL", "redis:6379"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "kafka:9092"),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

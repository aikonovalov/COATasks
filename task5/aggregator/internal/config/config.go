package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	ClickHouseURL     string
	PostgresDSN       string
	HTTPPort          string
	AggregateInterval time.Duration
	S3Enabled         bool
	S3Endpoint        string
	S3Region          string
	S3AccessKey       string
	S3SecretKey       string
	S3Bucket          string
}

func Load() *Config {
	return &Config{
		ClickHouseURL:     envOr("CLICKHOUSE_URL", "http://clickhouse:8123"),
		PostgresDSN:       envOr("POSTGRES_DSN", "postgres://cinema:cinema@postgres:5432/cinema?sslmode=disable"),
		HTTPPort:          envOr("HTTP_PORT", "8090"),
		AggregateInterval: time.Duration(envInt("AGGREGATE_INTERVAL_SECONDS", 3600)) * time.Second,
		S3Enabled:         envOr("S3_EXPORT_ENABLED", "true") == "true",
		S3Endpoint:        envOr("S3_ENDPOINT", "http://minio:9000"),
		S3Region:          envOr("AWS_REGION", "us-east-1"),
		S3AccessKey:       envOr("S3_ACCESS_KEY", "minio"),
		S3SecretKey:       envOr("S3_SECRET_KEY", "minio123"),
		S3Bucket:          envOr("S3_BUCKET", "movie-analytics"),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

func envInt(key string, def int) int {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	
	return def
}

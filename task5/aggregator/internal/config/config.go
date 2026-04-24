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
}

func Load() *Config {
	return &Config{
		ClickHouseURL:     envOr("CLICKHOUSE_URL", "http://clickhouse:8123"),
		PostgresDSN:       envOr("POSTGRES_DSN", "postgres://cinema:cinema@postgres:5432/cinema?sslmode=disable"),
		HTTPPort:          envOr("HTTP_PORT", "8090"),
		AggregateInterval: time.Duration(envInt("AGGREGATE_INTERVAL_SECONDS", 3600)) * time.Second,
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

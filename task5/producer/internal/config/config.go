package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers            []string
	KafkaTopic              string
	SchemaRegistryURL       string
	SchemaRegistrySubject   string
	HTTPPort                string
	GeneratorMode           bool
	GeneratorIntervalMS     int
	KafkaRetries            int
	KafkaRetryBackoffInitMS int
	KafkaRetryBackoffMaxMS  int
}

func Load() *Config {
	return &Config{
		KafkaBrokers:            getEnvSlice("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092"),
		KafkaTopic:              getEnv("KAFKA_TOPIC", "movie-events"),
		SchemaRegistryURL:       getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
		SchemaRegistrySubject:   getEnv("SCHEMA_REGISTRY_SUBJECT", "movie-events-value"),
		HTTPPort:                getEnv("HTTP_PORT", "8080"),
		GeneratorMode:           getEnvBool("GENERATOR_MODE", false),
		GeneratorIntervalMS:     getEnvInt("GENERATOR_INTERVAL_MS", 500),
		KafkaRetries:            getEnvInt("KAFKA_RETRIES", 5),
		KafkaRetryBackoffInitMS: getEnvInt("KAFKA_RETRY_BACKOFF_INIT_MS", 100),
		KafkaRetryBackoffMaxMS:  getEnvInt("KAFKA_RETRY_BACKOFF_MAX_MS", 10000),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

func getEnvSlice(key, def string) []string {
	return strings.Split(getEnv(key, def), ",")
}

func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}

	return b
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}

	return i
}

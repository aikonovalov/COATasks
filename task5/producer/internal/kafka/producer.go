package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	eventpb "movie/producer/gen"
	"movie/producer/internal/config"
)

type Producer struct {
	cfg    *config.Config
	writer *kafkago.Writer
}

func NewProducer(cfg *config.Config) (*Producer, error) {
	schemaID, err := fetchSchemaID(cfg.SchemaRegistryURL, cfg.SchemaRegistrySubject)
	if err != nil {
		return nil, fmt.Errorf("schema registry check failed: %w", err)
	}

	slog.Info("schema registry ok",
		"subject", cfg.SchemaRegistrySubject,
		"schema_id", schemaID,
	)

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaTopic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		Async:        false,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}

	return &Producer{cfg: cfg, writer: writer}, nil
}

func (p *Producer) Publish(ctx context.Context, event *eventpb.Event) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := p.publishWithRetry(ctx, event.UserId, payload); err != nil {
		return err
	}

	slog.Info("event published",
		"event_id", fmt.Sprintf("%x", event.EventId),
		"event_type", event.EventType.String(),
		"timestamp", event.Timestamp.AsTime().UTC().Format(time.RFC3339),
		"user_id", event.UserId,
		"topic", p.cfg.KafkaTopic,
	)

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) publishWithRetry(ctx context.Context, key string, value []byte) error {
	delay := time.Duration(p.cfg.KafkaRetryBackoffInitMS) * time.Millisecond
	maxDelay := time.Duration(p.cfg.KafkaRetryBackoffMaxMS) * time.Millisecond

	for attempt := 0; attempt <= p.cfg.KafkaRetries; attempt++ {
		err := p.writer.WriteMessages(ctx, kafkago.Message{
			Key:   []byte(key),
			Value: value,
		})
		if err == nil {
			return nil
		}

		if attempt == p.cfg.KafkaRetries {
			return fmt.Errorf("kafka publish failed after %d attempts: %w", attempt+1, err)
		}

		jitter := time.Duration(rand.Int63n(int64(delay/4) + 1))
		sleep := delay + jitter
		if sleep > maxDelay {
			sleep = maxDelay
		}

		slog.Warn("kafka publish error, retrying",
			"attempt", attempt+1,
			"max", p.cfg.KafkaRetries,
			"delay_ms", sleep.Milliseconds(),
			"error", err,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
	return nil
}

func fetchSchemaID(registryURL, subject string) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", registryURL, subject)

	resp, err := http.Get(url)
	if err != nil {
		return 0, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("schema registry %s returned %d: %s", url, resp.StatusCode, body)
	}

	var result struct {
		ID int `json:"id"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("decode schema registry response: %w", err)
	}

	return result.ID, nil
}

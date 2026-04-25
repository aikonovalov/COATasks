package s3export

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"movie/aggregator/internal/store"
)

type Exporter struct {
	client *s3.Client
	bucket string
	pg     *store.Postgres
}

func New(endpoint, region, accessKey, secret, bucket string, pg *store.Postgres) (*Exporter, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secret, "")),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &Exporter{client: client, bucket: bucket, pg: pg}, nil
}

func (e *Exporter) ExportDate(ctx context.Context, d time.Time) error {
	rows, err := e.pg.ListMetricsByDate(ctx, d)
	if err != nil {
		return fmt.Errorf("read pg: %w", err)
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	if err := w.Write([]string{"date", "metric_name", "value", "extra_data", "computed_at"}); err != nil {
		return err
	}

	for _, m := range rows {
		extra := ""
		if m.Extra != nil {
			extra = string(m.Extra)
		}

		if err := w.Write([]string{
			m.Date.Format("2006-01-02"),
			m.MetricName,
			fmt.Sprintf("%g", m.Value),
			extra,
			m.ComputedAt.UTC().Format(time.RFC3339),
		}); err != nil {
			return err
		}
	}

	w.Flush()
	
	if err := w.Error(); err != nil {
		return err
	}

	key := fmt.Sprintf("daily/%s/aggregates.csv", d.Format("2006-01-02"))

	_, err = e.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(e.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("text/csv; charset=utf-8"),
	})
	if err != nil {
		return err
	}

	slog.Info("s3 export uploaded", "bucket", e.bucket, "key", key, "rows", len(rows))
	return nil
}

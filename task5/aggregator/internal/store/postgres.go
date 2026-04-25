package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() { p.pool.Close() }

func (p *Postgres) UpsertMetric(ctx context.Context, date time.Time, name string, value float64, extra []byte) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO daily_metrics (date, metric_name, value, extra_data, computed_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (date, metric_name) DO UPDATE SET
			value       = EXCLUDED.value,
			extra_data  = EXCLUDED.extra_data,
			computed_at = EXCLUDED.computed_at
	`, date, name, value, extra)

	return err
}

type DailyMetric struct {
	Date       time.Time
	MetricName string
	Value      float64
	Extra      []byte
	ComputedAt time.Time
}

func (p *Postgres) ListMetricsByDate(ctx context.Context, date time.Time) ([]DailyMetric, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT date, metric_name, value, extra_data, computed_at
		FROM daily_metrics
		WHERE date = $1::date
		ORDER BY metric_name
	`, date)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var out []DailyMetric

	for rows.Next() {
		var m DailyMetric
		if err := rows.Scan(&m.Date, &m.MetricName, &m.Value, &m.Extra, &m.ComputedAt); err != nil {
			return nil, err
		}

		out = append(out, m)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

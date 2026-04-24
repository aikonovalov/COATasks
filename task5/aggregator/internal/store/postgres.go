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

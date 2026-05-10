CREATE TABLE IF NOT EXISTS daily_metrics (
    date         DATE          NOT NULL,
    metric_name  VARCHAR(100)  NOT NULL,
    value        DOUBLE PRECISION NOT NULL DEFAULT 0,
    extra_data   JSONB,
    computed_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics (date);

COMMENT ON TABLE daily_metrics IS
    'Aggregated analytics metrics: one row per (date, metric_name). '
    'metric_name values: dau, avg_view_seconds, conversion, retention_d1, retention_d7, top_movies.';

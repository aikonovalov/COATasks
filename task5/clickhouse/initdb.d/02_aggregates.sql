CREATE TABLE IF NOT EXISTS agg_dau
(
    date     Date,
    dau      UInt64
)

ENGINE = ReplacingMergeTree

ORDER BY date;

CREATE TABLE IF NOT EXISTS agg_avg_view_seconds
(
    date        Date,
    avg_seconds Float64
)

ENGINE = ReplacingMergeTree

ORDER BY date;

CREATE TABLE IF NOT EXISTS agg_conversion
(
    date       Date,
    conversion Float64
)

ENGINE = ReplacingMergeTree

ORDER BY date;

CREATE TABLE IF NOT EXISTS agg_retention
(
    date         Date,
    retention_d1 Float64,
    retention_d7 Float64
)

ENGINE = ReplacingMergeTree

ORDER BY date;

CREATE TABLE IF NOT EXISTS agg_top_movies
(
    date     Date,
    rank     UInt16,
    movie_id String,
    views    UInt64
)

ENGINE = ReplacingMergeTree

ORDER BY (date, rank);

CREATE TABLE IF NOT EXISTS movie_events_queue
(
    event_id              String,
    user_id               String,
    movie_id              String,
    event_type            LowCardinality(String),
    `timestamp.seconds`   Int64,
    `timestamp.nanos`     Int32,
    device_type           LowCardinality(String),
    session_id            String,
    progress_seconds      Int32
)

ENGINE = Kafka

SETTINGS
    kafka_broker_list        = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list         = 'movie-events',
    kafka_group_name         = 'clickhouse-consumer',
    kafka_format             = 'ProtobufSingle',
    kafka_schema             = 'event:movie.events.Event',
    kafka_num_consumers      = 1,
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS movie_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    timestamp        DateTime64(9, 'UTC'),
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Int32,
    ingested_at      DateTime DEFAULT now()
)

ENGINE = MergeTree

PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_type, user_id, timestamp)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_events_mv
TO movie_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    fromUnixTimestamp64Nano(
        toInt64(`timestamp.seconds`) * 1000000000 + toInt64(`timestamp.nanos`)
    ) AS timestamp,
    device_type,
    session_id,
    progress_seconds
FROM movie_events_queue;

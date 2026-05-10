CREATE KEYSPACE IF NOT EXISTS warehouse WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3} AND durable_writes = true;

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product_zone (
  product_id text,
  zone_id text,
  available_quantity bigint,
  reserved_quantity bigint,
  last_event_ts timestamp,
  supplier_id text,
  PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product (
  product_id text,
  zone_id text,
  available_quantity bigint,
  reserved_quantity bigint,
  last_event_ts timestamp,
  supplier_id text,
  PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.inventory_by_zone (
  zone_id text,
  product_id text,
  available_quantity bigint,
  reserved_quantity bigint,
  last_event_ts timestamp,
  supplier_id text,
  PRIMARY KEY ((zone_id), product_id)
);

CREATE TABLE IF NOT EXISTS warehouse.processed_events (
  event_id text PRIMARY KEY,
  processed_at timestamp
);

CREATE TABLE IF NOT EXISTS warehouse.orders (
  order_id text PRIMARY KEY,
  status text,
  last_event_ts timestamp
);

CREATE TABLE IF NOT EXISTS warehouse.order_items (
  order_id text,
  product_id text,
  zone_id text,
  quantity int,
  PRIMARY KEY ((order_id), product_id, zone_id)
);

CREATE TABLE IF NOT EXISTS warehouse.event_audit (
  event_id text,
  occurred_at timeuuid,
  event_type text,
  kafka_partition int,
  kafka_offset bigint,
  PRIMARY KEY ((event_id), occurred_at)
) WITH CLUSTERING ORDER BY (occurred_at DESC);

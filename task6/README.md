# Склад

| URL | Назначение |
|-----|------------|
| http://localhost:8081 | Schema Registry |
| localhost:29092 | Kafka (с хоста) |
| http://localhost:9080/metrics | метрики consumer |
| http://localhost:9080/health | liveness/readiness |
| http://localhost:9090 | Prometheus |
| http://localhost:3000 | Grafana (`admin` / `admin`) |
| localhost:9042 | CQL к `cassandra-1` |

## Модель Cassandra (под запросы)

Partition key и clustering key выбраны так, чтобы типовые запросы из ТЗ не требовали JOIN и задевали мало партиций.

- `inventory_by_product_zone`: `PRIMARY KEY ((product_id), zone_id)` — партиция по `product_id`, внутри строки по зонам; запрос «товар + зона» и «все зоны товара».
- `inventory_by_product`: та же форма `(product_id), zone_id` — денормированная проекция, обновляется тем же событием.
- `inventory_by_zone`: `PRIMARY KEY ((zone_id), product_id)` — партиция по зоне, обход SKU в ячейке.
- `processed_events`: `(event_id)` — идемпотентность при at-least-once.
- `orders` / `order_items` — статус заказа и строки; `event_audit` — след по Kafka.

в коде один `LoggedBatch` на событие для нескольких таблиц и разных партиций. В Cassandra атомарность logged batch формально гарантируется в пределах одной партиции

## Consistency

Записи настроены на **QUORUM**

Чтения для проверок `processed_events` и текущих остатков перед апдейтом — **ONE**: меньше задержка, нагрузка

## Эволюция схем

**BACKWARD** (новые консьюмеры читают старые сообщения). Новое поле в Avro — union с `null` и `default: null`, чтобы старые записи оставались валидными.

Новая версия

1. Avro-файл
2. Регистрация схемы под тем же subject значения топика
3. Обновление producer
4. Обновление consumer
5. `docker compose up`

## Команды

```bash
docker compose run --rm --entrypoint /bin/sh schema-init -c '/app/producer scenario'
docker compose run --rm --entrypoint /bin/sh schema-init -c '/app/producer bad'
```

```bash
docker compose exec cassandra-1 cqlsh -e "SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"
```

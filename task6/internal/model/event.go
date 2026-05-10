package model

import "time"

type OrderLine struct {
	ProductID string `avro:"product_id"`
	ZoneID    string `avro:"zone_id"`
	Quantity  int    `avro:"quantity"`
}

type WarehouseEvent struct {
	EventID     string       `avro:"event_id"`
	EventType   string       `avro:"event_type"`
	OccurredAt  time.Time    `avro:"occurred_at"`
	ProductID   *string      `avro:"product_id"`
	ZoneID      *string      `avro:"zone_id"`
	FromZoneID  *string      `avro:"from_zone_id"`
	ToZoneID    *string      `avro:"to_zone_id"`
	Quantity    *int         `avro:"quantity"`
	CountedQty  *int         `avro:"counted_quantity"`
	OrderID     *string      `avro:"order_id"`
	OrderLines  *[]OrderLine `avro:"order_lines"`
	SupplierID  *string      `avro:"supplier_id"`
}

const (
	ProductReceived   = "PRODUCT_RECEIVED"
	ProductShipped    = "PRODUCT_SHIPPED"
	ProductMoved      = "PRODUCT_MOVED"
	ProductReserved   = "PRODUCT_RESERVED"
	ProductReleased   = "PRODUCT_RELEASED"
	InventoryCounted  = "INVENTORY_COUNTED"
	OrderCreated      = "ORDER_CREATED"
	OrderCompleted    = "ORDER_COMPLETED"
)

const (
	OrderStatusCreated   = "CREATED"
	OrderStatusCompleted = "COMPLETED"
)

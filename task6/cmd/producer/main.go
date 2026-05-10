package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
	"warehouse/internal/encoding"
	"warehouse/internal/model"
)

func env(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func mustRegister(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  env("KAFKA_TOPIC", "warehouse-events"),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
}

func ptrString(s string) *string { return &s }
func ptrInt(i int) *int          { return &i }

func send(ctx context.Context, enc *encoding.Resolver, w *kafka.Writer, sr *srclient.SchemaRegistryClient, ev *model.WarehouseEvent, schemaVer int) error {
	var sch *srclient.Schema
	var err error
	if schemaVer == 1 {
		sch, err = sr.GetSchemaByVersion("warehouse-events-value", 1)
	} else {
		sch, err = sr.GetLatestSchema("warehouse-events-value")
	}
	if err != nil {
		return err
	}
	if schemaVer == 1 {
		ev.SupplierID = nil
	}
	b, err := enc.EncodeWarehousing(sch.Schema(), sch.ID(), ev)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{Key: []byte(ev.EventID), Value: b})
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("expected subcommand register|send|scenario|bad")
	}
	sub := os.Args[1]
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:29092"), ",")
	srURL := env("SCHEMA_REGISTRY", "http://localhost:8081")
	sr := srclient.CreateSchemaRegistryClient(srURL)
	enc := encoding.NewResolver(srURL)

	switch sub {
	case "register":
		b1, err := os.ReadFile("schemas/WarehouseEvent_v1.avsc")
		if err != nil {
			log.Fatal(err)
		}
		if _, err := sr.CreateSchema("warehouse-events-value", string(b1), srclient.Avro); err != nil {
			log.Println("v1", err)
		}
		b2, err := os.ReadFile("schemas/WarehouseEvent_v2.avsc")
		if err != nil {
			log.Fatal(err)
		}
		if _, err := sr.CreateSchema("warehouse-events-value", string(b2), srclient.Avro); err != nil {
			log.Fatal(err)
		}
		log.Println("ok")
	case "send":
		fs := flag.NewFlagSet("send", flag.ExitOnError)
		typ := fs.String("type", "PRODUCT_RECEIVED", "")
		eventID := fs.String("id", "", "")
		product := fs.String("product", "", "")
		zone := fs.String("zone", "", "")
		fromZ := fs.String("from", "", "")
		toZ := fs.String("to", "", "")
		qty := fs.Int("qty", 0, "")
		counted := fs.Int("counted", 0, "")
		order := fs.String("order", "", "")
		supplier := fs.String("supplier", "", "")
		schemaVer := fs.Int("schema", 2, "")
		when := fs.String("time", "", "")
		lines := fs.String("lines", "", "")
		_ = fs.Parse(os.Args[2:])
		w := mustRegister(brokers)
		defer w.Close()
		ts := time.Now().UTC()
		if *when != "" {
			var err error
			ts, err = time.Parse(time.RFC3339, *when)
			if err != nil {
				log.Fatal(err)
			}
		}
		ev := model.WarehouseEvent{
			EventID:    *eventID,
			EventType:  *typ,
			OccurredAt: ts,
		}
		if *eventID == "" {
			ev.EventID = uuid.NewString()
		}
		if *product != "" {
			ev.ProductID = ptrString(*product)
		}
		if *zone != "" {
			ev.ZoneID = ptrString(*zone)
		}
		if *fromZ != "" {
			ev.FromZoneID = ptrString(*fromZ)
		}
		if *toZ != "" {
			ev.ToZoneID = ptrString(*toZ)
		}
		if *qty != 0 {
			ev.Quantity = ptrInt(*qty)
		}
		if *counted != 0 {
			ev.CountedQty = ptrInt(*counted)
		}
		if *order != "" {
			ev.OrderID = ptrString(*order)
		}
		if *supplier != "" && *schemaVer == 2 {
			ev.SupplierID = ptrString(*supplier)
		}
		if *lines != "" {
			ol, err := parseLines(*lines)
			if err != nil {
				log.Fatal(err)
			}
			ev.OrderLines = &ol
		}
		if err := send(context.Background(), enc, w, sr, &ev, *schemaVer); err != nil {
			log.Fatal(err)
		}
		log.Println(ev.EventID)
	case "scenario":
		ctx := context.Background()
		w := mustRegister(brokers)
		defer w.Close()
		oID := uuid.NewString()
		t0 := time.Now().UTC()
		steps := []model.WarehouseEvent{
			{EventID: uuid.NewString(), EventType: model.ProductReceived, OccurredAt: t0, ProductID: ptrString("SKU-001"), ZoneID: ptrString("ZONE-A"), Quantity: ptrInt(100), SupplierID: ptrString("SUP-INIT")},
			{EventID: uuid.NewString(), EventType: model.ProductReserved, OccurredAt: t0.Add(time.Minute), ProductID: ptrString("SKU-001"), ZoneID: ptrString("ZONE-A"), Quantity: ptrInt(30)},
			{EventID: uuid.NewString(), EventType: model.ProductMoved, OccurredAt: t0.Add(2 * time.Minute), ProductID: ptrString("SKU-001"), FromZoneID: ptrString("ZONE-A"), ToZoneID: ptrString("ZONE-B"), Quantity: ptrInt(20)},
			{EventID: uuid.NewString(), EventType: model.ProductShipped, OccurredAt: t0.Add(3 * time.Minute), ProductID: ptrString("SKU-001"), ZoneID: ptrString("ZONE-A"), Quantity: ptrInt(10)},
			{EventID: uuid.NewString(), EventType: model.OrderCreated, OccurredAt: t0.Add(4 * time.Minute), OrderID: &oID, OrderLines: &[]model.OrderLine{{ProductID: "SKU-001", ZoneID: "ZONE-A", Quantity: 15}}},
			{EventID: uuid.NewString(), EventType: model.OrderCompleted, OccurredAt: t0.Add(5 * time.Minute), OrderID: &oID},
		}
		for _, ev := range steps {
			if err := send(ctx, enc, w, sr, &ev, 2); err != nil {
				log.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond)
		}
		log.Println("done", oID)
	case "bad":
		ctx := context.Background()
		w := mustRegister(brokers)
		defer w.Close()
		ev := model.WarehouseEvent{
			EventID:    uuid.NewString(),
			EventType:  model.ProductShipped,
			OccurredAt: time.Now().UTC(),
			ProductID:  ptrString("SKU-X"),
			ZoneID:     ptrString("ZONE-A"),
			Quantity:   ptrInt(-5),
		}
		if err := send(ctx, enc, w, sr, &ev, 2); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("unknown")
	}
}

func parseLines(s string) ([]model.OrderLine, error) {
	var out []model.OrderLine
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		ch := strings.Split(part, ",")
		if len(ch) != 3 {
			return nil, fmt.Errorf("line %q", part)
		}
		var ol model.OrderLine
		ol.ProductID = strings.TrimSpace(ch[0])
		ol.ZoneID = strings.TrimSpace(ch[1])
		n, err := strconv.Atoi(strings.TrimSpace(ch[2]))
		if err != nil {
			return nil, err
		}
		ol.Quantity = n
		out = append(out, ol)
	}
	return out, nil
}

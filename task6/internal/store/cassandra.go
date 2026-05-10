package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"warehouse/internal/model"
)

var ErrCassandra = errors.New("cassandra batch")

type Store struct {
	sess *gocql.Session
}

func New(hosts []string, keyspace string) (*Store, error) {
	cfg := gocql.NewCluster(hosts...)
	cfg.Keyspace = keyspace
	cfg.Consistency = gocql.Quorum
	cfg.ConnectTimeout = 20 * time.Second
	cfg.Timeout = 20 * time.Second
	sess, err := cfg.CreateSession()
	if err != nil {
		return nil, err
	}
	return &Store{sess: sess}, nil
}

func (s *Store) Close() {
	s.sess.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.sess.Query("SELECT release_version FROM system.local").WithContext(ctx).Consistency(gocql.One).Scan(new(string))
}

type invRow struct {
	Av   int64
	Rsv  int64
	Last time.Time
	Sup  *string
}

func (s *Store) getPZ(ctx context.Context, productID, zoneID string) (invRow, error) {
	var r invRow
	q := `SELECT available_quantity, reserved_quantity, last_event_ts, supplier_id FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?`
	err := s.sess.Query(q, productID, zoneID).Consistency(gocql.One).WithContext(ctx).Scan(&r.Av, &r.Rsv, &r.Last, &r.Sup)
	if err == gocql.ErrNotFound {
		return invRow{}, nil
	}
	return r, err
}

func stale(row invRow, ts time.Time) bool {
	if row.Last.IsZero() {
		return false
	}
	return ts.Truncate(time.Millisecond).Before(row.Last.Truncate(time.Millisecond))
}

func supMerge(kind string, row invRow, ev *model.WarehouseEvent) *string {
	if kind == model.ProductReceived && ev.SupplierID != nil {
		return ev.SupplierID
	}
	return row.Sup
}

func ptrStr(s *string) interface{} {
	if s == nil {
		return nil
	}
	return *s
}

func writeTriple(b *gocql.Batch, productID, zoneID string, av, rsv int64, ts time.Time, sup *string) {
	b.Query(`UPDATE inventory_by_product_zone SET available_quantity = ?, reserved_quantity = ?, last_event_ts = ?, supplier_id = ? WHERE product_id = ? AND zone_id = ?`,
		av, rsv, ts, ptrStr(sup), productID, zoneID)
	b.Query(`UPDATE inventory_by_product SET available_quantity = ?, reserved_quantity = ?, last_event_ts = ?, supplier_id = ? WHERE product_id = ? AND zone_id = ?`,
		av, rsv, ts, ptrStr(sup), productID, zoneID)
	b.Query(`UPDATE inventory_by_zone SET available_quantity = ?, reserved_quantity = ?, last_event_ts = ?, supplier_id = ? WHERE zone_id = ? AND product_id = ?`,
		av, rsv, ts, ptrStr(sup), zoneID, productID)
}

func (s *Store) Processed(ctx context.Context, eventID string) (bool, error) {
	var x string
	err := s.sess.Query(`SELECT event_id FROM processed_events WHERE event_id = ?`, eventID).
		Consistency(gocql.One).WithContext(ctx).Scan(&x)
	if err == gocql.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Apply(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	ok, err := s.Processed(ctx, ev.EventID)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	switch ev.EventType {
	case model.ProductReceived:
		return s.applyReceived(ctx, ev, partition, offset)
	case model.ProductShipped:
		return s.applyShipped(ctx, ev, partition, offset)
	case model.ProductMoved:
		return s.applyMoved(ctx, ev, partition, offset)
	case model.ProductReserved:
		return s.applyReserved(ctx, ev, partition, offset)
	case model.ProductReleased:
		return s.applyReleased(ctx, ev, partition, offset)
	case model.InventoryCounted:
		return s.applyCounted(ctx, ev, partition, offset)
	case model.OrderCreated:
		return s.applyOrderCreated(ctx, ev, partition, offset)
	case model.OrderCompleted:
		return s.applyOrderCompleted(ctx, ev, partition, offset)
	default:
		return fmt.Errorf("unknown event_type %q", ev.EventType)
	}
}

func auditQueries(b *gocql.Batch, ev *model.WarehouseEvent, partition int, offset int64) {
	tu := gocql.UUIDFromTime(time.Now().UTC())
	b.Query(`INSERT INTO event_audit (event_id, occurred_at, event_type, kafka_partition, kafka_offset) VALUES (?, ?, ?, ?, ?)`,
		ev.EventID, tu, ev.EventType, partition, offset)
}

func finishBatch(s *gocql.Session, ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64, b *gocql.Batch) error {
	b.Query(`INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?)`, ev.EventID, time.Now().UTC())
	auditQueries(b, ev, partition, offset)
	if err := s.ExecuteBatch(b); err != nil {
		return fmt.Errorf("%w: %w", ErrCassandra, err)
	}
	return nil
}

func (s *Store) skipDuplicate(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	b.Query(`INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?)`, ev.EventID, time.Now().UTC())
	auditQueries(b, ev, partition, offset)
	if err := s.sess.ExecuteBatch(b); err != nil {
		return fmt.Errorf("%w: %w", ErrCassandra, err)
	}
	return nil
}

func (s *Store) applyReceived(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	if ev.ProductID == nil || ev.ZoneID == nil || ev.Quantity == nil {
		return fmt.Errorf("missing fields")
	}
	if *ev.Quantity <= 0 {
		return fmt.Errorf("invalid quantity %d", *ev.Quantity)
	}
	row, err := s.getPZ(ctx, *ev.ProductID, *ev.ZoneID)
	if err != nil {
		return err
	}
	if stale(row, ev.OccurredAt) {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	av := row.Av + int64(*ev.Quantity)
	sup := supMerge(model.ProductReceived, row, ev)
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	writeTriple(b, *ev.ProductID, *ev.ZoneID, av, row.Rsv, ev.OccurredAt, sup)
	return finishBatch(s.sess, ctx, ev, partition, offset, b)
}

func (s *Store) applyShipped(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	if ev.ProductID == nil || ev.ZoneID == nil || ev.Quantity == nil {
		return fmt.Errorf("missing fields")
	}
	if *ev.Quantity <= 0 {
		return fmt.Errorf("invalid quantity %d", *ev.Quantity)
	}
	row, err := s.getPZ(ctx, *ev.ProductID, *ev.ZoneID)
	if err != nil {
		return err
	}
	if stale(row, ev.OccurredAt) {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	if row.Av < int64(*ev.Quantity) {
		return fmt.Errorf("insufficient available %d need %d", row.Av, *ev.Quantity)
	}
	av := row.Av - int64(*ev.Quantity)
	sup := supMerge("", row, ev)
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	writeTriple(b, *ev.ProductID, *ev.ZoneID, av, row.Rsv, ev.OccurredAt, sup)
	return finishBatch(s.sess, ctx, ev, partition, offset, b)
}

func (s *Store) applyMoved(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	if ev.ProductID == nil || ev.FromZoneID == nil || ev.ToZoneID == nil || ev.Quantity == nil {
		return fmt.Errorf("missing fields")
	}
	if *ev.Quantity <= 0 {
		return fmt.Errorf("invalid quantity %d", *ev.Quantity)
	}
	if *ev.FromZoneID == *ev.ToZoneID {
		return fmt.Errorf("from and to equal")
	}
	fromRow, err := s.getPZ(ctx, *ev.ProductID, *ev.FromZoneID)
	if err != nil {
		return err
	}
	toRow, err := s.getPZ(ctx, *ev.ProductID, *ev.ToZoneID)
	if err != nil {
		return err
	}
	if stale(fromRow, ev.OccurredAt) || stale(toRow, ev.OccurredAt) {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	if fromRow.Av < int64(*ev.Quantity) {
		return fmt.Errorf("insufficient available on source")
	}
	fromAv := fromRow.Av - int64(*ev.Quantity)
	toAv := toRow.Av + int64(*ev.Quantity)
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	writeTriple(b, *ev.ProductID, *ev.FromZoneID, fromAv, fromRow.Rsv, ev.OccurredAt, supMerge("", fromRow, ev))
	writeTriple(b, *ev.ProductID, *ev.ToZoneID, toAv, toRow.Rsv, ev.OccurredAt, supMerge("", toRow, ev))
	return finishBatch(s.sess, ctx, ev, partition, offset, b)
}

func (s *Store) applyReserved(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	if ev.ProductID == nil || ev.ZoneID == nil || ev.Quantity == nil {
		return fmt.Errorf("missing fields")
	}
	if *ev.Quantity <= 0 {
		return fmt.Errorf("invalid quantity %d", *ev.Quantity)
	}
	row, err := s.getPZ(ctx, *ev.ProductID, *ev.ZoneID)
	if err != nil {
		return err
	}
	if stale(row, ev.OccurredAt) {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	if row.Av < int64(*ev.Quantity) {
		return fmt.Errorf("insufficient available")
	}
	av := row.Av - int64(*ev.Quantity)
	rsv := row.Rsv + int64(*ev.Quantity)
	sup := supMerge("", row, ev)
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	writeTriple(b, *ev.ProductID, *ev.ZoneID, av, rsv, ev.OccurredAt, sup)
	return finishBatch(s.sess, ctx, ev, partition, offset, b)
}

func (s *Store) applyReleased(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	if ev.ProductID == nil || ev.ZoneID == nil || ev.Quantity == nil {
		return fmt.Errorf("missing fields")
	}
	if *ev.Quantity <= 0 {
		return fmt.Errorf("invalid quantity %d", *ev.Quantity)
	}
	row, err := s.getPZ(ctx, *ev.ProductID, *ev.ZoneID)
	if err != nil {
		return err
	}
	if stale(row, ev.OccurredAt) {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	if row.Rsv < int64(*ev.Quantity) {
		return fmt.Errorf("insufficient reserved")
	}
	av := row.Av + int64(*ev.Quantity)
	rsv := row.Rsv - int64(*ev.Quantity)
	sup := supMerge("", row, ev)
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	writeTriple(b, *ev.ProductID, *ev.ZoneID, av, rsv, ev.OccurredAt, sup)
	return finishBatch(s.sess, ctx, ev, partition, offset, b)
}

func (s *Store) applyCounted(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	if ev.ProductID == nil || ev.ZoneID == nil || ev.CountedQty == nil {
		return fmt.Errorf("missing fields")
	}
	if *ev.CountedQty < 0 {
		return fmt.Errorf("invalid counted_quantity %d", *ev.CountedQty)
	}
	row, err := s.getPZ(ctx, *ev.ProductID, *ev.ZoneID)
	if err != nil {
		return err
	}
	if stale(row, ev.OccurredAt) {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	av := int64(*ev.CountedQty)
	sup := supMerge("", row, ev)
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	writeTriple(b, *ev.ProductID, *ev.ZoneID, av, row.Rsv, ev.OccurredAt, sup)
	return finishBatch(s.sess, ctx, ev, partition, offset, b)
}

func (s *Store) applyOrderCreated(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	if ev.OrderID == nil || ev.OrderLines == nil || len(*ev.OrderLines) == 0 {
		return fmt.Errorf("missing order fields")
	}
	var st string
	err := s.sess.Query(`SELECT status FROM orders WHERE order_id = ?`, *ev.OrderID).
		Consistency(gocql.One).WithContext(ctx).Scan(&st)
	if err == nil {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	if err != gocql.ErrNotFound {
		return err
	}
	type adj struct {
		row  invRow
		p, z string
		qty  int
	}
	adjs := make([]adj, 0, len(*ev.OrderLines))
	for _, ln := range *ev.OrderLines {
		if ln.Quantity <= 0 {
			return fmt.Errorf("invalid line quantity")
		}
		row, err := s.getPZ(ctx, ln.ProductID, ln.ZoneID)
		if err != nil {
			return err
		}
		if stale(row, ev.OccurredAt) {
			return s.skipDuplicate(ctx, ev, partition, offset)
		}
		if row.Av < int64(ln.Quantity) {
			return fmt.Errorf("insufficient available for %s/%s", ln.ProductID, ln.ZoneID)
		}
		adjs = append(adjs, adj{row: row, p: ln.ProductID, z: ln.ZoneID, qty: ln.Quantity})
	}
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	b.Query(`INSERT INTO orders (order_id, status, last_event_ts) VALUES (?, ?, ?)`,
		*ev.OrderID, model.OrderStatusCreated, ev.OccurredAt)
	for _, a := range adjs {
		av := a.row.Av - int64(a.qty)
		rsv := a.row.Rsv + int64(a.qty)
		sup := supMerge("", a.row, ev)
		writeTriple(b, a.p, a.z, av, rsv, ev.OccurredAt, sup)
		b.Query(`INSERT INTO order_items (order_id, product_id, zone_id, quantity) VALUES (?, ?, ?, ?)`,
			*ev.OrderID, a.p, a.z, a.qty)
	}
	return finishBatch(s.sess, ctx, ev, partition, offset, b)
}

func (s *Store) applyOrderCompleted(ctx context.Context, ev *model.WarehouseEvent, partition int, offset int64) error {
	if ev.OrderID == nil {
		return fmt.Errorf("missing order_id")
	}
	var st string
	var last time.Time
	err := s.sess.Query(`SELECT status, last_event_ts FROM orders WHERE order_id = ?`, *ev.OrderID).
		Consistency(gocql.One).WithContext(ctx).Scan(&st, &last)
	if err == gocql.ErrNotFound {
		return fmt.Errorf("order not found")
	}
	if err != nil {
		return err
	}
	if st == model.OrderStatusCompleted {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	if st != model.OrderStatusCreated {
		return fmt.Errorf("invalid order status %q", st)
	}
	if !last.IsZero() && ev.OccurredAt.Truncate(time.Millisecond).Before(last.Truncate(time.Millisecond)) {
		return s.skipDuplicate(ctx, ev, partition, offset)
	}
	iter := s.sess.Query(`SELECT product_id, zone_id, quantity FROM order_items WHERE order_id = ?`, *ev.OrderID).
		Consistency(gocql.One).WithContext(ctx).Iter()
	type line struct {
		p, z string
		q    int
	}
	var lines []line
	var pid, zid string
	var q int
	for iter.Scan(&pid, &zid, &q) {
		lines = append(lines, line{p: pid, z: zid, q: q})
	}
	if err := iter.Close(); err != nil {
		return err
	}
	lineRows := make([]invRow, len(lines))
	for i, ln := range lines {
		row, err := s.getPZ(ctx, ln.p, ln.z)
		if err != nil {
			return err
		}
		if stale(row, ev.OccurredAt) {
			return s.skipDuplicate(ctx, ev, partition, offset)
		}
		if row.Rsv < int64(ln.q) {
			return fmt.Errorf("insufficient reserved for line")
		}
		lineRows[i] = row
	}
	b := s.sess.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	b.Query(`UPDATE orders SET status = ?, last_event_ts = ? WHERE order_id = ?`,
		model.OrderStatusCompleted, ev.OccurredAt, *ev.OrderID)
	for i, ln := range lines {
		row := lineRows[i]
		rsv := row.Rsv - int64(ln.q)
		sup := supMerge("", row, ev)
		writeTriple(b, ln.p, ln.z, row.Av, rsv, ev.OccurredAt, sup)
	}
	return finishBatch(s.sess, ctx, ev, partition, offset, b)
}

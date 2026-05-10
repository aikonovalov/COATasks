package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"warehouse/internal/encoding"
	"warehouse/internal/migrate"
	"warehouse/internal/model"
	"warehouse/internal/store"
)

type dlqPayload struct {
	OriginalB64 string `json:"original_event_bytes_b64"`
	ErrReason   string `json:"error_reason"`
	ErrCode     string `json:"error_code"`
	FailedAt    string `json:"failed_at"`
	KafkaMeta   struct {
		Partition int   `json:"partition"`
		Offset    int64 `json:"offset"`
	} `json:"kafka_metadata"`
}

func env(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func dialKafka(ctx context.Context, brokers []string) error {
	var last error
	for _, b := range brokers {
		c, err := kafka.DialContext(ctx, "tcp", b)
		if err != nil {
			last = err
			continue
		}
		c.Close()
		return nil
	}
	return last
}

func refreshLag(ctx context.Context, brokers []string, topic string, parts int, off *sync.Map, g *prometheus.GaugeVec) {
	b := brokers[0]
	for i := 0; i < parts; i++ {
		conn, err := kafka.DialLeader(ctx, "tcp", b, topic, i)
		if err != nil {
			continue
		}
		hi, err := conn.ReadLastOffset()
		conn.Close()
		if err != nil {
			continue
		}
		var cur int64
		if v, ok := off.Load(i); ok {
			cur = v.(int64)
		}
		lag := hi - cur
		if lag < 0 {
			lag = 0
		}
		g.WithLabelValues(strconv.Itoa(i)).Set(float64(lag))
	}
}

func main() {
	kBrokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	srURL := env("SCHEMA_REGISTRY", "http://schema-registry:8081")
	cHosts := strings.Split(env("CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3"), ",")
	httpAddr := env("HTTP_ADDR", "0.0.0.0:9080")
	topic := env("KAFKA_TOPIC", "warehouse-events")
	dlqTopic := env("KAFKA_DLQ", "warehouse-events-dlq")
	partitions := 3
	if p := os.Getenv("KAFKA_PARTITIONS"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			partitions = n
		}
	}
	startOffset := kafka.LastOffset
	if strings.EqualFold(env("KAFKA_START", "first"), "first") {
		startOffset = kafka.FirstOffset
	}

	res := encoding.NewResolver(srURL)

	evCnt := promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "events_processed_total",
		Help: "processed events",
	}, []string{"event_type"})
	dur := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "event_processing_duration_seconds",
		Help:    "event handler seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"event_type"})
	cassWrErr := promauto.NewCounter(prometheus.CounterOpts{
		Name: "cassandra_write_errors_total",
		Help: "cassandra batch failures",
	})
	lagG := promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_lag",
		Help: "approx highwater minus last committed offset per partition",
	}, []string{"partition"})

	for p := 0; p < partitions; p++ {
		lagG.WithLabelValues(strconv.Itoa(p)).Set(0)
	}

	if err := initSchema(cHosts); err != nil {
		log.Fatal(err)
	}

	st, err := store.New(cHosts, "warehouse")
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	var offMu sync.Mutex
	committed := make(map[int]int64)
	offMap := sync.Map{}
	for i := 0; i < partitions; i++ {
		offMap.Store(i, int64(0))
	}

	ctx := context.Background()
	dlqW := &kafka.Writer{
		Addr:                   kafka.TCP(kBrokers...),
		Topic:                  dlqTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer dlqW.Close()

	sendDLQ := func(raw []byte, part int, o int64, reason, code string) {
		var d dlqPayload
		d.OriginalB64 = base64.StdEncoding.EncodeToString(raw)
		d.ErrReason = reason
		d.ErrCode = code
		d.FailedAt = time.Now().UTC().Format(time.RFC3339Nano)
		d.KafkaMeta.Partition = part
		d.KafkaMeta.Offset = o
		b, _ := json.Marshal(d)
		_ = dlqW.WriteMessages(context.Background(), kafka.Message{Value: b})
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kBrokers,
		GroupID:        "warehouse-state-consumer",
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		StartOffset:    startOffset,
		CommitInterval: 0,
	})
	defer r.Close()

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			offMu.Lock()
			m := make(map[int]int64, len(committed))
			for k, v := range committed {
				m[k] = v
			}
			offMu.Unlock()
			for p, v := range m {
				offMap.Store(p, v)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			refreshLag(ctx, kBrokers, topic, partitions, &offMap, lagG)
			cancel()
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := st.Ping(ctx); err != nil {
			http.Error(w, "cassandra", http.StatusServiceUnavailable)
			return
		}
		if err := dialKafka(ctx, kBrokers); err != nil {
			http.Error(w, "kafka", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	go func() {
		log.Fatal(http.ListenAndServe(httpAddr, nil))
	}()

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}
		t0 := time.Now()
		var ev model.WarehouseEvent
		if decErr := res.DecodeWarehousing(msg.Value, &ev); decErr != nil {
			sendDLQ(msg.Value, msg.Partition, msg.Offset, decErr.Error(), "DECODE_ERROR")
			if err := r.CommitMessages(ctx, msg); err != nil {
				log.Println("commit", err)
			}
			continue
		}
		if regVer, err := res.RegistrySchemaVersion(msg.Value); err == nil && regVer == 1 {
			ev.SupplierID = nil
		}
		aerr := st.Apply(ctx, &ev, msg.Partition, msg.Offset)
		if aerr != nil {
			if errors.Is(aerr, store.ErrCassandra) {
				cassWrErr.Inc()
			}
			sendDLQ(msg.Value, msg.Partition, msg.Offset, aerr.Error(), "APPLY_ERROR")
			if err := r.CommitMessages(ctx, msg); err != nil {
				log.Println("commit", err)
			}
			continue
		}
		evCnt.WithLabelValues(ev.EventType).Inc()
		dur.WithLabelValues(ev.EventType).Observe(time.Since(t0).Seconds())
		offMu.Lock()
		if cur, ok := committed[msg.Partition]; !ok || msg.Offset+1 > cur {
			committed[msg.Partition] = msg.Offset + 1
		}
		offMu.Unlock()
		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Println("commit", err)
			continue
		}
		log.Printf("handled event_id=%s type=%s partition=%d offset=%d", ev.EventID, ev.EventType, msg.Partition, msg.Offset)
	}
}

func initSchema(hosts []string) error {
	cfg := gocql.NewCluster(hosts...)
	cfg.Keyspace = ""
	cfg.Consistency = gocql.Quorum
	cfg.ConnectTimeout = 15 * time.Second
	cfg.Timeout = 30 * time.Second
	var last error
	for i := 0; i < 120; i++ {
		sess, err := cfg.CreateSession()
		if err != nil {
			last = err
			time.Sleep(2 * time.Second)
			continue
		}
		err = migrate.Apply(sess)
		sess.Close()
		if err == nil {
			return nil
		}
		last = err
		log.Println("migrate", err)
		time.Sleep(2 * time.Second)
	}
	return last
}

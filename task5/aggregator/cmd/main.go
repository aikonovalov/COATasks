package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"movie/aggregator/internal/api"
	"movie/aggregator/internal/config"
	"movie/aggregator/internal/metrics"
	"movie/aggregator/internal/s3export"
	"movie/aggregator/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := config.Load()

	var pg *store.Postgres
	for i := 0; i < 10; i++ {
		var err error
		pg, err = store.NewPostgres(context.Background(), cfg.PostgresDSN)
		if err == nil {
			break
		}

		slog.Warn("postgres not ready, retrying", "attempt", i+1, "err", err)

		time.Sleep(3 * time.Second)
	}

	if pg == nil {
		slog.Error("could not connect to postgres after retries")
		os.Exit(1)
	}

	defer pg.Close()

	var exp *s3export.Exporter
	if cfg.S3Enabled {
		var err error
		exp, err = s3export.New(cfg.S3Endpoint, cfg.S3Region, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, pg)
		if err != nil {
			slog.Error("s3 client init", "err", err)
			os.Exit(1)
		}

		slog.Info("s3 export enabled", "endpoint", cfg.S3Endpoint, "bucket", cfg.S3Bucket)
	}

	svc := metrics.NewService(cfg.ClickHouseURL, pg, exp)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		runOnce(ctx, svc)

		ticker := time.NewTicker(cfg.AggregateInterval)

		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				runOnce(ctx, svc)
			case <-ctx.Done():
				return
			}
		}
	}()

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	api.NewHandler(svc).RegisterRoutes(r)

	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: r}
	go func() {
		<-ctx.Done()

		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)

		defer shutCancel()

		srv.Shutdown(shutCtx)
	}()

	slog.Info("aggregator started", "port", cfg.HTTPPort, "interval", cfg.AggregateInterval)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server error", "err", err)
	}
}

func runOnce(ctx context.Context, svc *metrics.Service) {
	yesterday := time.Now().UTC().AddDate(0, 0, -1)
	
	if err := svc.RunFor(ctx, yesterday); err != nil {
		slog.Error("scheduled aggregation failed", "err", err)
	}
}

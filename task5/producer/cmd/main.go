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

	"movie/producer/internal/api"
	"movie/producer/internal/config"
	"movie/producer/internal/generator"
	"movie/producer/internal/kafka"
)

func main() {
	cfg := config.Load()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		slog.Error("failed to create Kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	gen := generator.New(producer, cfg)

	if cfg.GeneratorMode {
		slog.Info("auto-starting event generator")
		go gen.Run(ctx)
	}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	api.NewHandler(producer, gen, ctx).RegisterRoutes(r)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: r,
	}

	go func() {
		slog.Info("HTTP server listening", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			cancel()
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case s := <-sig:
		slog.Info("received signal", "signal", s)
	case <-ctx.Done():
	}

	gen.Stop()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	slog.Info("producer shutdown complete")
}

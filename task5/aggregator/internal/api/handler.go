package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"movie/aggregator/internal/metrics"
)

type Handler struct {
	svc *metrics.Service
}

func NewHandler(svc *metrics.Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Get("/health", h.health)
	r.Post("/api/v1/aggregate", h.triggerAggregate)
	r.Post("/api/v1/export", h.triggerExport)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) triggerAggregate(w http.ResponseWriter, r *http.Request) {
	date := time.Now().UTC().AddDate(0, 0, -1)

	if ds := r.URL.Query().Get("date"); ds != "" {
		t, err := time.Parse("2006-01-02", ds)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid date, use YYYY-MM-DD",
			})

			return
		}

		date = t
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	
	defer cancel()

	if err := h.svc.RunFor(ctx, date); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})

		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"date":   date.Format("2006-01-02"),
	})
}

func (h *Handler) triggerExport(w http.ResponseWriter, r *http.Request) {
	date := time.Now().UTC().AddDate(0, 0, -1)

	if ds := r.URL.Query().Get("date"); ds != "" {
		t, err := time.Parse("2006-01-02", ds)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid date, use YYYY-MM-DD",
			})

			return
		}

		date = t
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)

	defer cancel()

	if err := h.svc.ExportToS3(ctx, date); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})

		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"date":    date.Format("2006-01-02"),
		"format":  "csv",
		"pattern": "s3://movie-analytics/daily/YYYY-MM-DD/aggregates.csv",
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

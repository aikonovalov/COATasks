package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventpb "movie/producer/gen"
	"movie/producer/internal/generator"
	kafkapkg "movie/producer/internal/kafka"
)

type Handler struct {
	producer  *kafkapkg.Producer
	generator *generator.Generator
	appCtx    context.Context
}

func NewHandler(p *kafkapkg.Producer, g *generator.Generator, appCtx context.Context) *Handler {
	return &Handler{producer: p, generator: g, appCtx: appCtx}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Get("/api/v1/health", h.health)
	r.Post("/api/v1/events", h.publishEvent)
	r.Post("/api/v1/generator/start", h.startGenerator)
	r.Post("/api/v1/generator/stop", h.stopGenerator)
}

type eventRequest struct {
	UserID          string `json:"user_id"`
	MovieID         string `json:"movie_id"`
	EventType       string `json:"event_type"`
	DeviceType      string `json:"device_type"`
	SessionID       string `json:"session_id"`
	ProgressSeconds int32  `json:"progress_seconds"`
}

var eventTypeMap = map[string]eventpb.EventType{
	"VIEW_STARTED":  eventpb.EventType_VIEW_STARTED,
	"VIEW_FINISHED": eventpb.EventType_VIEW_FINISHED,
	"VIEW_PAUSED":   eventpb.EventType_VIEW_PAUSED,
	"VIEW_RESUMED":  eventpb.EventType_VIEW_RESUMED,
	"LIKED":         eventpb.EventType_LIKED,
	"SEARCHED":      eventpb.EventType_SEARCHED,
}

var deviceTypeMap = map[string]eventpb.DeviceType{
	"MOBILE":  eventpb.DeviceType_MOBILE,
	"DESKTOP": eventpb.DeviceType_DESKTOP,
	"TV":      eventpb.DeviceType_TV,
	"TABLET":  eventpb.DeviceType_TABLET,
}

func (h *Handler) publishEvent(w http.ResponseWriter, r *http.Request) {
	var req eventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	if req.UserID == "" || req.MovieID == "" || req.EventType == "" || req.DeviceType == "" || req.SessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "required fields: user_id, movie_id, event_type, device_type, session_id",
		})
		return
	}

	evType, ok := eventTypeMap[req.EventType]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid event_type; valid: VIEW_STARTED VIEW_FINISHED VIEW_PAUSED VIEW_RESUMED LIKED SEARCHED",
		})
		return
	}

	devType, ok := deviceTypeMap[req.DeviceType]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid device_type; valid: MOBILE DESKTOP TV TABLET",
		})
		return
	}

	id := uuid.New()
	event := &eventpb.Event{
		EventId:         id[:],
		UserId:          req.UserID,
		MovieId:         req.MovieID,
		EventType:       evType,
		Timestamp:       timestamppb.New(time.Now().UTC()),
		DeviceType:      devType,
		SessionId:       req.SessionID,
		ProgressSeconds: req.ProgressSeconds,
	}

	if err := h.producer.Publish(r.Context(), event); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to publish event"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"event_id": hex.EncodeToString(event.EventId),
		"status":   "published",
	})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) startGenerator(w http.ResponseWriter, r *http.Request) {
	if h.generator.IsRunning() {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "generator is already running"})
		return
	}
	go h.generator.Run(h.appCtx)
	writeJSON(w, http.StatusOK, map[string]string{"status": "generator started"})
}

func (h *Handler) stopGenerator(w http.ResponseWriter, r *http.Request) {
	h.generator.Stop()
	writeJSON(w, http.StatusOK, map[string]string{"status": "generator stopped"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

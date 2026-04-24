package generator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventpb "movie/producer/gen"
	"movie/producer/internal/config"
)

type Publisher interface {
	Publish(ctx context.Context, event *eventpb.Event) error
}

type sessionState int

const (
	stateIdle     sessionState = iota
	stateWatching
	statePaused
)

type userSession struct {
	userID          string
	sessionID       string
	movieID         string
	state           sessionState
	progressSeconds int32
	deviceType      eventpb.DeviceType
}

type Generator struct {
	pub Publisher
	cfg *config.Config

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
}

func New(pub Publisher, cfg *config.Config) *Generator {
	return &Generator{pub: pub, cfg: cfg}
}

func (g *Generator) IsRunning() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.running
}

func (g *Generator) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cancel != nil {
		g.cancel()
	}
	g.running = false
}

const (
	numUsers  = 10
	numMovies = 20
)

var deviceTypes = []eventpb.DeviceType{
	eventpb.DeviceType_MOBILE,
	eventpb.DeviceType_DESKTOP,
	eventpb.DeviceType_TV,
	eventpb.DeviceType_TABLET,
}

func (g *Generator) Run(parent context.Context) {
	g.mu.Lock()

	if g.running {
		g.mu.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(parent)
	g.cancel = cancel
	g.running = true

	g.mu.Unlock()

	defer func() {
		g.mu.Lock()

		g.running = false

		g.mu.Unlock()

		slog.Info("generator stopped")
	}()

	slog.Info("generator started",
		"users", numUsers,
		"interval_ms", g.cfg.GeneratorIntervalMS,
	)

	sessions := make([]*userSession, numUsers)
	for i := range sessions {
		sessions[i] = &userSession{
			userID:     fmt.Sprintf("user-%04d", i),
			deviceType: deviceTypes[rand.Intn(len(deviceTypes))],
		}
	}

	ticker := time.NewTicker(time.Duration(g.cfg.GeneratorIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			sess := sessions[rand.Intn(numUsers)]
			event := g.nextEvent(sess)
			if event == nil {
				continue
			}

			if err := g.pub.Publish(ctx, event); err != nil {
				slog.Warn("generator: publish failed", "error", err)
			}
		}
	}
}

func (g *Generator) nextEvent(s *userSession) *eventpb.Event {
	eventID := uuid.New()
	now := time.Now().UTC()

	switch s.state {
	case stateIdle:
		s.sessionID = uuid.NewString()
		s.movieID = fmt.Sprintf("movie-%04d", rand.Intn(numMovies))
		s.progressSeconds = 0

		if rand.Float32() < 0.15 {
			return buildEvent(eventID[:], s, eventpb.EventType_SEARCHED, now)
		}

		s.state = stateWatching

		return buildEvent(eventID[:], s, eventpb.EventType_VIEW_STARTED, now)

	case stateWatching:
		s.progressSeconds += int32(10 + rand.Intn(20))

		r := rand.Float32()
		switch {
		case r < 0.06:
			s.state = statePaused
			return buildEvent(eventID[:], s, eventpb.EventType_VIEW_PAUSED, now)
		case r < 0.11:
			return buildEvent(eventID[:], s, eventpb.EventType_LIKED, now)
		case r < 0.13:
			return buildEvent(eventID[:], s, eventpb.EventType_SEARCHED, now)
		case r < 0.22:
			s.state = stateIdle
			return buildEvent(eventID[:], s, eventpb.EventType_VIEW_FINISHED, now)
		default:
			return nil
		}

	case statePaused:
		s.state = stateWatching
		return buildEvent(eventID[:], s, eventpb.EventType_VIEW_RESUMED, now)
	}

	return nil
}

func buildEvent(id []byte, s *userSession, et eventpb.EventType, ts time.Time) *eventpb.Event {
	return &eventpb.Event{
		EventId:         id,
		UserId:          s.userID,
		MovieId:         s.movieID,
		EventType:       et,
		Timestamp:       timestamppb.New(ts),
		DeviceType:      s.deviceType,
		SessionId:       s.sessionID,
		ProgressSeconds: s.progressSeconds,
	}
}

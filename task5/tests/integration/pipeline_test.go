package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	producerURL = envOr("PRODUCER_URL", "http://movie-producer:8080")
	chURL       = envOr("CLICKHOUSE_URL", "http://clickhouse:8123")
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

func chCount(t *testing.T, query string) int {
	t.Helper()

	raw := strings.TrimSpace(chQuery(t, query))
	if raw == "" {
		return 0
	}

	var n int
	fmt.Sscan(raw, &n)

	return n
}

func chQuery(t *testing.T, query string) string {
	t.Helper()

	req, _ := http.NewRequest(http.MethodPost, chURL, strings.NewReader(query))
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("ClickHouse request error: %v", err)
		return ""
	}

	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Logf("ClickHouse %d: %s", resp.StatusCode, raw)
		return ""
	}

	return string(raw)
}

func assertEq(t *testing.T, field, want, got string) {
	t.Helper()

	if want != got {
		t.Errorf("field %s: want %q, got %q", field, want, got)
	}
}

func TestPipelineEndToEnd(t *testing.T) {
	sessionID := uuid.New().String()

	body, _ := json.Marshal(map[string]any{
		"user_id":          "e2e-test-user",
		"movie_id":         "e2e-test-movie",
		"event_type":       "VIEW_STARTED",
		"device_type":      "DESKTOP",
		"session_id":       sessionID,
		"progress_seconds": 0,
	})

	resp, err := http.Post(producerURL+"/api/v1/events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /api/v1/events: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, raw)
	}

	var pubResp struct {
		EventID string `json:"event_id"`
		Status  string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&pubResp); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}

	if pubResp.Status != "published" {
		t.Fatalf("expected status=published, got %q", pubResp.Status)
	}

	if pubResp.EventID == "" {
		t.Fatal("empty event_id in response")
	}

	t.Logf("event published: id=%s session=%s", pubResp.EventID, sessionID)

	t.Log("polling ClickHouse for the event…")

	countQ := fmt.Sprintf(
		"SELECT count() FROM movie_events WHERE session_id = '%s' FORMAT TabSeparated",
		sessionID,
	)

	var found bool
	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		if chCount(t, countQ) > 0 {
			found = true

			break
		}

		time.Sleep(2 * time.Second)
	}

	if !found {
		t.Fatalf("event not found in ClickHouse after 30 s (session_id=%s)", sessionID)
	}

	t.Log("event found in ClickHouse")

	rowQ := fmt.Sprintf(
		"SELECT user_id, movie_id, event_type, device_type, session_id, progress_seconds"+
			" FROM movie_events WHERE session_id = '%s' LIMIT 1 FORMAT JSONEachRow",
		sessionID,
	)

	raw := chQuery(t, rowQ)

	var row struct {
		UserID          string `json:"user_id"`
		MovieID         string `json:"movie_id"`
		EventType       string `json:"event_type"`
		DeviceType      string `json:"device_type"`
		SessionID       string `json:"session_id"`
		ProgressSeconds int    `json:"progress_seconds"`
	}

	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &row); err != nil {
		t.Fatalf("decode row JSON (%q): %v", raw, err)
	}

	assertEq(t, "user_id", "e2e-test-user", row.UserID)
	assertEq(t, "movie_id", "e2e-test-movie", row.MovieID)
	assertEq(t, "event_type", "VIEW_STARTED", row.EventType)
	assertEq(t, "device_type", "DESKTOP", row.DeviceType)
	assertEq(t, "session_id", sessionID, row.SessionID)

	if row.ProgressSeconds != 0 {
		t.Errorf("progress_seconds: expected 0, got %d", row.ProgressSeconds)
	}

	t.Log("Success")

}

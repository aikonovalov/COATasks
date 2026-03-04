package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
)

type responseWriterWrapper struct {
	http.ResponseWriter
	status int
}

func (w *responseWriterWrapper) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func hidePassword(body []byte) string {
	if len(body) == 0 {
		return ""
	}

	var m map[string]interface{}
	if json.Unmarshal(body, &m) != nil {
		return string(body)
	}

	
	if _, ok := m["password"]; ok {
		m["password"] = "[Данные удалены]"
	}

	out, _ := json.Marshal(m)

	return string(out)
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := uuid.New().String()
		w.Header().Set("X-Request-Id", requestID)

		var bodyForLog string
		if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete {
			if r.Body != nil {
				body, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(body))
				bodyForLog = hidePassword(body)
			}
		}

		wrap := &responseWriterWrapper{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrap, r)

		entry := map[string]interface{}{
			"request_id":  requestID,
			"method":      r.Method,
			"endpoint":    r.URL.Path,
			"status_code": wrap.status,
			"duration_ms": time.Since(start).Milliseconds(),
			"user_id":     getUserIDFromContext(r.Context()),
			"timestamp":   start.UTC().Format(time.RFC3339),
		}

		if bodyForLog != "" {
			entry["request_body"] = bodyForLog
		}

		logJSON, _ := json.Marshal(entry)
		
		fmt.Fprintln(os.Stdout, string(logJSON))
	})
}

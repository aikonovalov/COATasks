package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"movie/aggregator/internal/store"
)

type MovieView struct {
	MovieID string `json:"movie_id"`
	Views   int64  `json:"views"`
}

type DailyResult struct {
	Date           time.Time
	TotalEvents    int64
	DAU            int64
	AvgViewSeconds float64
	TopMovies      []MovieView
	Conversion     float64
	RetentionD1    float64
	RetentionD7    float64
}

type Service struct {
	chURL string
	http  *http.Client
	pg    *store.Postgres
}

func NewService(chURL string, pg *store.Postgres) *Service {
	return &Service{chURL: chURL, http: &http.Client{}, pg: pg}
}

func (s *Service) RunFor(ctx context.Context, date time.Time) error {
	d := date.Format("2006-01-02")
	start := time.Now()
	slog.Info("aggregation started", "date", d)

	res, err := s.compute(ctx, d)
	if err != nil {
		return fmt.Errorf("compute %s: %w", d, err)
	}

	if err := s.storeClickHouse(res); err != nil {
		slog.Warn("write to ClickHouse aggregate tables failed", "err", err)
	}

	if err := s.storePostgres(ctx, res); err != nil {
		return fmt.Errorf("store postgres: %w", err)
	}

	slog.Info("aggregation done",
		"date", d,
		"total_events", res.TotalEvents,
		"dau", res.DAU,
		"conversion", fmt.Sprintf("%.3f", res.Conversion),
		"retention_d1", fmt.Sprintf("%.3f", res.RetentionD1),
		"elapsed_ms", time.Since(start).Milliseconds())
	
	return nil
}

func (s *Service) compute(_ context.Context, date string) (*DailyResult, error) {
	d, _ := time.Parse("2006-01-02", date)
	res := &DailyResult{Date: d}

	if rows, err := s.query(fmt.Sprintf(`
		SELECT count() AS n FROM movie_events WHERE toDate(timestamp) = toDate('%s') FORMAT JSONEachRow`, date)); err == nil && len(rows) > 0 {
		res.TotalEvents = toInt64(rows[0]["n"])
	}

	rows, err := s.query(fmt.Sprintf(`
		SELECT uniq(user_id) AS dau FROM movie_events WHERE toDate(timestamp) = toDate('%s') FORMAT JSONEachRow`, date))
	if err != nil {
		return nil, fmt.Errorf("dau: %w", err)
	}

	if len(rows) > 0 {
		res.DAU = toInt64(rows[0]["dau"])
	}

	rows, err = s.query(fmt.Sprintf(`
		SELECT avg(progress_seconds) AS avg_sec
		FROM movie_events
		WHERE toDate(timestamp) = toDate('%s') AND event_type = 'VIEW_FINISHED'
		FORMAT JSONEachRow`, date))
	if err != nil {
		return nil, fmt.Errorf("avg_view: %w", err)
	}

	if len(rows) > 0 {
		res.AvgViewSeconds = toFloat64(rows[0]["avg_sec"])
	}

	rows, err = s.query(fmt.Sprintf(`
		SELECT movie_id, count() AS views
		FROM movie_events
		WHERE toDate(timestamp) = toDate('%s') AND event_type = 'VIEW_STARTED'
		GROUP BY movie_id ORDER BY views DESC LIMIT 10
		FORMAT JSONEachRow`, date))
	if err != nil {
		return nil, fmt.Errorf("top_movies: %w", err)
	}

	for _, r := range rows {
		res.TopMovies = append(res.TopMovies, MovieView{
			MovieID: toString(r["movie_id"]),
			Views:   toInt64(r["views"]),
		})
	}

	rows, err = s.query(fmt.Sprintf(`
		SELECT
			countIf(event_type = 'VIEW_STARTED')  AS started,
			countIf(event_type = 'VIEW_FINISHED') AS finished,
			if(countIf(event_type = 'VIEW_STARTED') > 0,
			   toFloat64(countIf(event_type = 'VIEW_FINISHED')) / countIf(event_type = 'VIEW_STARTED'),
			   0) AS conversion
		FROM movie_events
		WHERE toDate(timestamp) = toDate('%s')
		  AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
		FORMAT JSONEachRow`, date))
	if err != nil {
		return nil, fmt.Errorf("conversion: %w", err)
	}

	if len(rows) > 0 {
		res.Conversion = toFloat64(rows[0]["conversion"])
	}

	rows, err = s.query(fmt.Sprintf(`
		SELECT
			count(DISTINCT c.user_id)  AS cohort_size,
			count(DISTINCT d1.user_id) AS returned_d1,
			count(DISTINCT d7.user_id) AS returned_d7
		FROM (
			SELECT user_id FROM movie_events
			GROUP BY user_id HAVING min(toDate(timestamp)) = toDate('%s')
		) c
		LEFT JOIN (
			SELECT DISTINCT user_id FROM movie_events WHERE toDate(timestamp) = toDate('%s') + 1
		) d1 ON c.user_id = d1.user_id
		LEFT JOIN (
			SELECT DISTINCT user_id FROM movie_events WHERE toDate(timestamp) = toDate('%s') + 7
		) d7 ON c.user_id = d7.user_id
		FORMAT JSONEachRow`, date, date, date))
	if err != nil {
		return nil, fmt.Errorf("retention: %w", err)
	}

	if len(rows) > 0 {
		cohort := toFloat64(rows[0]["cohort_size"])
		d1 := toFloat64(rows[0]["returned_d1"])
		d7 := toFloat64(rows[0]["returned_d7"])

		if cohort > 0 {
			res.RetentionD1 = d1 / cohort
			res.RetentionD7 = d7 / cohort
		}
	}

	return res, nil
}

func (s *Service) storeClickHouse(m *DailyResult) error {
	date := m.Date.Format("2006-01-02")

	queries := []string{
		fmt.Sprintf("INSERT INTO agg_dau (date, dau) VALUES (toDate('%s'), %d)", date, m.DAU),
		fmt.Sprintf("INSERT INTO agg_avg_view_seconds (date, avg_seconds) VALUES (toDate('%s'), %.4f)", date, m.AvgViewSeconds),
		fmt.Sprintf("INSERT INTO agg_conversion (date, conversion) VALUES (toDate('%s'), %.6f)", date, m.Conversion),
		fmt.Sprintf("INSERT INTO agg_retention (date, retention_d1, retention_d7) VALUES (toDate('%s'), %.6f, %.6f)", date, m.RetentionD1, m.RetentionD7),
	}

	for _, q := range queries {
		if err := s.exec(q); err != nil {
			return err
		}
	}

	for i, mv := range m.TopMovies {
		safe := strings.ReplaceAll(mv.MovieID, "'", "''")

		q := fmt.Sprintf("INSERT INTO agg_top_movies (date, rank, movie_id, views) VALUES (toDate('%s'), %d, '%s', %d)",
			date, i+1, safe, mv.Views)
		
		if err := s.exec(q); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) storePostgres(ctx context.Context, m *DailyResult) error {
	topJSON, _ := json.Marshal(m.TopMovies)
	rows := []struct {
		name  string
		value float64
		extra []byte
	} {
		{"dau", float64(m.DAU), nil},
		{"avg_view_seconds", m.AvgViewSeconds, nil},
		{"conversion", m.Conversion, nil},
		{"retention_d1", m.RetentionD1, nil},
		{"retention_d7", m.RetentionD7, nil},
		{"top_movies", float64(len(m.TopMovies)), topJSON},
	}

	for _, r := range rows {
		if err := s.pg.UpsertMetric(ctx, m.Date, r.name, r.value, r.extra); err != nil {
			return fmt.Errorf("upsert %s: %w", r.name, err)
		}
	}

	return nil
}


func (s *Service) query(q string) ([]map[string]any, error) {
	resp, err := s.http.Post(s.chURL, "text/plain", strings.NewReader(q))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ch %d: %s", resp.StatusCode, body)
	}

	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()

	var rows []map[string]any
	for {
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			break
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (s *Service) exec(q string) error {
	resp, err := s.http.Post(s.chURL, "text/plain", strings.NewReader(q))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ch exec %d: %s", resp.StatusCode, body)
	}

	return nil
}


func toInt64(v any) int64 {
	switch n := v.(type) {
	case json.Number:
		i, _ := n.Int64()
		return i

	case float64:
		return int64(n)

	case string:
		var i int64
		fmt.Sscan(n, &i)

		return i
	}

	return 0
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case json.Number:
		f, _ := n.Float64()
		return f

	case float64:
		return n

	case string:
		var f float64
		fmt.Sscan(n, &f)

		return f
	}

	return 0
}

func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s

	case json.Number:
		return s.String()
	}
	
	return fmt.Sprintf("%v", v)
}

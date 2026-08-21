package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type db interface {
	Ping(context.Context) error
	Query(context.Context, string, ...any) (rowset, error)
	Close()
}

type rowset interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}

type pgDB struct {
	pool *pgxpool.Pool
}

func (p *pgDB) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *pgDB) Query(ctx context.Context, sql string, args ...any) (rowset, error) {
	return p.pool.Query(ctx, sql, args...)
}

func (p *pgDB) Close() {
	p.pool.Close()
}

type app struct {
	db  db
	log *slog.Logger
}

type summaryResp struct {
	TotalRequests     float64 `json:"total_requests"`
	RequestsPerMinute float64 `json:"requests_per_minute"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	ErrorRate         float64 `json:"error_rate"`
	UpdatedAt         string  `json:"updated_at,omitempty"`
}

type languagePairResp struct {
	LanguagePair          string  `json:"language_pair"`
	RequestCount          int64   `json:"request_count"`
	AvgLatencyMsTotal     float64 `json:"avg_latency_ms_total"`
	AvgLatencyMsTranslate float64 `json:"avg_latency_ms_translate"`
	ErrorCount            int64   `json:"error_count"`
	ErrorRate             float64 `json:"error_rate"`
}

type pointResp struct {
	WindowStart       string  `json:"window_start"`
	WindowEnd         string  `json:"window_end"`
	LanguagePair      string  `json:"language_pair,omitempty"`
	RequestCount      int64   `json:"request_count,omitempty"`
	AvgLatencyMsTotal float64 `json:"avg_latency_ms_total,omitempty"`
	ErrorCount        int64   `json:"error_count,omitempty"`
	ErrorRate         float64 `json:"error_rate,omitempty"`
}

type errorResp struct {
	Error string `json:"error"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		log.Error("database config invalid", "err", err)
		os.Exit(1)
	}
	database := &pgDB{pool: pool}
	defer database.Close()

	a := &app{db: database, log: log}
	srv := &http.Server{
		Addr:              ":" + cmp.Or(os.Getenv("PORT"), "8001"),
		Handler:           withCORS(a.routes()),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("analytics-api listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown failed", "err", err)
	}
}

func databaseURL() string {
	if raw := os.Getenv("DATABASE_URL"); raw != "" {
		return raw
	}
	host := cmp.Or(os.Getenv("POSTGRES_HOST"), "analytics-db")
	port := cmp.Or(os.Getenv("POSTGRES_PORT"), "5432")
	name := cmp.Or(os.Getenv("POSTGRES_DB"), "analytics")
	user := cmp.Or(os.Getenv("POSTGRES_USER"), "analytics")
	password := cmp.Or(os.Getenv("POSTGRES_PASSWORD"), "analytics-change-me")
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, name)
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.health)
	mux.HandleFunc("/ready", a.ready)
	mux.HandleFunc("/metrics/summary", a.summary)
	mux.HandleFunc("/metrics/language-pairs", a.languagePairs)
	mux.HandleFunc("/metrics/latency", a.latency)
	mux.HandleFunc("/metrics/errors", a.errors)
	mux.HandleFunc("/metrics/timeseries", a.timeseries)
	return mux
}

func (a *app) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "GET only"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *app) ready(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "GET only"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.db.Ping(ctx); err != nil {
		a.log.Warn("database not ready", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, errorResp{Error: "database not ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (a *app) summary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "GET only"})
		return
	}
	rows, err := a.db.Query(r.Context(), `
		SELECT metric_key, metric_value, updated_at
		FROM global_live_metrics
		WHERE metric_key IN ('requests_total', 'latest_request_count_1m', 'requests_per_minute', 'avg_latency_ms', 'error_rate')
	`)
	if err != nil {
		a.dbError(w, err)
		return
	}
	defer rows.Close()

	out := summaryResp{}
	var latest time.Time
	for rows.Next() {
		var key string
		var value float64
		var updated time.Time
		if err := rows.Scan(&key, &value, &updated); err != nil {
			a.dbError(w, err)
			return
		}
		switch key {
		case "requests_total", "latest_request_count_1m":
			if out.TotalRequests == 0 {
				out.TotalRequests = value
			}
		case "requests_per_minute":
			out.RequestsPerMinute = value
		case "avg_latency_ms":
			out.AvgLatencyMs = value
		case "error_rate":
			out.ErrorRate = value
		}
		if updated.After(latest) {
			latest = updated
		}
	}
	if err := rows.Err(); err != nil {
		a.dbError(w, err)
		return
	}
	if !latest.IsZero() {
		out.UpdatedAt = latest.UTC().Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *app) languagePairs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "GET only"})
		return
	}
	limit := queryLimit(r, 10)
	rows, err := a.db.Query(r.Context(), `
		SELECT language_pair,
		       SUM(request_count)::bigint AS request_count,
		       COALESCE(AVG(avg_latency_ms_total), 0) AS avg_latency_ms_total,
		       COALESCE(AVG(avg_latency_ms_translate), 0) AS avg_latency_ms_translate,
		       SUM(error_count)::bigint AS error_count,
		       CASE WHEN SUM(request_count) > 0 THEN SUM(error_count)::float / SUM(request_count)::float ELSE 0 END AS error_rate
		FROM language_pair_windows
		WHERE window_type = '1m'
		  AND window_start >= now() - interval '1 hour'
		GROUP BY language_pair
		ORDER BY request_count DESC, language_pair
		LIMIT $1
	`, limit)
	if err != nil {
		a.dbError(w, err)
		return
	}
	defer rows.Close()
	out := []languagePairResp{}
	for rows.Next() {
		var item languagePairResp
		if err := rows.Scan(&item.LanguagePair, &item.RequestCount, &item.AvgLatencyMsTotal, &item.AvgLatencyMsTranslate, &item.ErrorCount, &item.ErrorRate); err != nil {
			a.dbError(w, err)
			return
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		a.dbError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *app) latency(w http.ResponseWriter, r *http.Request) {
	a.windowSeries(w, r, `
		SELECT window_start, window_end, COALESCE(AVG(avg_latency_ms_total), 0) AS avg_latency_ms_total
		FROM language_pair_windows
		WHERE window_type = '1m'
		  AND window_start >= now() - interval '1 hour'
		GROUP BY window_start, window_end
		ORDER BY window_start
	`, scanLatencyPoint)
}

func (a *app) errors(w http.ResponseWriter, r *http.Request) {
	a.windowSeries(w, r, `
		SELECT window_start, window_end,
		       SUM(error_count)::bigint AS error_count,
		       CASE WHEN SUM(request_count) > 0 THEN SUM(error_count)::float / SUM(request_count)::float ELSE 0 END AS error_rate
		FROM language_pair_windows
		WHERE window_type = '1m'
		  AND window_start >= now() - interval '1 hour'
		GROUP BY window_start, window_end
		ORDER BY window_start
	`, scanErrorPoint)
}

func (a *app) timeseries(w http.ResponseWriter, r *http.Request) {
	a.windowSeries(w, r, `
		SELECT window_start, window_end,
		       SUM(request_count)::bigint AS request_count,
		       COALESCE(AVG(avg_latency_ms_total), 0) AS avg_latency_ms_total,
		       SUM(error_count)::bigint AS error_count,
		       CASE WHEN SUM(request_count) > 0 THEN SUM(error_count)::float / SUM(request_count)::float ELSE 0 END AS error_rate
		FROM language_pair_windows
		WHERE window_type = '1m'
		  AND window_start >= now() - interval '1 hour'
		GROUP BY window_start, window_end
		ORDER BY window_start
	`, scanTimeseriesPoint)
}

func (a *app) windowSeries(w http.ResponseWriter, r *http.Request, sql string, scan func(rowset) (pointResp, error)) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "GET only"})
		return
	}
	rows, err := a.db.Query(r.Context(), sql)
	if err != nil {
		a.dbError(w, err)
		return
	}
	defer rows.Close()
	out := []pointResp{}
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			a.dbError(w, err)
			return
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		a.dbError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func scanLatencyPoint(rows rowset) (pointResp, error) {
	var start, end time.Time
	var latency float64
	if err := rows.Scan(&start, &end, &latency); err != nil {
		return pointResp{}, err
	}
	return pointResp{WindowStart: start.UTC().Format(time.RFC3339), WindowEnd: end.UTC().Format(time.RFC3339), AvgLatencyMsTotal: latency}, nil
}

func scanErrorPoint(rows rowset) (pointResp, error) {
	var start, end time.Time
	var errorCount int64
	var errorRate float64
	if err := rows.Scan(&start, &end, &errorCount, &errorRate); err != nil {
		return pointResp{}, err
	}
	return pointResp{WindowStart: start.UTC().Format(time.RFC3339), WindowEnd: end.UTC().Format(time.RFC3339), ErrorCount: errorCount, ErrorRate: errorRate}, nil
}

func scanTimeseriesPoint(rows rowset) (pointResp, error) {
	var start, end time.Time
	var requestCount, errorCount int64
	var latency, errorRate float64
	if err := rows.Scan(&start, &end, &requestCount, &latency, &errorCount, &errorRate); err != nil {
		return pointResp{}, err
	}
	return pointResp{WindowStart: start.UTC().Format(time.RFC3339), WindowEnd: end.UTC().Format(time.RFC3339), RequestCount: requestCount, AvgLatencyMsTotal: latency, ErrorCount: errorCount, ErrorRate: errorRate}, nil
}

func queryLimit(r *http.Request, fallback int) int {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return fallback
	}
	if value > 100 {
		return 100
	}
	return value
}

func (a *app) dbError(w http.ResponseWriter, err error) {
	a.log.Warn("database query failed", "err", err)
	writeJSON(w, http.StatusServiceUnavailable, errorResp{Error: "database unavailable"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "86400")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

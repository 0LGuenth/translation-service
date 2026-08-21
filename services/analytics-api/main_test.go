package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeDB struct {
	pingErr  error
	queryErr error
	rows     []rowset
	closed   bool
}

func (f *fakeDB) Ping(context.Context) error {
	return f.pingErr
}

func (f *fakeDB) Query(context.Context, string, ...any) (rowset, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	if len(f.rows) == 0 {
		return &fakeRows{}, nil
	}
	rows := f.rows[0]
	f.rows = f.rows[1:]
	return rows, nil
}

func (f *fakeDB) Close() {
	f.closed = true
}

type fakeRows struct {
	values [][]any
	idx    int
	err    error
}

func (f *fakeRows) Next() bool {
	return f.idx < len(f.values)
}

func (f *fakeRows) Scan(dest ...any) error {
	if f.idx >= len(f.values) {
		return errors.New("scan after end")
	}
	row := f.values[f.idx]
	f.idx++
	for i := range dest {
		switch target := dest[i].(type) {
		case *string:
			*target = row[i].(string)
		case *float64:
			*target = row[i].(float64)
		case *int64:
			*target = row[i].(int64)
		case *time.Time:
			*target = row[i].(time.Time)
		default:
			return errors.New("unsupported scan target")
		}
	}
	return nil
}

func (f *fakeRows) Err() error {
	return f.err
}

func (f *fakeRows) Close() {}

func testApp(database db) http.Handler {
	return (&app{db: database, log: slog.New(slog.NewTextHandler(io.Discard, nil))}).routes()
}

func TestHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	testApp(&fakeDB{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestReadyReportsDatabaseFailure(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	testApp(&fakeDB{pingErr: errors.New("down")}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestSummary(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	rows := &fakeRows{values: [][]any{
		{"requests_total", 42.0, now},
		{"requests_per_minute", 7.0, now},
		{"avg_latency_ms", 123.0, now},
		{"error_rate", 0.125, now},
	}}
	req := httptest.NewRequest(http.MethodGet, "/metrics/summary", nil)
	rec := httptest.NewRecorder()
	testApp(&fakeDB{rows: []rowset{rows}}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var got summaryResp
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.TotalRequests != 42 || got.RequestsPerMinute != 7 || got.AvgLatencyMs != 123 || got.ErrorRate != 0.125 {
		t.Fatalf("unexpected summary: %+v", got)
	}
}

func TestLanguagePairs(t *testing.T) {
	rows := &fakeRows{values: [][]any{
		{"de-en", int64(10), 110.0, 80.0, int64(1), 0.1},
	}}
	req := httptest.NewRequest(http.MethodGet, "/metrics/language-pairs", nil)
	rec := httptest.NewRecorder()
	testApp(&fakeDB{rows: []rowset{rows}}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var got []languagePairResp
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].LanguagePair != "de-en" || got[0].RequestCount != 10 {
		t.Fatalf("unexpected pairs: %+v", got)
	}
}

func TestDatabaseErrorsReturnUnavailable(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics/timeseries", nil)
	rec := httptest.NewRecorder()
	testApp(&fakeDB{queryErr: errors.New("boom")}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
}

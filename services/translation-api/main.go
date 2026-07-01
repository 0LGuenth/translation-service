package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/text/language"
)

// translateReq request is the request schema for a translation
type translateReq struct {
	Text    string `json:"text"`
	SrcLang string `json:"src_lang"`
	TgtLang string `json:"tgt_lang"`
}

// translateResp is the response schema sent after translation
type translateResp struct {
	Translated         string `json:"translated"`
	Model              string `json:"model"`
	LatencyMsTotal     int64  `json:"latency_ms_total"`
	LatencyMsTranslate int64  `json:"latency_ms_translate"`
}

// backend used to translate a request
type backend interface {
	Translate(ctx context.Context, in translateReq) (translateResp, error)
}

// echoBackend to check if the deployment is working; TODO remove if translation-llm is available
type echoBackend struct{}

func (echoBackend) Translate(_ context.Context, in translateReq) (translateResp, error) {
	return translateResp{
		Translated: in.Text, // echo, not a translation
		Model:      "echo",
	}, nil
}

// httpBackend POSTs to translation-llm; TODO implement if translation-llm is available
type httpBackend struct {
	url    string
	client *http.Client
}

// validate checks non-empty text, length cap, 639-1 lang codes.
func validate(in translateReq, maxTextLen int) error {
	switch {
	case in.Text == "":
		return errors.New("text is required")
	case len(in.Text) > maxTextLen:
		return errors.New("text too long (max 5000 chars)")
	}

	if _, err := language.ParseBase(in.SrcLang); len(in.SrcLang) != 2 || err != nil {
		return errors.New("src_lang must be a 2-letter ISO 639-1 code")
	}
	if _, err := language.ParseBase(in.TgtLang); len(in.TgtLang) != 2 || err != nil {
		return errors.New("tgt_lang must be a 2-letter ISO 639-1 code")
	}
	return nil
}

// writeJSON serialises v as JSON and writes status to the response
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr writes a JSON error response with a single "error" field.
func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// handleTranslate decodes the request, validates it, delegates to the backend, and writes the response
func handleTranslate(b backend, log *slog.Logger, maxTextLen int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTotal := time.Now()
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		var in translateReq
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if err := validate(in, maxTextLen); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		startTranslate := time.Now()
		out, err := b.Translate(r.Context(), in)
		if err != nil {
			log.Error("translate failed", "err", err, "src", in.SrcLang, "tgt", in.TgtLang)
			writeErr(w, http.StatusBadGateway, err.Error())
			return
		}
		out.LatencyMsTotal = time.Since(startTotal).Milliseconds()
		out.LatencyMsTranslate = time.Since(startTranslate).Milliseconds()
		writeJSON(w, http.StatusOK, out)
	}
}

func main() {
	cfg := struct {
		port            string
		maxTextLen      int
		llmTimeout      time.Duration
		shutdownTimeout time.Duration
	}{
		port:            envOr("PORT", "8000"),
		maxTextLen:      envInt("MAX_TEXT_LENGTH", 5000),
		llmTimeout:      time.Duration(envInt("LLM_TIMEOUT_SECONDS", 30)) * time.Second,
		shutdownTimeout: time.Duration(envInt("SHUTDOWN_TIMEOUT_SECONDS", 20)) * time.Second,
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	backend := echoBackend{}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ready"}) })
	mux.HandleFunc("/translate", handleTranslate(backend, log, cfg.maxTextLen))

	srv := &http.Server{
		Addr:              ":" + cfg.port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown; drain in-flight requests.
	go func() {
		log.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
			os.Exit(1)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

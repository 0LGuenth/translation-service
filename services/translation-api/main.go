// Translation API — Go HTTP gateway.
//
// Mirrors PDF gridflex-api (slides 13–28): /translate plus /health and /ready
// for k8s probes. Translation is delegated to TRANSLATION_LLM_URL.
package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type translateReq struct {
	Text    string `json:"text"`
	SrcLang string `json:"src_lang"`
	TgtLang string `json:"tgt_lang"`
	Src     string `json:"src,omitempty"`
}

type llmTranslateReq struct {
	Text    string `json:"text"`
	SrcLang string `json:"src_lang"`
	TgtLang string `json:"tgt_lang"`
}

type translateResp struct {
	Translated string `json:"translated"`
	Model      string `json:"model"`
	LatencyMs  int64  `json:"latency_ms"`
	ReqID      string `json:"req_id,omitempty"`
}

type errorResp struct {
	Error string `json:"error"`
	ReqID string `json:"req_id,omitempty"`
}

type languagePairResp struct {
	SrcLang string `json:"src_lang"`
	TgtLang string `json:"tgt_lang"`
}

type languagesResp struct {
	Pairs []languagePairResp `json:"pairs"`
}

type requestedPairResp struct {
	SrcLang string `json:"src_lang"`
	TgtLang string `json:"tgt_lang"`
	State   string `json:"state"`
}

type loadedPairsResp struct {
	LoadedPairs   []languagePairResp `json:"loaded_pairs"`
	LoadingPairs  []languagePairResp `json:"loading_pairs"`
	RequestedPair *requestedPairResp `json:"requested_pair,omitempty"`
}

type translationEvent struct {
	ReqID              string `json:"req_id"`
	Src                string `json:"src"`
	UserIDHashed       string `json:"user_id_hashed"`
	SrcLang            string `json:"src_lang"`
	TgtLang            string `json:"tgt_lang"`
	CharCount          int    `json:"char_count"`
	Model              string `json:"model,omitempty"`
	Status             string `json:"status"`
	LatencyMsTotal     int64  `json:"latency_ms_total"`
	LatencyMsTranslate int64  `json:"latency_ms_translate"`
	EventTs            string `json:"event_ts"`
	ErrorType          string `json:"error_type,omitempty"`
}

// backend translates one request
type backend interface {
	Translate(ctx context.Context, in llmTranslateReq) (translateResp, error)
}

// httpBackend POSTs to translation-llm. Expects the same {translated,model}
// shape back
type httpBackend struct {
	url    string
	client *http.Client
}

type backendError struct {
	statusCode int
	errorType  string
	message    string
}

func (e *backendError) Error() string {
	return e.message
}

func (b *httpBackend) Translate(ctx context.Context, in llmTranslateReq) (translateResp, error) {
	body, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.url+"/translate", bytes.NewReader(body))
	if err != nil {
		return translateResp{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return translateResp{}, classifyBackendRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return translateResp{}, classifyBackendStatus(resp.StatusCode, msg)
	}
	var out translateResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return translateResp{}, err
	}
	return out, nil
}

func classifyBackendRequestError(err error) error {
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return &backendError{
			statusCode: http.StatusGatewayTimeout,
			errorType:  "llm_timeout",
			message:    "translation backend timed out",
		}
	}
	return &backendError{
		statusCode: http.StatusBadGateway,
		errorType:  "llm_unavailable",
		message:    "translation backend unavailable",
	}
}

func classifyBackendStatus(status int, body []byte) error {
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(status)
	}
	errorType := "llm_backend_error"
	statusCode := http.StatusBadGateway
	if status >= 400 && status < 500 {
		statusCode = status
		errorType = "unsupported_language_pair"
	}
	return &backendError{
		statusCode: statusCode,
		errorType:  errorType,
		message:    fmt.Sprintf("translation backend rejected request: %s", message),
	}
}

type supportedPair struct {
	src string
	tgt string
}

type supportedPairs map[string]supportedPair

var languageAliases = map[string]string{
	"deu": "de",
	"ger": "de",
	"eng": "en",
	"fra": "fr",
	"fre": "fr",
	"spa": "es",
	"ita": "it",
}

func parseSupportedPairs(raw string) supportedPairs {
	pairs := supportedPairs{}
	for _, spec := range strings.Split(raw, ",") {
		spec = strings.ToLower(strings.TrimSpace(spec))
		if spec == "" {
			continue
		}
		src, tgt, ok := strings.Cut(spec, "-")
		if !ok || src == "" || tgt == "" {
			continue
		}
		pairs[pairKey(src, tgt)] = supportedPair{src: src, tgt: tgt}
	}
	return pairs
}

func normalizeLangCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if alias, ok := languageAliases[value]; ok {
		return alias
	}
	return value
}

func (p supportedPairs) allows(src, tgt string) bool {
	if len(p) == 0 {
		return true
	}
	_, ok := p[pairKey(src, tgt)]
	return ok
}

func (p supportedPairs) list() []languagePairResp {
	out := make([]languagePairResp, 0, len(p))
	for _, pair := range p {
		out = append(out, languagePairResp{SrcLang: pair.src, TgtLang: pair.tgt})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SrcLang == out[j].SrcLang {
			return out[i].TgtLang < out[j].TgtLang
		}
		return out[i].SrcLang < out[j].SrcLang
	})
	return out
}

func pairKey(src, tgt string) string {
	return strings.ToLower(strings.TrimSpace(src)) + "-" + strings.ToLower(strings.TrimSpace(tgt))
}

// validate checks the bounds the PDF + ARCHITECTURE.md call out: non-empty
// text, sane length cap, BCP-47-ish lang codes.
func validate(in translateReq, pairs supportedPairs) error {
	switch {
	case in.Text == "":
		return errors.New("text is required")
	case len(in.Text) > 5000:
		return errors.New("text too long (max 5000)")
	case len(in.SrcLang) < 2 || len(in.SrcLang) > 5:
		return errors.New("src_lang must be 2–5 chars")
	case len(in.TgtLang) < 2 || len(in.TgtLang) > 5:
		return errors.New("tgt_lang must be 2–5 chars")
	case strings.TrimSpace(in.Src) != "" && len(in.Src) > 64:
		return errors.New("src must be at most 64 chars")
	case !pairs.allows(in.SrcLang, in.TgtLang):
		return fmt.Errorf("language pair %s->%s not supported", in.SrcLang, in.TgtLang)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Source, X-Request-ID, X-User-ID")
		w.Header().Set("Access-Control-Max-Age", "86400")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type eventPublisher interface {
	Publish(ctx context.Context, event translationEvent) error
	Close() error
}

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, translationEvent) error { return nil }
func (noopPublisher) Close() error                                    { return nil }

type kafkaPublisher struct {
	eventsWriter *kafka.Writer
	errorsWriter *kafka.Writer
	timeout      time.Duration
}

func newEventPublisher(log *slog.Logger) (eventPublisher, error) {
	if !envBool("KAFKA_ENABLED", false) {
		log.Info("kafka disabled")
		return noopPublisher{}, nil
	}
	brokers := splitCSV(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		return nil, errors.New("KAFKA_ENABLED=true but KAFKA_BROKERS is empty")
	}
	eventsTopic := cmp.Or(os.Getenv("KAFKA_TRANSLATION_EVENTS_TOPIC"), "translation-events")
	errorsTopic := cmp.Or(os.Getenv("KAFKA_TRANSLATION_ERRORS_TOPIC"), "translation-errors")
	timeout := envDuration("KAFKA_WRITE_TIMEOUT", 2*time.Second)
	log.Info("kafka enabled", "brokers", strings.Join(brokers, ","), "events_topic", eventsTopic, "errors_topic", errorsTopic)
	return &kafkaPublisher{
		eventsWriter: newKafkaWriter(brokers, eventsTopic, timeout),
		errorsWriter: newKafkaWriter(brokers, errorsTopic, timeout),
		timeout:      timeout,
	}, nil
}

func newKafkaWriter(brokers []string, topic string, timeout time.Duration) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: timeout,
		ReadTimeout:  timeout,
		RequiredAcks: kafka.RequireOne,
	}
}

func (p *kafkaPublisher) Publish(ctx context.Context, event translationEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	writer := p.eventsWriter
	if event.Status != "success" {
		writer = p.errorsWriter
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.ReqID),
		Value: payload,
		Time:  time.Now().UTC(),
	})
}

func (p *kafkaPublisher) Close() error {
	errEvents := p.eventsWriter.Close()
	errErrors := p.errorsWriter.Close()
	return errors.Join(errEvents, errErrors)
}

func envBool(name string, fallback bool) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envDuration(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return d
}

func splitCSV(raw string) []string {
	var out []string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

type appConfig struct {
	defaultSource  string
	supportedPairs supportedPairs
	demoUserID     string
}

type metricsRecorder struct {
	mu       sync.Mutex
	requests map[metricKey]int64
	latency  map[metricKey]latencyStats
}

type metricKey struct {
	Status    string
	SrcLang   string
	TgtLang   string
	ErrorType string
}

type latencyStats struct {
	Count int64
	SumMs float64
}

func newMetricsRecorder() *metricsRecorder {
	return &metricsRecorder{
		requests: map[metricKey]int64{},
		latency:  map[metricKey]latencyStats{},
	}
}

func (m *metricsRecorder) Record(event translationEvent) {
	if m == nil {
		return
	}
	key := metricKey{
		Status:    event.Status,
		SrcLang:   event.SrcLang,
		TgtLang:   event.TgtLang,
		ErrorType: event.ErrorType,
	}
	if key.ErrorType == "" {
		key.ErrorType = "none"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[key]++
	stats := m.latency[key]
	stats.Count++
	stats.SumMs += float64(event.LatencyMsTotal)
	m.latency[key] = stats
}

func (m *metricsRecorder) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "GET only"})
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		m.mu.Lock()
		defer m.mu.Unlock()

		fmt.Fprintln(w, "# HELP translation_requests_total Number of translation requests by status and language pair.")
		fmt.Fprintln(w, "# TYPE translation_requests_total counter")
		for key, value := range m.requests {
			fmt.Fprintf(w, "translation_requests_total{%s} %d\n", metricLabels(key), value)
		}

		fmt.Fprintln(w, "# HELP translation_request_latency_ms_total Total request latency in milliseconds.")
		fmt.Fprintln(w, "# TYPE translation_request_latency_ms_total counter")
		for key, stats := range m.latency {
			fmt.Fprintf(w, "translation_request_latency_ms_total{%s} %.0f\n", metricLabels(key), stats.SumMs)
		}

		fmt.Fprintln(w, "# HELP translation_request_latency_ms_count Count of latency observations.")
		fmt.Fprintln(w, "# TYPE translation_request_latency_ms_count counter")
		for key, stats := range m.latency {
			fmt.Fprintf(w, "translation_request_latency_ms_count{%s} %d\n", metricLabels(key), stats.Count)
		}
	}
}

func metricLabels(key metricKey) string {
	return strings.Join([]string{
		`status="` + prometheusEscape(key.Status) + `"`,
		`src_lang="` + prometheusEscape(key.SrcLang) + `"`,
		`tgt_lang="` + prometheusEscape(key.TgtLang) + `"`,
		`language_pair="` + prometheusEscape(key.SrcLang+"-"+key.TgtLang) + `"`,
		`error_type="` + prometheusEscape(key.ErrorType) + `"`,
	}, ",")
}

func prometheusEscape(value string) string {
	return strings.Trim(strconv.Quote(value), `"`)
}

func handleTranslate(b backend, log *slog.Logger, pub eventPublisher, cfg appConfig, metrics *metricsRecorder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := requestID(r)
		start := time.Now()
		source := sourceFromRequest(r, "", cfg.defaultSource)
		userHash := userHashFromRequest(r, cfg.demoUserID)

		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "POST only", ReqID: reqID})
			return
		}
		var in translateReq
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			event := newEvent(reqID, source, userHash, in, "error", "", start, 0, "invalid_json")
			metrics.Record(event)
			publishAsync(pub, log, event)
			writeJSON(w, http.StatusBadRequest, errorResp{Error: "invalid JSON", ReqID: reqID})
			return
		}
		in.SrcLang = normalizeLangCode(in.SrcLang)
		in.TgtLang = normalizeLangCode(in.TgtLang)
		source = sourceFromRequest(r, in.Src, cfg.defaultSource)
		in.Src = source
		if err := validate(in, cfg.supportedPairs); err != nil {
			event := newEvent(reqID, source, userHash, in, "error", "", start, 0, "validation_error")
			metrics.Record(event)
			publishAsync(pub, log, event)
			writeJSON(w, http.StatusBadRequest, errorResp{Error: err.Error(), ReqID: reqID})
			return
		}
		translateStart := time.Now()
		out, err := b.Translate(r.Context(), llmTranslateReq{
			Text:    in.Text,
			SrcLang: in.SrcLang,
			TgtLang: in.TgtLang,
		})
		translateLatency := time.Since(translateStart).Milliseconds()
		if err != nil {
			statusCode, errorType := statusAndType(err)
			event := newEvent(reqID, source, userHash, in, "error", "", start, translateLatency, errorType)
			metrics.Record(event)
			publishAsync(pub, log, event)
			log.Error("translate failed",
				"req_id", reqID,
				"src", source,
				"src_lang", in.SrcLang,
				"tgt_lang", in.TgtLang,
				"error_type", errorType,
				"err", err,
			)
			writeJSON(w, statusCode, errorResp{Error: err.Error(), ReqID: reqID})
			return
		}
		out.LatencyMs = time.Since(start).Milliseconds()
		out.ReqID = reqID
		event := newEvent(reqID, source, userHash, in, "success", out.Model, start, translateLatency, "")
		metrics.Record(event)
		publishAsync(pub, log, event)
		log.Info("translate succeeded",
			"req_id", reqID,
			"src", source,
			"src_lang", in.SrcLang,
			"tgt_lang", in.TgtLang,
			"model", out.Model,
			"latency_ms_total", out.LatencyMs,
			"latency_ms_translate", translateLatency,
		)
		writeJSON(w, http.StatusOK, out)
	}
}

func handleLanguages(cfg appConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "GET only"})
			return
		}
		writeJSON(w, http.StatusOK, languagesResp{Pairs: cfg.supportedPairs.list()})
	}
}

func handleLoadedPairs(llmURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, errorResp{Error: "GET only"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		statusURL := llmURL + "/model-status"
		if r.URL.RawQuery != "" {
			statusURL += "?" + r.URL.RawQuery
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, errorResp{Error: "failed to create upstream request"})
			return
		}
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, errorResp{Error: "llm model status unavailable"})
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			writeJSON(w, http.StatusBadGateway, errorResp{Error: strings.TrimSpace(string(body))})
			return
		}
		var out loadedPairsResp
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			writeJSON(w, http.StatusBadGateway, errorResp{Error: "invalid llm model status"})
			return
		}
		writeJSON(w, http.StatusOK, out)
	}
}

func statusAndType(err error) (int, string) {
	var be *backendError
	if errors.As(err, &be) {
		return be.statusCode, be.errorType
	}
	return http.StatusBadGateway, "llm_backend_error"
}

func newEvent(reqID, source, userHash string, in translateReq, status, model string, start time.Time, translateLatency int64, errorType string) translationEvent {
	return translationEvent{
		ReqID:              reqID,
		Src:                source,
		UserIDHashed:       userHash,
		SrcLang:            in.SrcLang,
		TgtLang:            in.TgtLang,
		CharCount:          utf8.RuneCountInString(in.Text),
		Model:              model,
		Status:             status,
		LatencyMsTotal:     time.Since(start).Milliseconds(),
		LatencyMsTranslate: translateLatency,
		EventTs:            time.Now().UTC().Format(time.RFC3339Nano),
		ErrorType:          errorType,
	}
}

func publishAsync(pub eventPublisher, log *slog.Logger, event translationEvent) {
	go func() {
		if err := pub.Publish(context.Background(), event); err != nil {
			log.Warn("kafka publish failed", "req_id", event.ReqID, "status", event.Status, "err", err)
		}
	}()
}

func requestID(r *http.Request) string {
	if fromHeader := cleanToken(r.Header.Get("X-Request-ID"), 128); fromHeader != "" {
		return fromHeader
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func sourceFromRequest(r *http.Request, bodySource, fallback string) string {
	if source := cleanToken(bodySource, 64); source != "" {
		return source
	}
	if source := cleanToken(r.Header.Get("X-Source"), 64); source != "" {
		return source
	}
	if source := cleanToken(r.Header.Get("X-Client-Source"), 64); source != "" {
		return source
	}
	return fallback
}

func userHashFromRequest(r *http.Request, fallback string) string {
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if userID == "" {
		userID = fallback
	}
	sum := sha256.Sum256([]byte(userID))
	return hex.EncodeToString(sum[:])
}

func cleanToken(value string, maxLen int) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxLen {
		return ""
	}
	for _, r := range value {
		if r < 33 || r == 127 {
			return ""
		}
	}
	return value
}

func setupTracing(ctx context.Context, log *slog.Logger) func(context.Context) error {
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		return func(context.Context) error { return nil }
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		log.Warn("otel exporter disabled", "endpoint", endpoint, "err", err)
		return func(context.Context) error { return nil }
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("translation-api"),
		)),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	log.Info("otel tracing enabled", "endpoint", endpoint)
	return provider.Shutdown
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	shutdownTracing := setupTracing(context.Background(), log)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			log.Warn("otel shutdown failed", "err", err)
		}
	}()
	addr := ":" + cmp.Or(os.Getenv("PORT"), "8000")
	url := os.Getenv("TRANSLATION_LLM_URL")
	if url == "" {
		log.Error("TRANSLATION_LLM_URL not found; exiting")
		os.Exit(1)
	}
	backendTimeout := envDuration("TRANSLATION_BACKEND_TIMEOUT", 120*time.Second)
	backend := &httpBackend{url: url, client: &http.Client{Timeout: backendTimeout}}
	publisher, err := newEventPublisher(log)
	if err != nil {
		log.Error("kafka config invalid", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Warn("kafka close failed", "err", err)
		}
	}()
	cfg := appConfig{
		defaultSource:  cmp.Or(os.Getenv("DEFAULT_SRC"), "local"),
		supportedPairs: parseSupportedPairs(os.Getenv("SUPPORTED_PAIRS")),
		demoUserID:     cmp.Or(os.Getenv("DEMO_USER_ID"), "demo-user"),
	}
	metrics := newMetricsRecorder()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ready"}) })
	mux.HandleFunc("/metrics", metrics.Handler())
	mux.HandleFunc("/languages", handleLanguages(cfg))
	mux.HandleFunc("/loaded-pairs", handleLoadedPairs(url))
	mux.HandleFunc("/translate", handleTranslate(backend, log, publisher, cfg, metrics))

	srv := &http.Server{
		Addr:              addr,
		Handler:           otelhttp.NewHandler(withCORS(mux), "translation-api"),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown — k8s sends SIGTERM, we drain in-flight requests.
	go func() {
		log.Info("listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
			os.Exit(1)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

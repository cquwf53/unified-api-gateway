// Package server implements the gateway data plane and its local management
// API. The handlers operate on immutable config snapshots, so an activation
// can never change the profile of an in-flight request.
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/adapter"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/route"
	"unified-api-gateway/internal/store"
	"unified-api-gateway/internal/telemetry"
	"unified-api-gateway/internal/transform"
	"unified-api-gateway/web"
)

const (
	defaultMaxBody    int64 = 8 << 20
	defaultAdminToken       = ""
)

// gatewayState bundles everything derived from one configuration snapshot.
// Publishing a single pointer makes hot activation atomic: a request can
// never observe the config of one version with the pipelines or policy
// runtimes of another.
type gatewayState struct {
	snapshot  *config.ConfigSnapshot
	runtimes  map[string]*profileRuntime
	pipelines map[string]*compiledPipeline
	policy    policySet
}

// allowsAnonymousOverride reports whether open-mode X-Gateway-Profile
// requests may select a profile without client authentication. All planes
// (HTTP, WebSocket, gRPC) must consult the same pinned state.
func (st *gatewayState) allowsAnonymousOverride() bool {
	return st == nil || st.policy.allowAnonymousProfileOverride
}

type Server struct {
	state      atomic.Pointer[gatewayState]
	registry   *adapter.Registry
	client     *http.Client
	metrics    *telemetry.Metrics
	store      *store.Store
	configPath string
	started    time.Time
	sequence   atomic.Uint64

	clientMu     sync.Mutex
	clientsByKey map[clientKey]*http.Client

	breakerMu sync.Mutex
	breakers  map[string]*breaker
	shadowSem chan struct{}
	auditCh   chan store.AuditEvent
	auditWG   sync.WaitGroup
	wsOnce    sync.Once
	wsSem     chan struct{}
}

type Options struct {
	Snapshot   *config.ConfigSnapshot
	Store      *store.Store
	ConfigPath string
	HTTPClient *http.Client
}

func New(options Options) (*Server, error) {
	if options.Snapshot == nil || options.Snapshot.Config == nil {
		return nil, errors.New("server: config snapshot is required")
	}
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 90 * time.Second}
	}
	s := &Server{
		registry:   adapter.NewRegistry(),
		client:     client,
		metrics:    telemetry.New(100),
		store:      options.Store,
		configPath: options.ConfigPath,
		started:    time.Now().UTC(),
		shadowSem:  make(chan struct{}, maxInflightShadow),
		auditCh:    make(chan store.AuditEvent, 256),
	}
	if options.Store != nil {
		s.auditWG.Add(1)
		go s.drainAuditEvents()
	}
	s.publishState(options.Snapshot)
	return s, nil
}

// publishState derives runtimes and pipelines for a snapshot and swaps the
// whole state in one atomic store.
func (s *Server) publishState(snapshot *config.ConfigSnapshot) {
	cfg := snapshot.Config
	state := &gatewayState{
		snapshot:  snapshot,
		runtimes:  s.buildRuntimes(cfg),
		pipelines: buildPipelines(cfg),
		policy:    decodePolicySet(cfg),
	}
	s.warnInsecureUpstreams(cfg)
	s.state.Store(state)
}

// clientKey identifies a derived HTTP client by the properties that affect
// its behaviour; derived clients are cached so connection pools are shared.
type clientKey struct {
	insecure bool
	timeout  time.Duration
	stream   bool
}

// upstreamClient returns a cached client for one upstream. Streaming clients
// have no total timeout (long completions must not be cut mid-body) but bound
// the time until response headers via the transport. Insecure upstreams get a
// transport that skips TLS verification; the configuration loader warns about
// every such upstream at activation time.
func (s *Server) upstreamClient(upstream config.Upstream, stream bool) *http.Client {
	timeout := upstream.EffectiveTimeout()
	if stream {
		timeout = 0
	}
	key := clientKey{insecure: upstream.Insecure, timeout: timeout, stream: stream}
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	if s.clientsByKey == nil {
		s.clientsByKey = map[clientKey]*http.Client{}
	}
	if cached, ok := s.clientsByKey[key]; ok {
		return cached
	}
	client := &http.Client{Timeout: timeout}
	transport := s.clonableTransport()
	switch {
	case transport == nil:
		if !stream {
			// A custom, non-clonable RoundTripper was injected: reuse the
			// caller's client verbatim and only override the timeout.
			copyOf := *s.client
			copyOf.Timeout = timeout
			client = &copyOf
		}
	case stream || upstream.Insecure:
		if upstream.Insecure {
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit opt-in per upstream
		}
		if stream {
			transport.ResponseHeaderTimeout = streamResponseHeaderTimeout
		}
		client.Transport = transport
	}
	s.clientsByKey[key] = client
	return client
}

func (s *Server) clonableTransport() *http.Transport {
	if t, ok := s.client.Transport.(*http.Transport); ok {
		return t.Clone()
	}
	if s.client.Transport == nil {
		if t, ok := http.DefaultTransport.(*http.Transport); ok {
			return t.Clone()
		}
	}
	return nil
}

func (s *Server) Snapshot() *config.ConfigSnapshot {
	if s == nil {
		return nil
	}
	if state := s.state.Load(); state != nil {
		return state.snapshot
	}
	return nil
}

func (s *Server) Activate(snapshot *config.ConfigSnapshot) error {
	if snapshot == nil || snapshot.Config == nil {
		return errors.New("server: cannot activate an empty snapshot")
	}
	if err := snapshot.Config.Validate(); err != nil {
		return err
	}
	s.publishState(snapshot.Clone())
	if s.store != nil {
		_ = s.store.AddAudit(context.Background(), store.AuditEvent{
			Action:    "config.activate",
			Actor:     "admin",
			VersionID: snapshot.ID,
			Details:   map[string]any{"source": snapshot.Source},
		})
	}
	return nil
}

// warnInsecureUpstreams logs every upstream that disabled TLS verification;
// it runs on each published state.
func (s *Server) warnInsecureUpstreams(cfg *config.Config) {
	if cfg == nil {
		return
	}
	for name, upstream := range cfg.Upstreams {
		if upstream.Insecure {
			slog.Warn("upstream TLS verification is disabled", "upstream", name, "base_url", upstream.EffectiveURL())
		}
	}
}

func (s *Server) Metrics() telemetry.Snapshot {
	return s.metrics.Snapshot()
}

func (s *Server) ServeDataHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch r.URL.Path {
	case "/", "/index.html":
		// The admin console lives on the management plane only; serving it
		// here would invite calls to /api/* that the data plane cannot answer.
		s.writeJSON(w, http.StatusOK, map[string]any{"service": "unified-api-gateway", "plane": "data", "admin_console": "served on the admin listener (default http://127.0.0.1:8781/)", "health": "/healthz"})
		return
	case "/health", "/healthz", "/livez":
		s.writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "started_at": s.started})
		return
	case "/readyz":
		if s.Snapshot() == nil {
			s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
			return
		}
		if s.store != nil {
			if err := s.store.Ping(r.Context()); err != nil {
				s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "store": err.Error()})
				return
			}
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "config": s.Snapshot().ID})
		return
	case "/metrics":
		if !s.Snapshot().Config.Observability.MetricsEnabled() {
			s.writeError(w, http.StatusNotFound, "observability", "metrics_disabled", "metrics endpoints are disabled by configuration", "")
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, s.metrics.Prometheus())
		return
	case "/v1/models":
		// The model list enumerates every profile (the values X-Gateway-Profile
		// accepts), so it must sit behind the same client authentication as
		// the proxy path.
		if snapshot := s.Snapshot(); snapshot != nil && snapshot.Config != nil {
			if requiresClientAuth(snapshot.Config) && s.callClientKey(snapshot.Config, r) == "" {
				s.writeError(w, http.StatusUnauthorized, "auth", "client_auth_required", "a valid X-API-Key or Bearer client key is required", "")
				return
			}
		}
		s.handleModels(w, r)
		return
	}
	s.handleProxy(w, r)
}

func (s *Server) ServeAdminHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" || r.URL.Path == "/livez" {
		s.writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "started_at": s.started})
		return
	}
	if r.URL.Path == "/readyz" {
		if s.Snapshot() == nil {
			s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "config": s.Snapshot().ID})
		return
	}
	if !s.authorizeAdmin(r) {
		s.writeError(w, http.StatusUnauthorized, "auth", "admin_auth_required", "administrator authentication is required", "")
		return
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.URL.Path == "/" || r.URL.Path == "/index.html" || strings.HasSuffix(r.URL.Path, ".css") || strings.HasSuffix(r.URL.Path, ".js") || strings.HasSuffix(r.URL.Path, ".ico") {
		s.writeStatic(w, r)
		return
	}
	switch r.URL.Path {
	case "/api/status", "/admin/v1/status":
		s.writeJSON(w, http.StatusOK, s.statusPayload())
	case "/api/metrics", "/admin/v1/metrics":
		if !s.Snapshot().Config.Observability.MetricsEnabled() {
			s.writeError(w, http.StatusNotFound, "observability", "metrics_disabled", "metrics endpoints are disabled by configuration", "")
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, s.metrics.Prometheus())
	case "/api/config", "/admin/v1/config":
		s.writeJSON(w, http.StatusOK, redactConfig(s.Snapshot().Config))
	case "/api/profiles", "/admin/v1/profiles":
		s.writeJSON(w, http.StatusOK, map[string]any{"profiles": s.profilePayload()})
	case "/api/routes", "/admin/v1/routes":
		s.writeJSON(w, http.StatusOK, map[string]any{"routes": s.Snapshot().Config.Routes})
	case "/api/versions", "/admin/v1/versions":
		s.handleVersions(w, r)
	case "/api/audit", "/admin/v1/audit":
		s.handleAudit(w, r)
	case "/api/recordings", "/admin/v1/recordings":
		s.handleRecordings(w, r)
	case "/api/preview", "/api/config/preview", "/admin/v1/preview", "/admin/v1/config/preview":
		s.handlePreview(w, r)
	case "/api/config/validate", "/admin/v1/config/validate":
		s.handleValidate(w, r)
	case "/api/activate", "/admin/v1/config/activate":
		s.handleActivate(w, r)
	case "/api/switch", "/admin/v1/profile/switch":
		s.handleQuickSwitch(w, r)
	case "/api/rollback", "/admin/v1/config/rollback":
		s.handleRollback(w, r)
	case "/api/explain", "/admin/v1/routes/explain":
		s.handleExplain(w, r)
	default:
		s.writeError(w, http.StatusNotFound, "admin", "not_found", "management endpoint not found", "")
	}
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	finish := s.metrics.Begin()
	defer finish()
	started := time.Now()
	var selectedRoute *config.Route
	var profile *config.Profile
	upstreamName := ""
	observeFailure := func(status int, stage, code, message string) {
		s.writeError(w, status, stage, code, message, "")
		s.observe("", started, r, selectedRoute, profile, upstreamName, status, stage, code)
	}
	// Pin every derived view (config, runtimes, pipelines, policy) for the
	// whole request: a concurrent activation must not mix versions mid-flight
	// or strip policy enforcement from an in-flight request.
	pinned := s.state.Load()
	snapshot := pinned.snapshot
	if snapshot == nil || snapshot.Config == nil {
		observeFailure(http.StatusServiceUnavailable, "config", "not_ready", "no active configuration")
		return
	}
	cfg := snapshot.Config
	requestID := s.inboundRequestID(cfg, r)
	// fail answers and observes in one step so every early return keeps
	// requests_total == requests_ok + requests_failed.
	fail := func(status int, stage, code, message string) {
		s.writeError(w, status, stage, code, message, requestID)
		s.observe(requestID, started, r, selectedRoute, profile, upstreamName, status, stage, code)
	}
	w.Header().Set("X-Request-Id", requestID)
	if custom := strings.TrimSpace(cfg.Observability.RequestIDHeader); custom != "" && !strings.EqualFold(custom, "X-Request-Id") {
		w.Header().Set(custom, requestID)
	}
	if isWebSocketUpgrade(r) {
		s.handleWebSocket(w, r, snapshot, requestID)
		return
	}
	maxBody := cfg.Limits.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBody
	}
	if r.ContentLength > maxBody {
		fail(http.StatusRequestEntityTooLarge, "ingress", "body_too_large", "request body exceeds configured limit")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		fail(http.StatusBadRequest, "ingress", "body_read_failed", err.Error())
		return
	}
	if int64(len(body)) > maxBody {
		s.writeError(w, http.StatusRequestEntityTooLarge, "ingress", "body_too_large", "request body exceeds configured limit", requestID)
		return
	}

	clientID, tenant := s.authenticateClient(cfg, r)
	if requiresClientAuth(cfg) && clientID == "" {
		fail(http.StatusUnauthorized, "auth", "client_auth_required", "a valid X-API-Key or Bearer client key is required")
		return
	}
	match := route.FromHTTPRequest(r)
	match.ClientID = clientID
	match.Client = clientID
	match.Tenant = tenant
	match.Profile = strings.TrimSpace(r.Header.Get("X-Gateway-Profile"))
	if match.Profile != "" && !requiresClientAuth(cfg) && clientID == "" {
		if !pinned.allowsAnonymousOverride() {
			fail(http.StatusForbidden, "auth", "profile_override_denied", "anonymous profile overrides are disabled by policy")
			return
		}
		match.ClientID = "__anonymous__"
		match.Client = "__anonymous__"
	}
	match.Model = requestModel(body)
	profile, selectedRoute, err = route.SelectProfile(cfg, match)
	if err != nil {
		status := http.StatusNotFound
		code := "route_not_found"
		if errors.Is(err, route.ErrProfileForbidden) {
			status, code = http.StatusForbidden, "profile_override_denied"
		} else if errors.Is(err, route.ErrProfileNotFound) {
			status, code = http.StatusBadRequest, "profile_not_found"
		}
		fail(status, "route", code, err.Error())
		return
	}
	if profile == nil {
		fail(http.StatusNotFound, "route", "profile_not_found", "no profile selected")
		return
	}
	if !methodAllowed(profile.AllowedMethods, r.Method) {
		allowed := make([]string, 0, len(profile.AllowedMethods))
		for _, method := range profile.AllowedMethods {
			allowed = append(allowed, strings.ToUpper(strings.TrimSpace(method)))
		}
		w.Header().Set("Allow", strings.Join(allowed, ", "))
		fail(http.StatusMethodNotAllowed, "route", "method_not_allowed", "profile "+profile.Name+" does not allow "+r.Method)
		return
	}
	if profile.MaxBodyBytes > 0 && int64(len(body)) > profile.MaxBodyBytes {
		fail(http.StatusRequestEntityTooLarge, "ingress", "body_too_large", "request body exceeds the profile limit")
		return
	}

	upstreamName = profile.Upstream
	upstream, hasUpstream := cfg.Upstreams[upstreamName]
	if !hasUpstream && strings.ToLower(profile.TrafficMode) != "mock" && strings.ToLower(profile.TrafficMode) != "dry-run" {
		fail(http.StatusBadGateway, "upstream", "upstream_not_found", "selected upstream is not configured")
		return
	}
	mode := normalizeMode(profile.TrafficMode)
	if mode == "" {
		mode = "live"
	}
	willCallUpstream := mode == "live" || mode == "shadow" || mode == "record"
	rt := pinned.runtimes[profile.Name]
	release, rejection := rt.beginPolicy(r.Context(), r.Method, r.Header, willCallUpstream)
	if rejection != nil {
		if rejection.retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(rejection.retryAfter.Seconds()+0.999)))
		}
		fail(rejection.status, "policy", rejection.code, "request refused by traffic policy: "+rejection.code)
		return
	}
	defer release()

	in := transform.Envelope{
		Method:  r.Method,
		Scheme:  requestScheme(r),
		Host:    r.Host,
		Path:    r.URL.Path,
		Query:   r.URL.Query(),
		Headers: r.Header.Clone(),
		Body:    body,
		Metadata: map[string]any{
			"request_id":    requestID,
			"client_id":     clientID,
			"tenant":        tenant,
			"client_stream": requestStream(body) || strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream"),
		},
	}
	prepared, err := s.prepareRequest(r.Context(), pinned, cfg, *profile, in)
	if err != nil {
		fail(http.StatusUnprocessableEntity, "transform", "request_transform_failed", safeError(err))
		return
	}

	if mode == "dry-run" {
		s.writeDryRunResponse(w, requestID, profile, prepared)
		s.observe(requestID, started, r, selectedRoute, profile, upstreamName, http.StatusOK, "", "")
		return
	}
	if mode == "mock" {
		response := transform.Envelope{StatusCode: http.StatusOK, Headers: make(http.Header), Body: []byte(`{}`), ContentType: "application/json"}
		response, err = s.applyResponseTransforms(r.Context(), pinned, cfg, *profile, response)
		if err != nil {
			fail(http.StatusBadGateway, "transform", "mock_response_failed", safeError(err))
			return
		}
		s.writeEnvelope(w, response)
		s.observe(requestID, started, r, selectedRoute, profile, upstreamName, response.StatusCode, "", "")
		return
	}
	if mode == "replay" {
		if s.store == nil {
			fail(http.StatusNotImplemented, "replay", "replay_not_configured", "replay storage is not configured")
			return
		}
		clientID, tenant := metadataStrings(prepared)
		recording, replayErr := s.store.GetRecording(r.Context(), recordingKey(profile.Name, prepared), clientID, tenant)
		if replayErr == nil {
			response := transform.Envelope{StatusCode: recording.StatusCode, Headers: headersFromValues(recording.Headers), Body: recording.Body, ContentType: recording.ContentType}
			response.Headers = response.Headers.Clone()
			response.Headers.Set("X-Gateway-Replay", "exact")
			s.writeEnvelope(w, response)
			s.observe(requestID, started, r, selectedRoute, profile, upstreamName, recording.StatusCode, "", "")
			return
		}
		// Exact hashes rarely match twice for chat traffic, so fall back to
		// the most recent recording of the same profile, method and path and
		// label the response so clients can tell.
		if recording, fuzzyErr := s.store.LatestRecordingForProfile(r.Context(), profile.Name, prepared.Method, prepared.Path, clientID, tenant); fuzzyErr == nil {
			response := transform.Envelope{StatusCode: recording.StatusCode, Headers: headersFromValues(recording.Headers), Body: recording.Body, ContentType: recording.ContentType}
			response.Headers = response.Headers.Clone()
			response.Headers.Set("X-Gateway-Replay", "approximate")
			s.writeEnvelope(w, response)
			s.observe(requestID, started, r, selectedRoute, profile, upstreamName, recording.StatusCode, "", "")
			return
		}
		fail(http.StatusNotFound, "replay", "recording_not_found", "no recording matches this request")
		return
	}
	if mode == "record" && !cfg.Observability.RecordBody {
		fail(http.StatusConflict, "record", "record_body_disabled", "record mode requires observability.record_body: true")
		return
	}
	if mode == "live" && wantsStreamFromEnvelope(prepared) && canDirectStream(*profile) {
		status, headersSent, streamErr := s.streamUpstream(r.Context(), cfg, upstream, *profile, prepared, requestID, w)
		recordStreamResult(rt, headersSent, status, streamErr)
		if streamErr != nil && !headersSent {
			s.writeError(w, upstreamStatus(streamErr), "upstream", upstreamCode(streamErr), safeError(streamErr), requestID)
		}
		s.observe(requestID, started, r, selectedRoute, profile, upstreamName, status, streamErrorStage(headersSent, streamErr), streamErrorCode(streamErr))
		return
	}
	if mode == "live" && wantsStreamFromEnvelope(prepared) && profile.IsStreamSafe() && len(profile.AllResponseTransforms()) == 0 {
		// Semantic adapters translate frame by frame so the client receives a
		// live stream instead of a buffered burst. An explicit
		// stream_safe: false opts out (the response is then buffered and
		// converted as a whole).
		status, headersSent, streamErr := s.streamTranslatedUpstream(r.Context(), pinned, cfg, upstream, *profile, prepared, requestID, w)
		recordStreamResult(rt, headersSent, status, streamErr)
		if streamErr != nil && !headersSent {
			s.writeError(w, upstreamStatus(streamErr), "upstream", upstreamCode(streamErr), safeError(streamErr), requestID)
		}
		s.observe(requestID, started, r, selectedRoute, profile, upstreamName, status, streamErrorStage(headersSent, streamErr), streamErrorCode(streamErr))
		return
	}

	upstreamEnvelope, err := s.executeUpstream(r.Context(), cfg, upstream, *profile, prepared, requestID, rt, r.Method, r.Header)
	if err != nil {
		fail(upstreamStatus(err), "upstream", upstreamCode(err), safeError(err))
		return
	}
	upstreamEnvelope.Metadata = prepared.Metadata
	if mode == "shadow" {
		// Shadow is opt-in and never blocks the primary response. When a
		// shadow_upstream is configured the mirror goes there instead of
		// double-billing the primary provider.
		shadowUpstream := upstream
		if name := profile.ShadowUpstream; name != "" {
			if configured, ok := cfg.Upstreams[name]; ok {
				shadowUpstream = configured
			}
		}
		s.dispatchShadow(cfg, shadowUpstream, *profile, prepared, requestID)
	}
	response, err := s.prepareResponse(r.Context(), pinned, cfg, *profile, upstreamEnvelope)
	if err != nil {
		fail(http.StatusBadGateway, "transform", "response_transform_failed", safeError(err))
		return
	}
	if mode == "record" && s.store != nil {
		recordingClient, recordingTenant := metadataStrings(prepared)
		if saveErr := s.store.SaveRecording(r.Context(), store.Recording{Key: recordingKey(profile.Name, prepared), ProfileID: profile.Name, Method: prepared.Method, Path: prepared.Path, ClientID: recordingClient, Tenant: recordingTenant, StatusCode: response.StatusCode, ContentType: response.ContentType, Headers: redactHeadersWith(response.Headers, configuredRedactions(cfg)), Body: response.Body}); saveErr != nil {
			fail(http.StatusInternalServerError, "record", "record_save_failed", saveErr.Error())
			return
		}
	}
	s.writeEnvelope(w, response)
	s.observe(requestID, started, r, selectedRoute, profile, upstreamName, response.StatusCode, "", "")
}

func canDirectStream(profile config.Profile) bool {
	return strings.EqualFold(profile.Adapter, "transparent") && len(profile.AllResponseTransforms()) == 0
}

// methodAllowed enforces profile.allowed_methods when it is set; an empty
// list keeps the route matcher in charge.
func methodAllowed(allowed []string, method string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, candidate := range allowed {
		if strings.EqualFold(strings.TrimSpace(candidate), method) {
			return true
		}
	}
	return false
}

func streamErrorStage(headersSent bool, err error) string {
	if err == nil || headersSent {
		return ""
	}
	return "upstream"
}

// recordStreamResult feeds stream outcomes into the circuit breaker. A
// client-side write failure after headers were sent is not the upstream's
// fault, and an upstream 5xx status must count as a failure even though the
// bytes were forwarded without error.
func recordStreamResult(rt *profileRuntime, headersSent bool, status int, err error) {
	if rt == nil {
		return
	}
	if err == nil {
		rt.recordUpstreamResult(status < 500)
		return
	}
	if !headersSent || upstreamCode(err) != "client_stream_write_failed" {
		rt.recordUpstreamResult(false)
	}
}

func streamErrorCode(err error) string {
	if err == nil {
		return ""
	}
	return upstreamCode(err)
}

func (s *Server) prepareRequest(ctx context.Context, pinned *gatewayState, cfg *config.Config, profile config.Profile, in transform.Envelope) (transform.Envelope, error) {
	adapterName := profile.Adapter
	if adapterName == "" {
		adapterName = "transparent"
	}
	a, ok := s.registry.Get(adapterName)
	if !ok {
		return transform.Envelope{}, fmt.Errorf("unknown adapter %q", adapterName)
	}
	converted := adapter.Envelope{
		Method: in.Method, Header: in.Headers.Clone(), Body: append([]byte(nil), in.Body...),
		ContentType: in.ContentType, Stream: requestStream(in.Body), Protocol: profile.ClientContract,
	}
	if in.Path != "" {
		converted.URL, _ = url.Parse(in.Path)
	}
	converted, err := a.AdaptRequest(converted, profile)
	if err != nil {
		return transform.Envelope{}, err
	}
	in.Method, in.Body, in.Headers = converted.Method, converted.Body, converted.Header
	if converted.URL != nil && converted.URL.Path != "" {
		in.Path = converted.URL.Path
	}
	for _, compiled := range s.compiledRequestSteps(pinned, cfg, profile) {
		changed, err := compiled.Apply(ctx, in)
		if err != nil {
			return transform.Envelope{}, err
		}
		in = changed
	}
	return in, nil
}

func (s *Server) prepareResponse(ctx context.Context, pinned *gatewayState, cfg *config.Config, profile config.Profile, in transform.Envelope) (transform.Envelope, error) {
	response := in
	if response.StatusCode >= 400 {
		// An upstream error body is not a protocol response: adapting it
		// would fabricate an empty "successful" message and destroy the
		// upstream's diagnosis. Answer with the client contract's error
		// envelope (verbatim when the upstream already speaks it) and still
		// run the response transform chain so status_map/header rules keep
		// working on errors.
		return s.applyResponseTransforms(ctx, pinned, cfg, profile, errorEnvelope(profile.ClientContract, response))
	}
	a, ok := s.registry.Get(profile.Adapter)
	if !ok {
		return transform.Envelope{}, fmt.Errorf("unknown adapter %q", profile.Adapter)
	}
	if strings.Contains(strings.ToLower(response.ContentType), "text/event-stream") {
		events, err := adapter.ParseSSE(bytes.NewReader(response.Body))
		if err != nil {
			return transform.Envelope{}, fmt.Errorf("parse upstream SSE: %w", err)
		}
		translated, err := adapter.TranslateSSE(events, profile.UpstreamContract, profile.ClientContract)
		if err != nil {
			return transform.Envelope{}, err
		}
		if !wantsStreamFromEnvelope(in) {
			body, contentType, aggregateErr := adapter.AggregateSSE(translated, profile.ClientContract)
			if aggregateErr != nil {
				return transform.Envelope{}, aggregateErr
			}
			response.Body = body
			response.ContentType = contentType
			response.Headers = response.Headers.Clone()
			response.Headers.Set("Content-Type", contentType)
		} else {
			response.Body = adapter.EncodeSSE(translated)
			response.ContentType = "text/event-stream"
			response.Headers = response.Headers.Clone()
			response.Headers.Set("Content-Type", "text/event-stream")
		}
		// Translated SSE responses still run the compiled response chain
		// (header rules, status maps) before returning.
		for _, compiled := range s.compiledResponseSteps(pinned, cfg, profile) {
			changed, err := compiled.Apply(ctx, response)
			if err != nil {
				return transform.Envelope{}, err
			}
			response = changed
		}
		return response, nil
	}
	converted := adapter.Envelope{Method: http.MethodPost, Header: response.Headers.Clone(), Body: response.Body, StatusCode: response.StatusCode, ContentType: response.ContentType, Protocol: profile.UpstreamContract}
	converted, err := a.AdaptResponse(converted, profile)
	if err != nil {
		return transform.Envelope{}, err
	}
	response.Body, response.Headers, response.StatusCode, response.ContentType = converted.Body, converted.Header, converted.StatusCode, converted.ContentType
	for _, compiled := range s.compiledResponseSteps(pinned, cfg, profile) {
		changed, err := compiled.Apply(ctx, response)
		if err != nil {
			return transform.Envelope{}, err
		}
		response = changed
	}
	if wantsStreamFromEnvelope(in) && !strings.Contains(strings.ToLower(response.ContentType), "event-stream") {
		response.Headers = response.Headers.Clone()
		response.Headers.Set("Content-Type", "text/event-stream")
		response.ContentType = "text/event-stream"
		response.Body = adapter.EncodeSSE([]adapter.SSEEvent{{Data: string(response.Body)}, {Data: "[DONE]"}})
	}
	return response, nil
}

// upstreamErrorMessage extracts a human-readable message from common error
// shapes ({"error":{"message":...}} / {"message":...}) and falls back to the
// raw body so nothing the upstream said is lost.
func upstreamErrorMessage(body []byte) string {
	var shaped struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &shaped) == nil {
		if shaped.Error != nil && shaped.Error.Message != "" {
			return shaped.Error.Message
		}
		if shaped.Message != "" {
			return shaped.Message
		}
	}
	if len(body) > 2048 {
		return string(body[:2048]) + "…"
	}
	return string(body)
}

// errorEnvelope renders an upstream error in the client's protocol shape,
// preserving the original status code and message. When the upstream already
// speaks the client's error vocabulary the envelope is forwarded verbatim:
// rewriting it would collapse provider error codes (rate_limit_exceeded,
// insufficient_quota, ...) that retry middleware branches on.
func errorEnvelope(clientContract string, response transform.Envelope) transform.Envelope {
	contract := strings.ToLower(clientContract)
	if errorMatchesContract(contract, response.Body) {
		return response
	}
	contentType := "application/json"
	message := upstreamErrorMessage(response.Body)
	var payload []byte
	switch {
	case strings.Contains(contract, "anthropic"):
		payload, _ = json.Marshal(map[string]any{"type": "error", "error": map[string]any{"type": "upstream_error", "message": message}})
	case strings.Contains(contract, "openai"), strings.Contains(contract, "responses"):
		payload, _ = json.Marshal(map[string]any{"error": map[string]any{"message": message, "type": "upstream_error", "code": response.StatusCode}})
	default:
		// Generic clients receive the upstream bytes unchanged.
		payload = response.Body
		contentType = response.ContentType
	}
	headers := response.Headers.Clone()
	if contentType != "" {
		headers.Set("Content-Type", contentType)
	}
	return transform.Envelope{StatusCode: response.StatusCode, Headers: headers, Body: payload, ContentType: contentType}
}

// errorMatchesContract reports whether an upstream error body already uses
// the client contract's error shape and can be forwarded as-is.
func errorMatchesContract(contract string, body []byte) bool {
	var probe struct {
		Error json.RawMessage `json:"error"`
		Type  string          `json:"type"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return false
	}
	hasErrorObject := len(probe.Error) > 0 && probe.Error[0] == '{'
	switch {
	case strings.Contains(contract, "anthropic"):
		return probe.Type == "error" && hasErrorObject
	case strings.Contains(contract, "openai"), strings.Contains(contract, "responses"):
		// OpenAI errors are a bare {"error":{...}}; Anthropic errors also
		// carry a top-level type:"error" marker and must still be translated.
		return hasErrorObject && probe.Type != "error"
	default:
		return true
	}
}

func (s *Server) applyResponseTransforms(ctx context.Context, pinned *gatewayState, cfg *config.Config, profile config.Profile, response transform.Envelope) (transform.Envelope, error) {
	for _, compiled := range s.compiledResponseSteps(pinned, cfg, profile) {
		var err error
		response, err = compiled.Apply(ctx, response)
		if err != nil {
			return transform.Envelope{}, err
		}
	}
	return response, nil
}

func (s *Server) callUpstream(ctx context.Context, cfg *config.Config, upstream config.Upstream, profile config.Profile, in transform.Envelope, requestID string) (transform.Envelope, error) {
	request, err := buildUpstreamRequest(ctx, cfg, upstream, in, requestID)
	if err != nil {
		return transform.Envelope{}, upstreamError{status: http.StatusBadGateway, code: "build_upstream_request", err: err}
	}
	response, err := s.upstreamClient(upstream, false).Do(request)
	if err != nil {
		return transform.Envelope{}, upstreamError{status: http.StatusBadGateway, code: "upstream_request_failed", err: err}
	}
	defer response.Body.Close()
	return readUpstreamResponse(profile, response)
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	cfg := s.Snapshot().Config
	models := make([]map[string]any, 0, len(cfg.Profiles))
	for id, profile := range cfg.Profiles {
		models = append(models, map[string]any{"id": id, "object": "model", "owned_by": "gateway", "adapter": profile.Adapter})
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": models})
}

type PreviewRequest struct {
	Profile string              `json:"profile"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
	Body    json.RawMessage     `json:"body"`
}

func (s *Server) handlePreview(w http.ResponseWriter, r *http.Request) {
	var input PreviewRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&input); err != nil {
		s.writeError(w, http.StatusBadRequest, "preview", "invalid_json", err.Error(), "")
		return
	}
	snapshot := s.Snapshot()
	match := config.MatchRequest{Method: input.Method, Path: input.Path, Headers: input.Headers, Profile: input.Profile}
	var profile *config.Profile
	var selectedRoute *config.Route
	var err error
	if input.Profile != "" {
		if selected, ok := snapshot.Config.Profiles[input.Profile]; ok {
			selected.Name = input.Profile
			profile = &selected
		} else {
			err = fmt.Errorf("profile %q not found", input.Profile)
		}
	} else {
		profile, selectedRoute, err = route.SelectProfile(snapshot.Config, match)
	}
	if err != nil || profile == nil {
		s.writeError(w, http.StatusBadRequest, "route", "profile_selection_failed", safeError(err), "")
		return
	}
	if input.Method == "" {
		input.Method = http.MethodPost
	}
	in := transform.Envelope{Method: input.Method, Path: input.Path, Headers: headersFromValues(input.Headers), Body: append([]byte(nil), input.Body...), Query: url.Values{}}
	prepared, err := s.prepareRequest(r.Context(), s.state.Load(), snapshot.Config, *profile, in)
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "transform", "preview_failed", safeError(err), "")
		return
	}
	s.writeJSON(w, http.StatusOK, s.previewPayload("preview", profile, selectedRoute, profile.Upstream, prepared, "preview", configuredRedactions(snapshot.Config)))
}

func (s *Server) handleActivate(w http.ResponseWriter, r *http.Request) {
	var input struct {
		YAML        string `json:"yaml"`
		Path        string `json:"path"`
		Description string `json:"description"`
		Actor       string `json:"actor"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&input); err != nil {
		s.writeError(w, http.StatusBadRequest, "config", "invalid_json", err.Error(), "")
		return
	}
	data := []byte(input.YAML)
	source := "admin-api"
	if len(bytes.TrimSpace(data)) == 0 && input.Path != "" {
		var err error
		data, err = os.ReadFile(input.Path)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "config", "read_failed", err.Error(), "")
			return
		}
		source = input.Path
	}
	if len(bytes.TrimSpace(data)) == 0 && s.configPath != "" {
		var err error
		data, err = os.ReadFile(s.configPath)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "config", "read_failed", err.Error(), "")
			return
		}
		source = s.configPath
	}
	cfg, err := config.Parse(data)
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "config", "validation_failed", err.Error(), "")
		return
	}
	snapshot, err := config.NewSnapshot(cfg, source)
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "config", "snapshot_failed", err.Error(), "")
		return
	}
	if s.store != nil {
		if err := s.store.SaveVersion(r.Context(), store.Version{ID: snapshot.ID, CreatedBy: input.Actor, Active: true, SHA256: snapshot.Hash, Description: input.Description, YAML: data}); err != nil {
			s.writeError(w, http.StatusInternalServerError, "store", "save_version_failed", err.Error(), "")
			return
		}
		if err := s.store.Activate(r.Context(), snapshot.ID); err != nil {
			s.writeError(w, http.StatusInternalServerError, "store", "activate_version_failed", err.Error(), "")
			return
		}
	}
	if err := s.Activate(snapshot); err != nil {
		s.writeError(w, http.StatusInternalServerError, "config", "activation_failed", err.Error(), "")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"activated": true, "id": snapshot.ID, "hash": snapshot.Hash, "loaded_at": snapshot.LoadedAt})
}

func (s *Server) handleQuickSwitch(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Profile     string `json:"profile"`
		RouteID     string `json:"route_id"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&input); err != nil {
		s.writeError(w, http.StatusBadRequest, "config", "invalid_json", err.Error(), "")
		return
	}
	current := s.Snapshot()
	if current == nil || current.Config == nil {
		s.writeError(w, http.StatusServiceUnavailable, "config", "not_ready", "no active configuration", "")
		return
	}
	if input.Profile == "" {
		s.writeError(w, http.StatusBadRequest, "config", "profile_required", "profile is required", "")
		return
	}
	if _, ok := current.Config.Profiles[input.Profile]; !ok {
		s.writeError(w, http.StatusNotFound, "config", "profile_not_found", input.Profile, "")
		return
	}
	clone := current.Config.Clone()
	if input.RouteID == "" {
		clone.DefaultsProfile = input.Profile
	} else {
		found := false
		for i := range clone.Routes {
			id := clone.Routes[i].ID
			if id == "" {
				id = clone.Routes[i].Name
			}
			if id == input.RouteID {
				clone.Routes[i].Profile = input.Profile
				found = true
				break
			}
		}
		if !found {
			s.writeError(w, http.StatusNotFound, "config", "route_not_found", input.RouteID, "")
			return
		}
	}
	if err := clone.Validate(); err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "config", "validation_failed", err.Error(), "")
		return
	}
	snapshot, err := config.NewSnapshot(clone, "quick-switch")
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "config", "snapshot_failed", err.Error(), "")
		return
	}
	data, _ := yaml.Marshal(clone)
	if s.store != nil {
		if err := s.store.SaveVersion(r.Context(), store.Version{ID: snapshot.ID, CreatedBy: "quick-switch", Active: true, SHA256: snapshot.Hash, Description: input.Description, YAML: data}); err != nil {
			s.writeError(w, http.StatusInternalServerError, "store", "save_version_failed", err.Error(), "")
			return
		}
		if err := s.store.Activate(r.Context(), snapshot.ID); err != nil {
			s.writeError(w, http.StatusInternalServerError, "store", "activate_version_failed", err.Error(), "")
			return
		}
	}
	if err := s.Activate(snapshot); err != nil {
		s.writeError(w, http.StatusInternalServerError, "config", "activation_failed", err.Error(), "")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"switched": true, "profile": input.Profile, "route_id": input.RouteID, "id": snapshot.ID})
}

func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	var input struct {
		YAML string `json:"yaml"`
		Path string `json:"path"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&input); err != nil {
		s.writeError(w, http.StatusBadRequest, "config", "invalid_json", err.Error(), "")
		return
	}
	data := []byte(input.YAML)
	if len(bytes.TrimSpace(data)) == 0 && input.Path != "" {
		var err error
		data, err = os.ReadFile(input.Path)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "config", "read_failed", err.Error(), "")
			return
		}
	}
	cfg, err := config.Parse(data)
	if err != nil {
		s.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	snapshot, err := config.NewSnapshot(cfg, "validate")
	if err != nil {
		s.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"valid": true, "id": snapshot.ID, "hash": snapshot.Hash, "profiles": len(cfg.Profiles), "routes": len(cfg.Routes)})
}

func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		s.writeError(w, http.StatusNotImplemented, "config", "store_unavailable", "configuration history is unavailable", "")
		return
	}
	var input struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.ID == "" {
		s.writeError(w, http.StatusBadRequest, "config", "version_required", "version id is required", "")
		return
	}
	version, err := s.store.GetVersion(r.Context(), input.ID)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "config", "version_not_found", err.Error(), "")
		return
	}
	cfg, err := config.Parse(version.YAML)
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "config", "stored_config_invalid", err.Error(), "")
		return
	}
	snapshot, err := config.NewSnapshot(cfg, "rollback:"+input.ID)
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "config", "snapshot_failed", err.Error(), "")
		return
	}
	if err := s.store.Activate(r.Context(), input.ID); err != nil {
		s.writeError(w, http.StatusInternalServerError, "config", "store_activation_failed", err.Error(), "")
		return
	}
	if err := s.Activate(snapshot); err != nil {
		s.writeError(w, http.StatusInternalServerError, "config", "activation_failed", err.Error(), "")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"rolled_back": true, "id": snapshot.ID})
}

func (s *Server) handleVersions(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		s.writeJSON(w, http.StatusOK, map[string]any{"versions": []any{}})
		return
	}
	items, err := s.store.ListVersions(r.Context(), queryLimit(r, 100))
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "store", "list_versions_failed", err.Error(), "")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"versions": items})
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		s.writeJSON(w, http.StatusOK, map[string]any{"events": []any{}})
		return
	}
	items, err := s.store.ListAudit(r.Context(), queryLimit(r, 100))
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "store", "list_audit_failed", err.Error(), "")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"events": items})
}

func (s *Server) handleRecordings(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		s.writeJSON(w, http.StatusOK, map[string]any{"recordings": []any{}})
		return
	}
	items, err := s.store.ListRecordings(r.Context(), queryLimit(r, 100))
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "store", "list_recordings_failed", err.Error(), "")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"recordings": items})
}

// queryLimit reads an optional ?limit= parameter, clamped to a sane range.
func queryLimit(r *http.Request, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	if value > 200 {
		return 200
	}
	return value
}

func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request) {
	cfg := s.Snapshot().Config
	req := route.FromHTTPRequest(r)
	req.Profile = r.URL.Query().Get("profile")
	if r.Method == http.MethodPost {
		var input struct {
			Method  string              `json:"method"`
			Path    string              `json:"path"`
			Headers map[string][]string `json:"headers"`
			Profile string              `json:"profile"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&input); err == nil {
			if input.Method != "" {
				req.Method = input.Method
			}
			if input.Path != "" {
				req.Path = input.Path
			}
			if input.Headers != nil {
				req.Headers = input.Headers
			}
			if input.Profile != "" {
				req.Profile = input.Profile
			}
		}
	}
	matched := route.MatchRoutes(cfg, req)
	profile, selected, err := route.SelectProfile(cfg, req)
	payload := map[string]any{"matched_routes": matched, "selected_route": selected}
	if profile != nil {
		payload["profile"] = profile
	}
	if err != nil {
		payload["error"] = err.Error()
	}
	s.writeJSON(w, http.StatusOK, payload)
}

func (s *Server) callClientKey(cfg *config.Config, r *http.Request) string {
	key := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if key == "" {
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			key = strings.TrimSpace(auth[7:])
		}
	}
	if key == "" {
		return ""
	}
	for id, client := range cfg.Clients {
		if value := resolveClientKey(client); value != "" && secureEqual(value, key) {
			return id
		}
	}
	if secureEqual(os.Getenv("GATEWAY_CLIENT_API_KEY"), key) {
		return "local"
	}
	return ""
}

func (s *Server) authenticateClient(cfg *config.Config, r *http.Request) (string, string) {
	id := s.callClientKey(cfg, r)
	if id == "" {
		return "", ""
	}
	client := cfg.Clients[id]
	tenant := client.Tenant
	return id, tenant
}

func requiresClientAuth(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	for _, client := range cfg.Clients {
		if client.Enabled != nil && !*client.Enabled {
			continue
		}
		if resolveClientKey(client) != "" {
			return true
		}
	}
	return false
}

func (s *Server) authorizeAdmin(r *http.Request) bool {
	token := strings.TrimSpace(os.Getenv("GATEWAY_ADMIN_TOKEN"))
	if token == "" {
		token = defaultAdminToken
	}
	if token == "" {
		return isLoopbackRemote(r.RemoteAddr)
	}
	provided := strings.TrimSpace(r.Header.Get("X-Admin-Token"))
	if provided == "" {
		provided = strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	}
	return provided != "" && secureEqual(provided, token)
}

// secureEqual compares secrets in constant time to avoid leaking content
// through response-time differences.
func secureEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (s *Server) statusPayload() map[string]any {
	snapshot := s.Snapshot()
	configInfo := map[string]any{}
	if snapshot != nil {
		configInfo = map[string]any{"id": snapshot.ID, "hash": snapshot.Hash, "version": snapshot.Version, "loaded_at": snapshot.LoadedAt, "source": snapshot.Source}
	}
	return map[string]any{"gateway_version": BuildVersion, "started_at": s.started, "config": configInfo, "metrics": s.metrics.Snapshot(), "adapters": s.registry.Names()}
}

// BuildVersion mirrors the CLI build version; it is set via -ldflags at
// build time and defaults to "dev".
var BuildVersion = "dev"

func (s *Server) profilePayload() []map[string]any {
	cfg := s.Snapshot().Config
	result := make([]map[string]any, 0, len(cfg.Profiles))
	for id, p := range cfg.Profiles {
		result = append(result, map[string]any{"id": id, "name": p.Name, "adapter": p.Adapter, "client_contract": p.ClientContract, "upstream_contract": p.UpstreamContract, "traffic_mode": normalizeMode(p.TrafficMode), "upstream": p.Upstream, "stream_safe": p.IsStreamSafe(), "allowed_methods": p.AllowedMethods})
	}
	return result
}

func (s *Server) previewPayload(requestID string, profile *config.Profile, selectedRoute *config.Route, upstream string, env transform.Envelope, mode string, extraRedactions map[string]bool) map[string]any {
	body := any(string(env.Body))
	var decoded any
	if json.Unmarshal(env.Body, &decoded) == nil {
		body = decoded
	}
	return map[string]any{"request_id": requestID, "mode": mode, "profile": profile.Name, "route": selectedRoute, "upstream": upstream, "request": map[string]any{"method": env.Method, "path": env.Path, "query": env.Query, "headers": redactHeadersWith(env.Headers, extraRedactions), "body": body}}
}

func (s *Server) writeDryRunResponse(w http.ResponseWriter, requestID string, profile *config.Profile, prepared transform.Envelope) {
	contract := strings.ToLower(profile.ClientContract)
	message := "[dry-run] request validated; upstream was not called"
	model := requestModel(prepared.Body)
	if model == "" {
		model = "gateway-dry-run"
	}
	if wantsStreamFromEnvelope(prepared) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Gateway-Dry-Run", "true")
		content := map[string]any{"id": "dryrun-" + requestID, "object": "chat.completion.chunk", "model": model, "choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "content": message}}}}
		finish := map[string]any{"id": "dryrun-" + requestID, "object": "chat.completion.chunk", "model": model, "choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}}
		contentBytes, _ := json.Marshal(content)
		finishBytes, _ := json.Marshal(finish)
		_, _ = w.Write(adapter.EncodeSSE([]adapter.SSEEvent{{Data: string(contentBytes)}, {Data: string(finishBytes)}, {Data: "[DONE]"}}))
		return
	}
	var payload any
	switch {
	case strings.Contains(contract, "anthropic"):
		payload = map[string]any{"id": "dryrun-" + requestID, "type": "message", "role": "assistant", "model": model, "content": []any{map[string]any{"type": "text", "text": message}}, "stop_reason": "end_turn", "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}}
	case strings.Contains(contract, "responses"):
		payload = map[string]any{"id": "dryrun-" + requestID, "object": "response", "status": "completed", "model": model, "output": []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": message}}}}, "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}}
	case strings.Contains(contract, "openai") || strings.Contains(prepared.Path, "/chat/completions"):
		payload = map[string]any{"id": "dryrun-" + requestID, "object": "chat.completion", "model": model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": message}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}
	default:
		payload = map[string]any{"mode": "dry-run", "request_id": requestID, "profile": profile.Name, "upstream_called": false, "request": map[string]any{"method": prepared.Method, "path": prepared.Path, "query": prepared.Query, "headers": redactHeaders(prepared.Headers), "body": json.RawMessage(prepared.Body)}}
	}
	w.Header().Set("X-Gateway-Dry-Run", "true")
	s.writeJSON(w, http.StatusOK, payload)
}

func (s *Server) observe(requestID string, started time.Time, r *http.Request, selectedRoute *config.Route, profile *config.Profile, upstream string, status int, stage, code string) {
	routeID := ""
	if selectedRoute != nil {
		routeID = selectedRoute.ID
	}
	profileName, mode := "", ""
	if profile != nil {
		profileName, mode = profile.Name, normalizeMode(profile.TrafficMode)
	}
	duration := time.Since(started)
	s.metrics.Observe(telemetry.RequestRecord{RequestID: requestID, StartedAt: started, Duration: duration, Method: r.Method, Path: r.URL.Path, Route: routeID, Profile: profileName, Mode: mode, Upstream: upstream, Status: status, ErrorStage: stage, ErrorCode: code})
	if status >= 400 || code != "" {
		slog.Info("request", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "route", routeID, "profile", profileName, "mode", mode, "upstream", upstream, "status", status, "duration_ms", duration.Milliseconds(), "error_stage", stage, "error_code", code)
	} else {
		slog.Debug("request", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "route", routeID, "profile", profileName, "mode", mode, "upstream", upstream, "status", status, "duration_ms", duration.Milliseconds())
	}
	if snapshot := s.Snapshot(); snapshot != nil && snapshot.Config != nil && auditEnabled(snapshot.Config) {
		s.submitAudit(store.AuditEvent{
			Action:    "request",
			Actor:     "gateway",
			RequestID: requestID,
			Details:   map[string]any{"method": r.Method, "path": r.URL.Path, "route": routeID, "profile": profileName, "mode": mode, "upstream": upstream, "status": status, "error_stage": stage, "error_code": code, "duration_ms": duration.Milliseconds()},
		})
	}
}

// drainAuditEvents writes request audit records asynchronously so SQLite
// inserts never sit in the data-plane hot path; overflow is dropped with a
// warning rather than blocking requests.
func (s *Server) drainAuditEvents() {
	defer s.auditWG.Done()
	for event := range s.auditCh {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.store.AddAudit(ctx, event); err != nil {
			slog.Warn("audit write failed", "action", event.Action, "error", err)
		}
		cancel()
	}
}

// Close flushes pending audit writes; call it after the HTTP servers have
// shut down so queued events are not lost on exit.
func (s *Server) Close() {
	if s.auditCh != nil {
		close(s.auditCh)
		s.auditWG.Wait()
	}
}

func (s *Server) submitAudit(event store.AuditEvent) {
	if s.store == nil {
		return
	}
	select {
	case s.auditCh <- event:
	default:
		slog.Warn("audit event dropped", "action", event.Action, "reason", "audit_backlog")
	}
}

// auditEnabled reports whether policies.audit.enabled requests per-request
// audit records.
func auditEnabled(cfg *config.Config) bool {
	audit, _ := cfg.Policies["audit"].(map[string]any)
	enabled, _ := audit["enabled"].(bool)
	return enabled
}

// configuredRedactions returns extra header names listed under
// policies.audit.redact, lower-cased for case-insensitive matching.
func configuredRedactions(cfg *config.Config) map[string]bool {
	out := map[string]bool{}
	if cfg == nil {
		return out
	}
	audit, _ := cfg.Policies["audit"].(map[string]any)
	list, _ := audit["redact"].([]any)
	for _, item := range list {
		if text, ok := item.(string); ok {
			if name := strings.ToLower(strings.TrimSpace(text)); name != "" {
				out[name] = true
			}
		}
	}
	return out
}

func redactHeaders(headers http.Header) map[string][]string {
	return redactHeadersWith(headers, nil)
}

func redactHeadersWith(headers http.Header, extra map[string]bool) map[string][]string {
	out := map[string][]string{}
	for key, values := range headers {
		if isSensitiveHeader(key) || extra[strings.ToLower(key)] {
			out[key] = []string{"[REDACTED]"}
		} else {
			out[key] = append([]string(nil), values...)
		}
	}
	return out
}

func (s *Server) writeStatic(w http.ResponseWriter, r *http.Request) {
	requested := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if requested == "" || requested == "." {
		requested = "index.html"
	}
	contentType := "text/html; charset=utf-8"
	switch {
	case strings.HasSuffix(requested, ".css"):
		contentType = "text/css; charset=utf-8"
	case strings.HasSuffix(requested, ".js"):
		contentType = "text/javascript; charset=utf-8"
	}
	if requested == "index.html" || requested == "" {
		// The console is a static, self-contained page; locking it to itself
		// closes the usual XSS injection surface.
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'self'")
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	data, err := web.FS.ReadFile(requested)
	if err != nil {
		// Fall back to legacy on-disk assets (for deployments that overlay a
		// customised console), then to a placeholder page.
		if disk, diskErr := os.ReadFile(filepath.Join("web", filepath.FromSlash(requested))); diskErr == nil {
			data = disk
		} else {
			data = []byte("<!doctype html><title>Unified API Gateway</title><p>Web console assets are not installed.</p>")
		}
	} else if etag := staticETag(data); etag != "" {
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("ETag", etag)
		if strings.TrimSpace(r.Header.Get("If-None-Match")) == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	_, _ = w.Write(data)
}

func staticETag(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:12]) + `"`
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Server) writeEnvelope(w http.ResponseWriter, env transform.Envelope) {
	for key, values := range env.Headers {
		// Content-Length describes the upstream body, which adapters and
		// response transforms may have rewritten; forwarding it truncates or
		// corrupts the client stream. net/http computes the real length.
		if isHopByHopHeader(key) || strings.EqualFold(key, "Content-Length") || strings.EqualFold(key, "Set-Cookie") {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if w.Header().Get("Content-Type") == "" && env.ContentType != "" {
		w.Header().Set("Content-Type", env.ContentType)
	}
	status := env.StatusCode
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(env.Body)
}

func (s *Server) writeError(w http.ResponseWriter, status int, stage, code, message, requestID string) {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	s.writeJSON(w, status, map[string]any{"error": map[string]any{"message": message, "type": "gateway_error", "code": code, "stage": stage, "request_id": requestID}})
}

func (s *Server) newRequestID() string {
	n := s.sequence.Add(1)
	h := sha256.Sum256([]byte(fmt.Sprintf("%d-%d", time.Now().UnixNano(), n)))
	return hex.EncodeToString(h[:8])
}

// requestIDPattern accepts conservative, header-safe identifiers. Client
// supplied values are echoed back into responses and logs; anything outside
// this shape is replaced with a generated id to prevent log injection.
var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{8,128}$`)

// inboundRequestID honours observability.request_id_header and validates the
// inbound value before reusing it.
func (s *Server) inboundRequestID(cfg *config.Config, r *http.Request) string {
	header := "X-Request-Id"
	if custom := strings.TrimSpace(cfg.Observability.RequestIDHeader); custom != "" {
		header = custom
	}
	if id := strings.TrimSpace(r.Header.Get(header)); requestIDPattern.MatchString(id) {
		return id
	}
	if header != "X-Request-Id" {
		if id := strings.TrimSpace(r.Header.Get("X-Request-Id")); requestIDPattern.MatchString(id) {
			return id
		}
	}
	return s.newRequestID()
}

func headersFromValues(values map[string][]string) http.Header {
	h := make(http.Header, len(values))
	for key, items := range values {
		for _, item := range items {
			h.Add(key, item)
		}
	}
	return h
}

func recordingKey(profile string, env transform.Envelope) string {
	clientID, tenant := metadataStrings(env)
	h := sha256.New()
	_, _ = io.WriteString(h, profile+"\n"+clientID+"\n"+tenant+"\n"+env.Method+"\n"+env.Path+"\n"+env.Query.Encode()+"\n")
	_, _ = h.Write(env.Body)
	return hex.EncodeToString(h.Sum(nil))
}

func metadataStrings(env transform.Envelope) (string, string) {
	clientID, _ := env.Metadata["client_id"].(string)
	tenant, _ := env.Metadata["tenant"].(string)
	return clientID, tenant
}

func filteredRequestHeaders(in http.Header) http.Header {
	out := make(http.Header)
	for key, values := range in {
		if isHopByHopHeader(key) || strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "X-API-Key") || strings.EqualFold(key, "Host") {
			continue
		}
		// Session cookies and the gateway's own admin credential never
		// belong on an upstream request.
		if strings.EqualFold(key, "Cookie") || strings.EqualFold(key, "X-Admin-Token") {
			continue
		}
		// Sec-WebSocket-* headers describe the client's upgrade handshake;
		// the outbound dialer must generate its own, and gorilla rejects a
		// caller-supplied Sec-WebSocket-Key/Version outright.
		if strings.HasPrefix(strings.ToLower(key), "sec-websocket-") {
			continue
		}
		out[key] = append([]string(nil), values...)
	}
	return out
}

func isHopByHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func isSensitiveHeader(key string) bool {
	switch strings.ToLower(key) {
	case "authorization", "proxy-authorization", "x-api-key", "x-admin-token", "cookie", "set-cookie":
		return true
	default:
		return false
	}
}

func resolveCredential(c config.Credential) string {
	if c.Value != "" {
		return c.Value
	}
	// Both "env" and "api_key" (Anthropic-style X-API-Key) credentials name
	// an environment variable in Env (or Ref); only the injected header
	// differs, which the caller picks from the type.
	if strings.EqualFold(c.Type, "env") || strings.EqualFold(c.Type, "api_key") {
		name := c.Env
		if name == "" {
			name = c.Ref
		}
		if name != "" {
			return os.Getenv(strings.TrimPrefix(name, "env:"))
		}
	}
	if c.Ref != "" {
		return resolveSecretValue(c.Ref)
	}
	return resolveSecretValue(c.Env)
}

func resolveSecretValue(value string) string {
	if strings.HasPrefix(value, "env:") {
		return os.Getenv(strings.TrimPrefix(value, "env:"))
	}
	if strings.HasPrefix(value, "$ENV:") {
		return os.Getenv(strings.TrimPrefix(value, "$ENV:"))
	}
	return value
}

func resolveClientKey(c config.ClientCredential) string {
	if c.Key != "" {
		return resolveSecretValue(c.Key)
	}
	if c.KeyEnv != "" {
		return os.Getenv(c.KeyEnv)
	}
	return resolveSecretValue(c.KeyRef)
}

func joinURLPath(base, requestPath string) string {
	if requestPath == "" {
		return base
	}
	if base == "" || base == "/" {
		return requestPath
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(requestPath, "/")
}

func requestScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func requestStream(body []byte) bool {
	var value map[string]any
	return json.Unmarshal(body, &value) == nil && value["stream"] == true
}

func wantsStreamFromEnvelope(env transform.Envelope) bool {
	if env.Metadata == nil {
		return false
	}
	value, _ := env.Metadata["client_stream"].(bool)
	return value
}

func requestModel(body []byte) string {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return ""
	}
	model, _ := value["model"].(string)
	return model
}

func normalizeMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "dry_run" {
		return "dry-run"
	}
	return mode
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type upstreamError struct {
	status int
	code   string
	err    error
}

func (e upstreamError) Error() string { return e.err.Error() }
func (e upstreamError) Unwrap() error { return e.err }

func upstreamStatus(err error) int {
	var e upstreamError
	if errors.As(err, &e) && e.status > 0 {
		return e.status
	}
	return http.StatusBadGateway
}

func upstreamCode(err error) string {
	var e upstreamError
	if errors.As(err, &e) && e.code != "" {
		return e.code
	}
	return "upstream_error"
}

func isLoopbackRemote(remote string) bool {
	host, _, err := netSplitHostPort(remote)
	if err != nil {
		return remote == "127.0.0.1" || remote == "[::1]" || remote == "::1"
	}
	return host == "127.0.0.1" || host == "::1" || host == "[::1]"
}

func netSplitHostPort(value string) (string, string, error) {
	if i := strings.LastIndex(value, ":"); i > -1 && !strings.Contains(value[i+1:], ":") {
		return strings.Trim(value[:i], "[]"), value[i+1:], nil
	}
	return value, strconv.Itoa(0), errors.New("no port")
}

func redactConfig(c *config.Config) *config.Config {
	clone := c.Clone()
	for id, credential := range clone.Credentials {
		credential.Value = "[REDACTED]"
		// A Ref that is not an env:/$ENV: reference is a literal secret.
		if credential.Ref != "" && !strings.HasPrefix(credential.Ref, "env:") && !strings.HasPrefix(credential.Ref, "$ENV:") {
			credential.Ref = "[REDACTED]"
		}
		clone.Credentials[id] = credential
	}
	for id, client := range clone.Clients {
		if client.Key != "" {
			client.Key = "[REDACTED]"
		}
		if client.KeyRef != "" && !strings.HasPrefix(client.KeyRef, "env:") && !strings.HasPrefix(client.KeyRef, "$ENV:") {
			client.KeyRef = "[REDACTED]"
		}
		clone.Clients[id] = client
	}
	for id, upstream := range clone.Upstreams {
		for key := range upstream.Headers {
			if isSensitiveHeader(key) {
				upstream.Headers[key] = "[REDACTED]"
			}
		}
		clone.Upstreams[id] = upstream
	}
	return clone
}

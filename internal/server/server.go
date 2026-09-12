// Package server implements the gateway data plane and its local management
// API. The handlers operate on immutable config snapshots, so an activation
// can never change the profile of an in-flight request.
package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/adapter"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/store"
	"unified-api-gateway/internal/telemetry"
	"unified-api-gateway/internal/transform"
)

const (
	defaultMaxBody    int64 = 8 << 20
	defaultAdminToken       = ""
)

var ErrClientAuthKeysRequired = errors.New("require_client_auth is enabled but no client credential resolves to a key")

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
	state         atomic.Pointer[gatewayState]
	ingressActive atomic.Int64
	admissionMu   sync.Mutex
	admissions    map[string]*profileAdmission
	// configMu serializes activation, rollback and quick-switch so two
	// concurrent admin calls cannot interleave their store/memory phases.
	configMu   sync.Mutex
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
	auditMu   sync.RWMutex
	auditOnce sync.Once
	auditDone bool
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
	snapshot, err := config.NewSnapshot(options.Snapshot.Config, options.Snapshot.Source)
	if err != nil {
		return nil, err
	}
	if err := s.publishState(snapshot); err != nil {
		return nil, err
	}
	if options.Store != nil {
		s.auditWG.Add(1)
		go s.drainAuditEvents()
	}
	return s, nil
}

// publishState derives every runtime view before swapping the single state
// pointer. A failed pipeline compile must never publish a partially enforced
// configuration.
func (s *Server) publishState(snapshot *config.ConfigSnapshot) error {
	if snapshot == nil || snapshot.Config == nil {
		return errors.New("server: config snapshot is required")
	}
	cfg := snapshot.Config
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("server: invalid configuration: %w", err)
	}
	for _, warning := range cfg.ShadowedRoutes() {
		slog.Warn("route selection conflict", "detail", warning)
	}
	pipelines, err := buildPipelines(cfg)
	if err != nil {
		return err
	}
	state := &gatewayState{
		snapshot:  snapshot,
		runtimes:  s.buildRuntimes(cfg),
		pipelines: pipelines,
		policy:    decodePolicySet(cfg),
	}
	s.warnInsecureUpstreams(cfg)
	s.state.Store(state)
	return nil
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
	if timeout <= 0 && !stream {
		timeout = s.client.Timeout
	}
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
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client := &http.Client{Timeout: timeout, CheckRedirect: noRedirect}
	transport := s.clonableTransport()
	switch {
	case transport == nil:
		// A custom, non-clonable RoundTripper was injected: preserve it on
		// both buffered and streaming paths and only override the total
		// timeout (zero for streams).
		copyOf := *s.client
		copyOf.Timeout = timeout
		copyOf.CheckRedirect = noRedirect
		client = &copyOf
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

// Snapshot returns an independent copy for callers to inspect or edit.
func (s *Server) Snapshot() *config.ConfigSnapshot {
	return s.currentSnapshot().Clone()
}

// currentSnapshot is for internal readers only. Published configuration must
// never be mutated; edits start from a clone and go through activation.
func (s *Server) currentSnapshot() *config.ConfigSnapshot {
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
	// Reparse the persisted representation before assigning its content address
	// so caller-edited metadata cannot corrupt version history or restart state.
	// Config's YAML marshaler preserves numeric rule values and status-map keys.
	data, err := yaml.Marshal(snapshot.Config)
	if err != nil {
		return fmt.Errorf("marshal activated configuration: %w", err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return err
	}
	candidate, err := config.NewSnapshot(cfg, snapshot.Source)
	if err != nil {
		return err
	}
	if err := validateRuntimeRequirements(candidate.Config); err != nil {
		return err
	}
	s.configMu.Lock()
	err = s.commitConfig(candidate, data, "server-api", "programmatic activation")
	s.configMu.Unlock()
	if err != nil {
		return err
	}
	if s.store != nil {
		if err := s.store.AddAudit(context.Background(), store.AuditEvent{
			Action:    "config.activate",
			Actor:     "admin",
			VersionID: candidate.ID,
			Details:   map[string]any{"source": candidate.Source},
		}); err != nil {
			s.metrics.AuditDropped()
			slog.Warn("audit write failed", "action", "config.activate", "error", err)
		}
	}
	return nil
}

func validateRuntimeRequirements(cfg *config.Config) error {
	if decodePolicySet(cfg).requireClientAuth && !ClientAuthKeysConfigured(cfg) {
		return ErrClientAuthKeysRequired
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

// commitConfig stores a version, publishes it to memory and then flips the
// stored active marker. Memory is authoritative for the running process;
// the DB marker only matters for the next boot.
func (s *Server) commitConfig(snapshot *config.ConfigSnapshot, data []byte, createdBy, description string) error {
	if snapshot == nil || snapshot.Config == nil {
		return errors.New("config snapshot is required")
	}
	if err := validateRuntimeRequirements(snapshot.Config); err != nil {
		return err
	}
	if s.store != nil {
		if err := snapshot.Config.ValidatePersistable(); err != nil {
			return fmt.Errorf("config_not_persistable: %w", err)
		}
	}
	previous := s.state.Load()
	if s.store != nil {
		// Save as inactive first. Marking it active here would make a crash
		// before publishState resurrect a configuration that never ran.
		if err := s.store.SaveVersion(context.Background(), store.Version{ID: snapshot.ID, CreatedBy: createdBy, SHA256: snapshot.Hash, Description: description, YAML: data}); err != nil {
			return err
		}
	}
	if err := s.publishState(snapshot); err != nil {
		if previous != nil {
			s.state.Store(previous)
		}
		return fmt.Errorf("publish configuration state: %w", err)
	}
	if s.store != nil {
		if err := s.store.Activate(context.Background(), snapshot.ID); err != nil {
			// Keep the running and restart state consistent. The newly saved
			// version remains available for a later retry but is not active.
			if previous != nil {
				s.state.Store(previous)
				s.restoreAdmissions(previous.snapshot.Config)
				// buildRuntimes hot-updates shared breaker parameters. Restore
				// those parameters without replacing the previous semaphores or
				// token buckets used by in-flight requests.
				oldPolicy := decodePolicySet(previous.snapshot.Config)
				for _, profile := range previous.snapshot.Config.Profiles {
					upstream := previous.snapshot.Config.Upstreams[profile.Upstream]
					s.breakerFor(breakerIdentity(profile.Upstream, upstream), oldPolicy.breakerCfg)
				}
			}
			return fmt.Errorf("activate stored version: %w", err)
		}
	}
	return nil
}

// ClientAuthKeysConfigured reports whether any enabled client credential
// resolves to a non-empty key. require_client_auth refuses to serve when
// this is false, closing the fail-open default.
func ClientAuthKeysConfigured(cfg *config.Config) bool {
	if cfg != nil {
		for _, client := range cfg.Clients {
			if client.Enabled != nil && !*client.Enabled {
				continue
			}
			if resolveClientKey(client) != "" {
				return true
			}
		}
	}
	// Keep the legacy single-key deployment path consistent with
	// callClientKey: the global environment key is also a valid credential.
	return (cfg == nil || len(cfg.Clients) == 0) && strings.TrimSpace(os.Getenv("GATEWAY_CLIENT_API_KEY")) != ""
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
	ids := make([]string, 0, len(cfg.Clients))
	for id := range cfg.Clients {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		client := cfg.Clients[id]
		if client.Enabled != nil && !*client.Enabled {
			continue
		}
		if value := resolveClientKey(client); value != "" && secureEqual(value, key) {
			return id
		}
	}
	if len(cfg.Clients) == 0 && secureEqual(os.Getenv("GATEWAY_CLIENT_API_KEY"), key) {
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
	if cfg != nil {
		for _, client := range cfg.Clients {
			if client.Enabled != nil && !*client.Enabled {
				continue
			}
			if resolveClientKey(client) != "" {
				return true
			}
		}
	}
	return (cfg == nil || len(cfg.Clients) == 0) && strings.TrimSpace(os.Getenv("GATEWAY_CLIENT_API_KEY")) != ""
}

// secureEqual compares secrets in constant time to avoid leaking content
// through response-time differences.
func secureEqual(a, b string) bool {
	// Hash first so differing secret lengths do not produce an early-return
	// timing signal from ConstantTimeCompare.
	left := sha256.Sum256([]byte(a))
	right := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(left[:], right[:]) == 1
}

// BuildVersion mirrors the CLI build version; it is set via -ldflags at
// build time and defaults to "dev".
var BuildVersion = "dev"

func (s *Server) observe(cfg *config.Config, requestID string, started time.Time, r *http.Request, selectedRoute *config.Route, profile *config.Profile, upstream string, status int, stage, code string) {
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
	if cfg != nil && auditEnabled(cfg) {
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
			s.metrics.AuditDropped()
			slog.Warn("audit write failed", "action", event.Action, "error", err)
		}
		cancel()
	}
}

// Close flushes pending audit writes; call it after the HTTP servers have
// shut down so queued events are not lost on exit.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.auditOnce.Do(func() {
		s.auditMu.Lock()
		s.auditDone = true
		if s.auditCh != nil {
			close(s.auditCh)
		}
		s.auditMu.Unlock()
		s.auditWG.Wait()
	})
}

func (s *Server) submitAudit(event store.AuditEvent) {
	if s.store == nil {
		return
	}
	s.auditMu.RLock()
	defer s.auditMu.RUnlock()
	if s.auditDone {
		return
	}
	select {
	case s.auditCh <- event:
	default:
		s.metrics.AuditDropped()
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
	add := func(text string) {
		if name := strings.ToLower(strings.TrimSpace(text)); name != "" {
			out[name] = true
		}
	}
	switch list := audit["redact"].(type) {
	case []any:
		for _, item := range list {
			if text, ok := item.(string); ok {
				add(text)
			}
		}
	case []string:
		for _, text := range list {
			add(text)
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

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Server) writeError(w http.ResponseWriter, status int, stage, code, message, requestID string) {
	s.writeErrorForProfile(w, status, stage, code, message, requestID, nil)
}

func (s *Server) writeErrorForProfile(w http.ResponseWriter, status int, stage, code, message, requestID string, profile *config.Profile) {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	details := map[string]any{"message": message, "type": "gateway_error", "code": code, "stage": stage, "request_id": requestID}
	if profile != nil && strings.Contains(strings.ToLower(profile.ClientContract), "anthropic") {
		s.writeJSON(w, status, map[string]any{"type": "error", "error": details})
		return
	}
	s.writeJSON(w, status, map[string]any{"error": details})
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

func filteredRequestHeaders(in http.Header) http.Header {
	out := make(http.Header)
	connectionHeaders := connectionHeaderNames(in)
	for key, values := range in {
		if isHopByHopHeader(key) || connectionHeaders[strings.ToLower(key)] || strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "X-API-Key") || strings.EqualFold(key, "Host") {
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

func connectionHeaderNames(headers http.Header) map[string]bool {
	out := map[string]bool{}
	for _, value := range headers.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			if normalized := strings.ToLower(strings.TrimSpace(name)); normalized != "" {
				out[normalized] = true
			}
		}
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
	normalized := strings.ToLower(strings.TrimSpace(key))
	switch normalized {
	case "authorization", "proxy-authorization", "x-api-key", "x-admin-token", "cookie", "set-cookie":
		return true
	}
	return strings.Contains(normalized, "api-key") ||
		strings.HasSuffix(normalized, "-token") ||
		strings.HasSuffix(normalized, "-secret") ||
		strings.HasSuffix(normalized, "-password")
}

func resolveCredential(c config.Credential) string {
	if c.Value != "" {
		return c.Value
	}
	// Both "env" and "api_key" (Anthropic-style X-API-Key) credentials name
	// an environment variable in Env (or Ref); only the injected header
	// differs, which the caller picks from the type.
	credentialType := strings.ToLower(strings.TrimSpace(c.Type))
	if credentialType == "env" || credentialType == "api_key" {
		name := c.Env
		if name == "" {
			name = c.Ref
		}
		if name != "" {
			return resolveEnvironmentName(name)
		}
	}
	if c.Ref != "" {
		return resolveSecretValue(c.Ref)
	}
	return resolveEnvironmentName(c.Env)
}

func resolveEnvironmentName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.TrimPrefix(strings.TrimPrefix(name, "env:"), "$ENV:")
	return os.Getenv(name)
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

// inspectBody parses a request body once for the two fields the data plane
// needs before routing: the model selector and the stream flag. It replaces
// the previous back-to-back requestModel/requestStream decodes.
func inspectBody(body []byte) (model string, stream bool) {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return "", false
	}
	model, _ = value["model"].(string)
	stream = value["stream"] == true
	return model, stream
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
	var upstream upstreamError
	if errors.As(err, &upstream) {
		switch upstream.code {
		case "build_upstream_request":
			return "failed to build upstream request"
		case "upstream_body_too_large", "upstream_stream_too_large":
			return "upstream response exceeded the configured limit"
		case "client_stream_write_failed":
			return "client stream closed before completion"
		default:
			return "upstream request failed"
		}
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

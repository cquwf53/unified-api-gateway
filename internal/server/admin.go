package server

// Management plane: the admin HTTP surface, configuration activation
// handlers and the embedded web console. Split out of server.go, which keeps
// process state and the plane-independent helpers.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/route"
	"unified-api-gateway/internal/transform"
	"unified-api-gateway/web"
)

func (s *Server) ServeAdminHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" || r.URL.Path == "/livez" {
		if !s.allowAdminMethod(w, r, "GET, HEAD") {
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "started_at": s.started})
		return
	}
	if r.URL.Path == "/readyz" {
		if !s.allowAdminMethod(w, r, "GET, HEAD") {
			return
		}
		s.handleReady(w, r)
		return
	}
	// The embedded login shell contains no configuration or credentials. It
	// must load before the browser can ask for an administrator token.
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		switch r.URL.Path {
		case "/", "/index.html", "/app.js", "/styles.css":
			s.writeStatic(w, r)
			return
		}
	}
	if !s.authorizeAdmin(r) {
		s.writeError(w, http.StatusUnauthorized, "auth", "admin_auth_required", "administrator authentication is required", "")
		return
	}
	if r.URL.Path == "/" || r.URL.Path == "/index.html" || strings.HasSuffix(r.URL.Path, ".css") || strings.HasSuffix(r.URL.Path, ".js") || strings.HasSuffix(r.URL.Path, ".ico") {
		if !s.allowAdminMethod(w, r, "GET, HEAD") {
			return
		}
		s.writeStatic(w, r)
		return
	}
	methods, bodyLimit := adminRequestPolicy(r.URL.Path)
	if methods != "" && !s.allowAdminMethod(w, r, methods) {
		return
	}
	if r.Method == http.MethodPost && bodyLimit > 0 {
		allowed, err := adminOriginAllowed(r)
		if err != nil {
			s.writeError(w, http.StatusServiceUnavailable, "admin", "admin_origin_invalid", err.Error(), "")
			return
		}
		if !allowed {
			s.writeError(w, http.StatusForbidden, "admin", "cross_origin_forbidden", "management requests must use the same origin as the console", "")
			return
		}
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || !strings.EqualFold(mediaType, "application/json") {
			s.writeError(w, http.StatusUnsupportedMediaType, "admin", "json_required", "Content-Type must be application/json", "")
			return
		}
		if r.ContentLength > bodyLimit {
			s.writeError(w, http.StatusRequestEntityTooLarge, "admin", "body_too_large", "management request exceeds its byte limit", "")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, bodyLimit)
	}
	switch r.URL.Path {
	case "/api/status", "/admin/v1/status":
		s.writeJSON(w, http.StatusOK, s.statusPayload())
	case "/api/metrics", "/admin/v1/metrics":
		if !s.currentSnapshot().Config.Observability.MetricsEnabled() {
			s.writeError(w, http.StatusNotFound, "observability", "metrics_disabled", "metrics endpoints are disabled by configuration", "")
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, s.metrics.Prometheus())
	case "/api/config", "/admin/v1/config":
		s.writeJSON(w, http.StatusOK, redactConfig(s.currentSnapshot().Config))
	case "/api/profiles", "/admin/v1/profiles":
		s.writeJSON(w, http.StatusOK, map[string]any{"profiles": s.profilePayload()})
	case "/api/routes", "/admin/v1/routes":
		s.writeJSON(w, http.StatusOK, map[string]any{"routes": s.currentSnapshot().Config.Routes})
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

// adminRequestPolicy keeps aliases on the same method and byte-limit contract.
func adminRequestPolicy(requestPath string) (methods string, bodyLimit int64) {
	switch requestPath {
	case "/api/activate", "/admin/v1/config/activate", "/api/config/validate", "/admin/v1/config/validate":
		return "POST", 16 << 20
	case "/api/preview", "/api/config/preview", "/admin/v1/preview", "/admin/v1/config/preview":
		return "POST", 4 << 20
	case "/api/switch", "/admin/v1/profile/switch", "/api/rollback", "/admin/v1/config/rollback":
		return "POST", 1 << 20
	case "/api/explain", "/admin/v1/routes/explain":
		return "GET, HEAD, POST", 1 << 20
	case "/api/status", "/admin/v1/status", "/api/metrics", "/admin/v1/metrics", "/api/config", "/admin/v1/config",
		"/api/profiles", "/admin/v1/profiles", "/api/routes", "/admin/v1/routes", "/api/versions", "/admin/v1/versions",
		"/api/audit", "/admin/v1/audit", "/api/recordings", "/admin/v1/recordings":
		return "GET, HEAD", 0
	default:
		return "", 0
	}
}

func (s *Server) allowAdminMethod(w http.ResponseWriter, r *http.Request, methods string) bool {
	for _, method := range strings.Split(methods, ", ") {
		if r.Method == method {
			return true
		}
	}
	w.Header().Set("Allow", methods)
	s.writeError(w, http.StatusMethodNotAllowed, "admin", "method_not_allowed", "HTTP method is not allowed for this management endpoint", "")
	return false
}

func parseAdminOrigin(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.Path != "" || strings.ContainsAny(raw, "?#") ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, errors.New("origin must contain only an http/https scheme, host and optional port")
	}
	authority, valid := canonicalWebSocketAuthority(parsed.Host, parsed.Scheme)
	if !valid || authority.port == 0 {
		return nil, errors.New("origin must contain a valid host and port")
	}
	return parsed, nil
}

// A reverse proxy may declare one external origin explicitly. It must preserve
// Host. Forwarded/X-Forwarded-* headers never establish a trusted origin.
func adminOriginAllowed(r *http.Request) (bool, error) {
	var configured *url.URL
	if raw := strings.TrimSpace(os.Getenv("GATEWAY_ADMIN_ORIGIN")); raw != "" {
		var err error
		configured, err = parseAdminOrigin(raw)
		if err != nil {
			return false, fmt.Errorf("GATEWAY_ADMIN_ORIGIN: %w", err)
		}
	}
	origins := r.Header.Values("Origin")
	if len(origins) == 0 {
		// CLI requests have no Origin. Fetch metadata can still identify an
		// explicitly cross-site browser request with a missing Origin.
		return !strings.EqualFold(r.Header.Get("Sec-Fetch-Site"), "cross-site"), nil
	}
	if len(origins) != 1 {
		return false, nil
	}
	origin := strings.TrimSpace(origins[0])
	if _, err := parseAdminOrigin(origin); err != nil {
		return false, nil
	}
	if configured != nil {
		return sameOrigin(origin, configured.Host, configured.Scheme) && sameOrigin(origin, r.Host, configured.Scheme), nil
	}
	return sameOrigin(origin, r.Host, requestScheme(r)), nil
}

// Read the complete bounded document before decoding or applying side effects.
// This rejects a valid JSON prefix followed by form data, a second value, or an
// oversized whitespace suffix, as well as null/unknown-field activation bodies.
func (s *Server) decodeAdminJSON(w http.ResponseWriter, r *http.Request, input any, stage string) bool {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var oversized *http.MaxBytesError
		if errors.As(err, &oversized) {
			s.writeError(w, http.StatusRequestEntityTooLarge, stage, "body_too_large", "management request exceeds its byte limit", "")
		} else {
			s.writeError(w, http.StatusBadRequest, stage, "invalid_json", "cannot read management JSON request", "")
		}
		return false
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' || !json.Valid(data) {
		s.writeError(w, http.StatusBadRequest, stage, "invalid_json", "request body must be exactly one JSON object", "")
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(input); err != nil {
		s.writeError(w, http.StatusBadRequest, stage, "invalid_json", err.Error(), "")
		return false
	}
	return true
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	snapshot := s.currentSnapshot()
	if snapshot == nil || snapshot.Config == nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
		return
	}
	if s.store != nil {
		if err := s.store.Ping(r.Context()); err != nil {
			slog.Warn("readiness store check failed", "error", err)
			s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "store": "unavailable"})
			return
		}
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "config": snapshot.ID})
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
	if !s.decodeAdminJSON(w, r, &input, "preview") {
		return
	}
	pinned := s.state.Load()
	if pinned == nil || pinned.snapshot == nil || pinned.snapshot.Config == nil {
		s.writeError(w, http.StatusServiceUnavailable, "preview", "not_ready", "no active configuration", "")
		return
	}
	snapshot := pinned.snapshot
	match := config.MatchRequest{Method: input.Method, Path: input.Path, Headers: input.Headers, Profile: input.Profile}
	var profile *config.Profile
	var selectedRoute *config.Route
	var err error
	if input.Profile != "" {
		if selected, ok := snapshot.Config.Profiles[input.Profile]; ok {
			selected.RuntimeID = input.Profile
			if selected.Name == "" {
				selected.Name = input.Profile
			}
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
	prepared, err := s.prepareRequest(r.Context(), pinned, snapshot.Config, *profile, in)
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "transform", "preview_failed", safeError(err), "")
		return
	}
	s.writeJSON(w, http.StatusOK, s.previewPayload("preview", profile, selectedRoute, profile.Upstream, prepared, "preview", configuredRedactions(snapshot.Config), snapshot.Config.Observability.RedactPrompts))
}
func (s *Server) writeCommitError(w http.ResponseWriter, err error, fallbackCode string) {
	status, stage, code := http.StatusInternalServerError, "store", fallbackCode
	if errors.Is(err, ErrClientAuthKeysRequired) {
		status, stage, code = http.StatusUnprocessableEntity, "config", "client_auth_keys_required"
	}
	s.writeError(w, status, stage, code, err.Error(), "")
}
func (s *Server) handleActivate(w http.ResponseWriter, r *http.Request) {
	var input struct {
		YAML        string `json:"yaml"`
		Path        string `json:"path"`
		Description string `json:"description"`
		Actor       string `json:"actor"`
	}
	if !s.decodeAdminJSON(w, r, &input, "config") {
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
	s.configMu.Lock()
	defer s.configMu.Unlock()
	createdBy := input.Actor
	if createdBy == "" {
		createdBy = "admin-api"
	}
	if err := s.commitConfig(snapshot, data, createdBy, input.Description); err != nil {
		s.writeCommitError(w, err, "save_version_failed")
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
	if !s.decodeAdminJSON(w, r, &input, "config") {
		return
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	current := s.currentSnapshot()
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
	if err := s.commitConfig(snapshot, data, "quick-switch", input.Description); err != nil {
		s.writeCommitError(w, err, "save_version_failed")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"switched": true, "profile": input.Profile, "route_id": input.RouteID, "id": snapshot.ID})
}

func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	var input struct {
		YAML string `json:"yaml"`
		Path string `json:"path"`
	}
	if !s.decodeAdminJSON(w, r, &input, "config") {
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
	if !s.decodeAdminJSON(w, r, &input, "config") {
		return
	}
	if input.ID == "" {
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
	if version.ID != snapshot.ID || version.SHA256 != "" && version.SHA256 != snapshot.Hash {
		s.writeError(w, http.StatusUnprocessableEntity, "config", "stored_config_integrity_failed", "stored configuration does not match its version id/hash", "")
		return
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if err := s.commitConfig(snapshot, version.YAML, "rollback:"+input.ID, version.Description); err != nil {
		s.writeCommitError(w, err, "rollback_failed")
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
	cfg := s.currentSnapshot().Config
	req := route.FromHTTPRequest(r)
	req.Profile = r.URL.Query().Get("profile")
	if method := r.URL.Query().Get("method"); method != "" {
		req.Method = method
	}
	if requestPath := r.URL.Query().Get("path"); requestPath != "" {
		req.Path = requestPath
	}
	if r.Method == http.MethodPost {
		var input struct {
			Method  string              `json:"method"`
			Path    string              `json:"path"`
			Headers map[string][]string `json:"headers"`
			Profile string              `json:"profile"`
		}
		if !s.decodeAdminJSON(w, r, &input, "explain") {
			return
		}
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
	matched := route.MatchRoutes(cfg, req)
	profile, selected, err := route.SelectProfile(cfg, req)
	payload := map[string]any{"matched_routes": matched, "selected_route": selected}
	if profile != nil {
		safeProfile := cfg.Redacted().Profiles[profile.RuntimeID]
		safeProfile.Name = profile.Name
		payload["profile"] = safeProfile
	}
	if err != nil {
		payload["error"] = err.Error()
	}
	s.writeJSON(w, http.StatusOK, payload)
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
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if len(auth) >= 7 && strings.EqualFold(auth[:7], "bearer ") {
			provided = strings.TrimSpace(auth[7:])
		}
	}
	return provided != "" && secureEqual(provided, token)
}
func (s *Server) statusPayload() map[string]any {
	snapshot := s.currentSnapshot()
	configInfo := map[string]any{}
	if snapshot != nil {
		configInfo = map[string]any{"id": snapshot.ID, "hash": snapshot.Hash, "version": snapshot.Version, "loaded_at": snapshot.LoadedAt, "source": snapshot.Source}
	}
	return map[string]any{"gateway_version": BuildVersion, "started_at": s.started, "config": configInfo, "metrics": s.metrics.Snapshot(), "adapters": s.registry.Names()}
}
func (s *Server) profilePayload() []map[string]any {
	cfg := s.currentSnapshot().Config
	result := make([]map[string]any, 0, len(cfg.Profiles))
	for _, id := range cfg.ProfileNames() {
		p := cfg.Profiles[id]
		result = append(result, map[string]any{"id": id, "name": p.Name, "adapter": p.Adapter, "client_contract": p.ClientContract, "upstream_contract": p.UpstreamContract, "traffic_mode": normalizeMode(p.TrafficMode), "upstream": p.Upstream, "stream_safe": p.IsStreamSafe(), "allowed_methods": p.AllowedMethods})
	}
	return result
}

func (s *Server) previewPayload(requestID string, profile *config.Profile, selectedRoute *config.Route, upstream string, env transform.Envelope, mode string, extraRedactions map[string]bool, redactPrompts bool) map[string]any {
	body := any(string(env.Body))
	var decoded any
	if json.Unmarshal(env.Body, &decoded) == nil {
		body = decoded
	}
	if redactPrompts {
		body = "[REDACTED]"
	}
	return map[string]any{"request_id": requestID, "mode": mode, "profile": profile.Name, "route": selectedRoute, "upstream": upstream, "request": map[string]any{"method": env.Method, "path": env.Path, "query": env.Query, "headers": redactHeadersWith(env.Headers, extraRedactions), "body": body}}
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
func redactConfig(c *config.Config) *config.Config {
	return c.Redacted()
}

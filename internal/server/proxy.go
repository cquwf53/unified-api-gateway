package server

// Data plane: ingress routing, the request pipeline and every traffic mode.
// Split out of server.go, which keeps process state, activation and the
// helpers shared by all three planes.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"unified-api-gateway/internal/adapter"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/route"
	"unified-api-gateway/internal/store"
	"unified-api-gateway/internal/transform"
)

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
		s.handleReady(w, r)
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
		if state := s.state.Load(); state != nil && state.snapshot != nil && state.snapshot.Config != nil {
			if (requiresClientAuth(state.snapshot.Config) || state.policy.requireClientAuth) && s.callClientKey(state.snapshot.Config, r) == "" {
				s.writeError(w, http.StatusUnauthorized, "auth", "client_auth_required", "a valid X-API-Key or Bearer client key is required", "")
				return
			}
		}
		s.handleModels(w, r)
		return
	}
	s.handleProxy(w, r)
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
		s.observe(nil, "", started, r, selectedRoute, profile, upstreamName, status, stage, code)
	}
	// Pin every derived view (config, runtimes, pipelines, policy) for the
	// whole request: a concurrent activation must not mix versions mid-flight
	// or strip policy enforcement from an in-flight request.
	pinned := s.state.Load()
	if pinned == nil {
		observeFailure(http.StatusServiceUnavailable, "config", "not_ready", "no active configuration")
		return
	}
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
		s.writeErrorForProfile(w, status, stage, code, message, requestID, profile)
		s.observe(cfg, requestID, started, r, selectedRoute, profile, upstreamName, status, stage, code)
	}
	w.Header().Set("X-Request-Id", requestID)
	if custom := strings.TrimSpace(cfg.Observability.RequestIDHeader); custom != "" && !strings.EqualFold(custom, "X-Request-Id") {
		w.Header().Set(custom, requestID)
	}
	if isWebSocketUpgrade(r) {
		s.handleWebSocket(w, r, pinned, requestID)
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
		fail(http.StatusRequestEntityTooLarge, "ingress", "body_too_large", "request body exceeds configured limit")
		return
	}

	bodyModel, bodyWantsStream := inspectBody(body)
	clientID, tenant := s.authenticateClient(cfg, r)
	if (requiresClientAuth(cfg) || pinned.policy.requireClientAuth) && clientID == "" {
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
	match.Model = bodyModel
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

	mode := normalizeMode(profile.TrafficMode)
	if mode == "" {
		mode = "live"
	}
	upstreamName = profile.Upstream
	upstream, hasUpstream := cfg.Upstreams[upstreamName]
	if !hasUpstream && mode != "mock" && mode != "dry-run" && mode != "replay" {
		fail(http.StatusBadGateway, "upstream", "upstream_not_found", "selected upstream is not configured")
		return
	}
	if mode == "record" && s.store == nil {
		fail(http.StatusNotImplemented, "record", "record_not_configured", "record storage is not configured")
		return
	}
	willCallUpstream := mode == "live" || mode == "shadow" || mode == "record"
	rt := pinned.runtimes[profileRuntimeID(*profile)]
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
		Method:      r.Method,
		Scheme:      requestScheme(r),
		Host:        r.Host,
		Path:        r.URL.Path,
		Query:       r.URL.Query(),
		Headers:     r.Header.Clone(),
		Body:        body,
		ContentType: r.Header.Get("Content-Type"),
		Metadata: map[string]any{
			"request_id":    requestID,
			"client_id":     clientID,
			"tenant":        tenant,
			"config_id":     snapshot.ID,
			"client_stream": bodyWantsStream || strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream"),
		},
	}
	prepared, err := s.prepareRequest(r.Context(), pinned, cfg, *profile, in)
	if err != nil {
		fail(http.StatusUnprocessableEntity, "transform", "request_transform_failed", safeError(err))
		return
	}

	if mode == "dry-run" {
		s.writeDryRunResponse(w, requestID, profile, prepared, cfg.Observability.RedactPrompts, configuredRedactions(cfg))
		s.observe(cfg, requestID, started, r, selectedRoute, profile, upstreamName, http.StatusOK, "", "")
		return
	}
	if mode == "mock" {
		response := transform.Envelope{StatusCode: http.StatusOK, Headers: make(http.Header), Body: []byte(`{}`), ContentType: "application/json"}
		response, err = s.applyResponseTransforms(r.Context(), pinned, cfg, *profile, response)
		if err != nil {
			fail(http.StatusBadGateway, "transform", "mock_response_failed", safeError(err))
			return
		}
		if err := enforceResponseLimit(cfg, *profile, response); err != nil {
			fail(http.StatusBadGateway, "egress", "response_too_large", safeError(err))
			return
		}
		s.writeEnvelope(w, response)
		s.observe(cfg, requestID, started, r, selectedRoute, profile, upstreamName, response.StatusCode, "", "")
		return
	}
	if mode == "replay" {
		if s.store == nil {
			fail(http.StatusNotImplemented, "replay", "replay_not_configured", "replay storage is not configured")
			return
		}
		clientID, tenant, configID := recordingDimensions(prepared)
		recording, replayErr := s.store.GetRecording(r.Context(), recordingKey(recordingNamespace(*profile), prepared), clientID, tenant, configID)
		if replayErr == nil {
			response := transform.Envelope{StatusCode: recording.StatusCode, Headers: headersFromValues(recording.Headers), Body: recording.Body, ContentType: recording.ContentType}
			response.Headers = response.Headers.Clone()
			response.Headers.Set("X-Gateway-Replay", "exact")
			if err := enforceResponseLimit(cfg, *profile, response); err != nil {
				fail(http.StatusBadGateway, "egress", "response_too_large", safeError(err))
				return
			}
			s.writeEnvelope(w, response)
			s.observe(cfg, requestID, started, r, selectedRoute, profile, upstreamName, recording.StatusCode, "", "")
			return
		}
		// Exact hashes rarely match twice for chat traffic, so fall back to
		// the most recent recording of the same profile, method and path and
		// label the response so clients can tell.
		if recording, fuzzyErr := s.store.LatestRecordingForProfile(r.Context(), recordingNamespace(*profile), prepared.Method, prepared.Path, clientID, tenant, configID); fuzzyErr == nil {
			response := transform.Envelope{StatusCode: recording.StatusCode, Headers: headersFromValues(recording.Headers), Body: recording.Body, ContentType: recording.ContentType}
			response.Headers = response.Headers.Clone()
			response.Headers.Set("X-Gateway-Replay", "approximate")
			if err := enforceResponseLimit(cfg, *profile, response); err != nil {
				fail(http.StatusBadGateway, "egress", "response_too_large", safeError(err))
				return
			}
			s.writeEnvelope(w, response)
			s.observe(cfg, requestID, started, r, selectedRoute, profile, upstreamName, recording.StatusCode, "", "")
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
		status, headersSent, streamErr := s.streamUpstream(r.Context(), pinned, cfg, upstream, *profile, prepared, requestID, w)
		recordStreamResult(rt, headersSent, status, streamErr)
		if streamErr != nil && !headersSent {
			s.writeErrorForProfile(w, upstreamStatus(streamErr), "upstream", upstreamCode(streamErr), safeError(streamErr), requestID, profile)
		}
		s.observe(cfg, requestID, started, r, selectedRoute, profile, upstreamName, status, streamErrorStage(headersSent, streamErr), streamErrorCode(streamErr))
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
			s.writeErrorForProfile(w, upstreamStatus(streamErr), "upstream", upstreamCode(streamErr), safeError(streamErr), requestID, profile)
		}
		s.observe(cfg, requestID, started, r, selectedRoute, profile, upstreamName, status, streamErrorStage(headersSent, streamErr), streamErrorCode(streamErr))
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
	if err := enforceResponseLimit(cfg, *profile, response); err != nil {
		fail(http.StatusBadGateway, "egress", "response_too_large", safeError(err))
		return
	}
	if mode == "record" && s.store != nil {
		recordingClient, recordingTenant, recordingConfig := recordingDimensions(prepared)
		if saveErr := s.store.SaveRecording(r.Context(), store.Recording{Key: recordingKey(recordingNamespace(*profile), prepared), ProfileID: recordingNamespace(*profile), Method: prepared.Method, Path: prepared.Path, ClientID: recordingClient, Tenant: recordingTenant, ConfigID: recordingConfig, StatusCode: response.StatusCode, ContentType: response.ContentType, Headers: redactHeadersWith(response.Headers, configuredRedactions(cfg)), Body: response.Body}); saveErr != nil {
			fail(http.StatusInternalServerError, "record", "record_save_failed", saveErr.Error())
			return
		}
	}
	s.writeEnvelope(w, response)
	s.observe(cfg, requestID, started, r, selectedRoute, profile, upstreamName, response.StatusCode, "", "")
}

func canDirectStream(profile config.Profile) bool {
	if !strings.EqualFold(profile.Adapter, "transparent") || len(profile.AllResponseTransforms()) != 0 {
		return false
	}
	if !profile.IsStreamSafe() {
		return false
	}
	clientContract := strings.ToLower(strings.TrimSpace(profile.ClientContract))
	upstreamContract := strings.ToLower(strings.TrimSpace(profile.UpstreamContract))
	return clientContract == "" || upstreamContract == "" || clientContract == upstreamContract
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
		return
	}
	// The client hung up mid-stream: the upstream outcome is unknown, so
	// neither success nor failure is recorded. beginPolicy's deferred,
	// generation-scoped release frees this request's half-open probe.
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
	in.ContentType = converted.ContentType
	if in.ContentType == "" {
		in.ContentType = in.Headers.Get("Content-Type")
	}
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
	if strings.Contains(strings.ToLower(transform.EffectiveContentType(response)), "text/event-stream") {
		events, err := adapter.ParseSSE(bytes.NewReader(response.Body))
		if err != nil {
			return transform.Envelope{}, fmt.Errorf("parse upstream SSE: %w", err)
		}
		translated, err := adapter.TranslateSSE(events, profile.UpstreamContract, profile.ClientContract)
		if err != nil {
			return transform.Envelope{}, err
		}
		clientWantsStream := wantsStreamFromEnvelope(in)
		// Aggregate first, run the transform chain on the JSON body, and
		// only then decide whether to re-wrap as SSE: applying a body rule
		// to SSE text would corrupt it (invalid_json).
		body, contentType, aggregateErr := adapter.AggregateSSE(translated, profile.ClientContract)
		if aggregateErr != nil {
			var failure *adapter.StreamError
			if errors.As(aggregateErr, &failure) {
				response.StatusCode = http.StatusBadGateway
				response.Body = failure.Payload
				response.ContentType = "application/json"
				response.Headers = response.Headers.Clone()
				response.Headers.Set("Content-Type", response.ContentType)
				return s.applyResponseTransforms(ctx, pinned, cfg, profile, errorEnvelope(profile.ClientContract, response))
			}
			return transform.Envelope{}, aggregateErr
		}
		response.Body = body
		response.ContentType = contentType
		response.Headers = response.Headers.Clone()
		response.Headers.Set("Content-Type", response.ContentType)
		for _, compiled := range s.compiledResponseSteps(pinned, cfg, profile) {
			changed, err := compiled.Apply(ctx, response)
			if err != nil {
				return transform.Envelope{}, err
			}
			response = changed
		}
		if clientWantsStream {
			streamBody, err := adapter.EncodeBufferedSSE(response.Body, profile.ClientContract)
			if err != nil {
				return transform.Envelope{}, err
			}
			response.Headers.Set("Content-Type", "text/event-stream")
			response.ContentType = "text/event-stream"
			response.Body = streamBody
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
	if wantsStreamFromEnvelope(in) && !strings.Contains(strings.ToLower(transform.EffectiveContentType(response)), "event-stream") {
		streamBody, err := adapter.EncodeBufferedSSE(response.Body, profile.ClientContract)
		if err != nil {
			return transform.Envelope{}, err
		}
		response.Headers = response.Headers.Clone()
		response.Headers.Set("Content-Type", "text/event-stream")
		response.ContentType = "text/event-stream"
		response.Body = streamBody
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
	return readUpstreamResponse(cfg, profile, response)
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	cfg := s.Snapshot().Config
	models := make([]map[string]any, 0, len(cfg.Profiles))
	for _, id := range cfg.ProfileNames() {
		profile := cfg.Profiles[id]
		models = append(models, map[string]any{"id": id, "object": "model", "owned_by": "gateway", "adapter": profile.Adapter})
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": models})
}
func (s *Server) writeDryRunResponse(w http.ResponseWriter, requestID string, profile *config.Profile, prepared transform.Envelope, redactPrompts bool, extraRedactions map[string]bool) {
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
		_, _ = w.Write(adapter.EncodeSSE(dryRunStreamEvents(requestID, model, message, profile.ClientContract)))
		return
	}
	var payload any
	switch {
	case strings.Contains(contract, "anthropic"):
		payload = map[string]any{"id": "msg_" + requestID, "type": "message", "role": "assistant", "model": model, "content": []any{map[string]any{"type": "text", "text": message}}, "stop_reason": "end_turn", "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}}
	case strings.Contains(contract, "responses"):
		payload = map[string]any{
			"id": "resp_" + requestID, "object": "response", "created_at": time.Now().Unix(), "status": "completed", "model": model,
			"output_text": message,
			"output": []any{map[string]any{
				"id": "msg_" + requestID, "type": "message", "status": "completed", "role": "assistant",
				"content": []any{map[string]any{"type": "output_text", "text": message, "annotations": []any{}}},
			}},
			"usage": map[string]any{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
		}
	case strings.Contains(contract, "openai") || strings.Contains(prepared.Path, "/chat/completions"):
		payload = map[string]any{"id": "chatcmpl-" + requestID, "object": "chat.completion", "created": time.Now().Unix(), "model": model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": message}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}
	default:
		dryRunBody := any(json.RawMessage(prepared.Body))
		if redactPrompts {
			dryRunBody = "[REDACTED]"
		}
		payload = map[string]any{"mode": "dry-run", "request_id": requestID, "profile": profile.Name, "upstream_called": false, "request": map[string]any{"method": prepared.Method, "path": prepared.Path, "query": prepared.Query, "headers": redactHeadersWith(prepared.Headers, extraRedactions), "body": dryRunBody}}
	}
	w.Header().Set("X-Gateway-Dry-Run", "true")
	s.writeJSON(w, http.StatusOK, payload)
}

func dryRunStreamEvents(requestID, model, message, contract string) []adapter.SSEEvent {
	marshalEvent := func(name string, payload map[string]any) adapter.SSEEvent {
		data, _ := json.Marshal(payload)
		return adapter.SSEEvent{Event: name, Data: string(data)}
	}
	messageID := "msg_" + requestID
	canonical := []adapter.SSEEvent{
		marshalEvent("message_start", map[string]any{
			"type":    "message_start",
			"message": map[string]any{"id": messageID, "type": "message", "role": "assistant", "model": model, "content": []any{}, "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}},
		}),
		marshalEvent("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}}),
		marshalEvent("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": message}}),
		marshalEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}),
		marshalEvent("message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil}, "usage": map[string]any{"output_tokens": 0}}),
		marshalEvent("message_stop", map[string]any{"type": "message_stop"}),
	}
	target := strings.ToLower(strings.TrimSpace(contract))
	if !strings.Contains(target, "anthropic") && !strings.Contains(target, "responses") && !strings.Contains(target, "openai") {
		target = "openai-chat"
	}
	translated, err := adapter.TranslateSSE(canonical, "anthropic-messages", target)
	if err != nil {
		return canonical
	}
	return translated
}
func enforceResponseLimit(cfg *config.Config, profile config.Profile, env transform.Envelope) error {
	if int64(len(env.Body)) > responseLimit(cfg, profile) {
		return upstreamError{status: http.StatusBadGateway, code: "response_too_large", err: errors.New("response exceeded configured limit after conversion")}
	}
	return nil
}

func (s *Server) writeEnvelope(w http.ResponseWriter, env transform.Envelope) {
	connectionHeaders := connectionHeaderNames(env.Headers)
	for key, values := range env.Headers {
		// Content-Length describes the upstream body, which adapters and
		// response transforms may have rewritten; forwarding it truncates or
		// corrupts the client stream. net/http computes the real length.
		if isHopByHopHeader(key) || connectionHeaders[strings.ToLower(key)] || strings.EqualFold(key, "Content-Length") || strings.EqualFold(key, "Set-Cookie") {
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
func recordingNamespace(profile config.Profile) string {
	if strings.TrimSpace(profile.Name) != "" {
		return profile.Name
	}
	return profileRuntimeID(profile)
}

func recordingKey(profile string, env transform.Envelope) string {
	clientID, tenant, configID := recordingDimensions(env)
	h := sha256.New()
	_, _ = io.WriteString(h, profile+"\n"+clientID+"\n"+tenant+"\n"+configID+"\n"+env.Method+"\n"+env.Path+"\n"+env.Query.Encode()+"\n")
	_, _ = h.Write(env.Body)
	return hex.EncodeToString(h.Sum(nil))
}

func recordingDimensions(env transform.Envelope) (string, string, string) {
	clientID, _ := env.Metadata["client_id"].(string)
	tenant, _ := env.Metadata["tenant"].(string)
	configID, _ := env.Metadata["config_id"].(string)
	return clientID, tenant, configID
}

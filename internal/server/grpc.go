package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/route"
)

// ServeGRPCHTTP provides a transparent gRPC-over-HTTP proxy surface. The
// listener can be placed behind an h2/h2c terminator; message-level protobuf
// mapping is intentionally rejected until a typed adapter is configured.
func (s *Server) ServeGRPCHTTP(w http.ResponseWriter, r *http.Request) {
	finish := s.metrics.Begin()
	defer finish()
	started := time.Now()
	status := 0
	stage, code := "", ""
	var cfg *config.Config
	var profile *config.Profile
	upstreamName := ""
	defer func() {
		if status == 0 {
			status = http.StatusBadGateway
		}
		s.observe(cfg, "", started, r, nil, profile, upstreamName, status, stage, code)
	}()
	fail := func(nextStatus int, nextStage, nextCode, message string) {
		status, stage, code = nextStatus, nextStage, nextCode
		s.writeError(w, nextStatus, nextStage, nextCode, message, "")
	}
	pinned := s.state.Load()
	if pinned != nil && pinned.snapshot != nil {
		cfg = pinned.snapshot.Config
	}
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/grpc") {
		fail(http.StatusUnsupportedMediaType, "grpc", "content_type_required", "gRPC requests must use application/grpc")
		return
	}
	if pinned == nil {
		fail(http.StatusServiceUnavailable, "grpc", "not_ready", "no active configuration")
		return
	}
	snapshot := pinned.snapshot
	if snapshot == nil || snapshot.Config == nil {
		fail(http.StatusServiceUnavailable, "grpc", "not_ready", "no active configuration")
		return
	}
	cfg = snapshot.Config
	match := route.FromHTTPRequest(r)
	match.ClientID, match.Tenant = s.authenticateClient(cfg, r)
	match.Client = match.ClientID
	if (requiresClientAuth(cfg) || pinned.policy.requireClientAuth) && match.ClientID == "" {
		fail(http.StatusUnauthorized, "auth", "client_auth_required", "a valid client key is required")
		return
	}
	match.Profile = strings.TrimSpace(r.Header.Get("X-Gateway-Profile"))
	if match.Profile != "" && !requiresClientAuth(cfg) && match.ClientID == "" && !pinned.allowsAnonymousOverride() {
		fail(http.StatusForbidden, "auth", "profile_override_denied", "anonymous profile overrides are disabled by policy")
		return
	}
	var err error
	profile, _, err = route.SelectProfile(cfg, match)
	if err != nil || profile == nil {
		fail(http.StatusNotFound, "grpc", "profile_not_found", "no gRPC profile matched")
		return
	}
	upstreamName = profile.Upstream
	if profile.Adapter != "" && !strings.EqualFold(profile.Adapter, "transparent") {
		fail(http.StatusNotImplemented, "grpc", "typed_adapter_required", "gRPC semantic transforms require a typed adapter")
		return
	}
	if rejectionCode, message := transparentPlaneSupported(*profile); rejectionCode != "" {
		fail(http.StatusNotImplemented, "grpc", rejectionCode, message)
		return
	}
	upstream, ok := cfg.Upstreams[profile.Upstream]
	if !ok {
		fail(http.StatusBadGateway, "grpc", "upstream_not_found", "gRPC upstream is not configured")
		return
	}
	if len(profile.AllowedMethods) > 0 && !methodAllowed(profile.AllowedMethods, r.Method) {
		fail(http.StatusMethodNotAllowed, "grpc", "method_not_allowed", "profile does not allow "+r.Method)
		return
	}
	requestLimit := grpcMessageLimit(cfg, *profile)
	if r.ContentLength > requestLimit {
		fail(http.StatusRequestEntityTooLarge, "grpc", "body_too_large", "gRPC message exceeds the profile limit")
		return
	}
	rt := pinned.runtimes[profileRuntimeID(*profile)]
	release, rejection := rt.beginPolicy(r.Context(), r.Method, r.Header, true)
	if rejection != nil {
		if rejection.retryAfter > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%.0f", rejection.retryAfter.Seconds()))
		}
		status, stage, code = rejection.status, "policy", rejection.code
		s.writeError(w, rejection.status, "policy", rejection.code, "request refused by traffic policy: "+rejection.code, "")
		return
	}
	defer release()
	releaseIngress, ingressRejection := s.beginIngress(pinned)
	if ingressRejection != nil {
		fail(ingressRejection.status, "ingress", ingressRejection.code, "too many request bodies are being parsed")
		return
	}
	defer releaseIngress()
	body, err := io.ReadAll(io.LimitReader(r.Body, requestLimit+1))
	releaseIngress()
	if err != nil {
		status, stage, code = http.StatusBadRequest, "grpc", "body_read_failed"
		s.writeError(w, status, stage, code, "failed to read gRPC request body", "")
		return
	}
	if int64(len(body)) > requestLimit {
		status, stage, code = http.StatusRequestEntityTooLarge, "grpc", "body_too_large"
		s.writeError(w, status, stage, code, "gRPC message exceeds the effective request limit", "")
		return
	}
	base, err := url.Parse(upstream.EffectiveURL())
	if err != nil {
		status, stage, code = http.StatusBadGateway, "grpc", "upstream_url_invalid"
		s.writeError(w, status, stage, code, "gRPC upstream URL is invalid", "")
		return
	}
	if base.Scheme == "grpc" {
		base.Scheme = "http"
	} else if base.Scheme == "grpcs" {
		base.Scheme = "https"
	}
	base.Path = joinURLPath(base.Path, r.URL.Path)
	base.RawQuery = joinRawQuery(base.RawQuery, r.URL.RawQuery)
	upstreamContext := r.Context()
	if rt != nil && rt.policy.requestTimeout > 0 {
		var cancel context.CancelFunc
		upstreamContext, cancel = context.WithTimeout(upstreamContext, rt.policy.requestTimeout)
		defer cancel()
	}
	request, err := http.NewRequestWithContext(upstreamContext, r.Method, base.String(), bytes.NewReader(body))
	if err != nil {
		status, stage, code = http.StatusBadGateway, "grpc", "request_build_failed"
		s.writeError(w, status, stage, code, "failed to build gRPC upstream request", "")
		return
	}
	request.Header = filteredRequestHeaders(r.Header)
	request.Header.Set("TE", "trailers")
	applyUpstreamAuth(request.Header, cfg, upstream)
	response, err := s.upstreamClient(upstream, false).Do(request)
	if err != nil {
		rt.recordUpstreamResult(false)
		status, stage, code = http.StatusBadGateway, "grpc", "upstream_request_failed"
		s.writeError(w, status, stage, code, "gRPC upstream request failed", "")
		return
	}
	defer response.Body.Close()
	responseLimit := grpcMessageLimit(cfg, *profile)
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, responseLimit+1))
	if err != nil {
		rt.recordUpstreamResult(false)
		status, stage, code = http.StatusBadGateway, "grpc", "response_read_failed"
		s.writeError(w, status, stage, code, "failed to read gRPC upstream response", "")
		return
	}
	if int64(len(responseBody)) > responseLimit {
		rt.recordUpstreamResult(false)
		status, stage, code = http.StatusBadGateway, "grpc", "response_too_large"
		s.writeError(w, status, stage, code, "upstream gRPC message exceeds the effective response limit", "")
		return
	}
	grpcStatus := strings.TrimSpace(response.Trailer.Get("Grpc-Status"))
	if grpcStatus == "" {
		grpcStatus = strings.TrimSpace(response.Header.Get("Grpc-Status"))
	}
	upstreamSucceeded := response.StatusCode < 500 && !grpcStatusTripsBreaker(grpcStatus)
	rt.recordUpstreamResult(upstreamSucceeded)
	if !upstreamSucceeded {
		stage, code = "grpc", "upstream_grpc_failure"
	}
	connectionHeaders := connectionHeaderNames(response.Header)
	for key, values := range response.Header {
		if isHopByHopHeader(key) || connectionHeaders[strings.ToLower(key)] || strings.EqualFold(key, "Set-Cookie") {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	// A gRPC status normally arrives in HTTP trailers. Declare every trailer
	// before WriteHeader, then populate its values after the response body.
	for key := range response.Trailer {
		if isHopByHopHeader(key) {
			continue
		}
		w.Header().Add("Trailer", http.CanonicalHeaderKey(key))
	}
	status = response.StatusCode
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(responseBody)
	for key, values := range response.Trailer {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
}

func grpcStatusTripsBreaker(status string) bool {
	// Only availability/server-side failures affect the shared upstream
	// circuit. Client/business errors (INVALID_ARGUMENT, NOT_FOUND,
	// PERMISSION_DENIED, UNAUTHENTICATED, ...) must not let one caller trip
	// the breaker for everyone else.
	switch strings.TrimSpace(status) {
	case "2", "4", "13", "14", "15": // UNKNOWN, DEADLINE_EXCEEDED, INTERNAL, UNAVAILABLE, DATA_LOSS
		return true
	default:
		return false
	}
}

func grpcMessageLimit(cfg *config.Config, profile config.Profile) int64 {
	limit := int64(16 << 20)
	if cfg != nil && cfg.Limits.MaxBodyBytes > 0 && cfg.Limits.MaxBodyBytes < limit {
		limit = cfg.Limits.MaxBodyBytes
	}
	if profile.MaxBodyBytes > 0 && profile.MaxBodyBytes < limit {
		limit = profile.MaxBodyBytes
	}
	return limit
}

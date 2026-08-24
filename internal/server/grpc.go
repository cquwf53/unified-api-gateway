package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"unified-api-gateway/internal/route"
)

// ServeGRPCHTTP provides a transparent gRPC-over-HTTP proxy surface. The
// listener can be placed behind an h2/h2c terminator; message-level protobuf
// mapping is intentionally rejected until a typed adapter is configured.
func (s *Server) ServeGRPCHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/grpc") {
		s.writeError(w, http.StatusUnsupportedMediaType, "grpc", "content_type_required", "gRPC requests must use application/grpc", "")
		return
	}
	snapshot := s.Snapshot()
	if snapshot == nil || snapshot.Config == nil {
		s.writeError(w, http.StatusServiceUnavailable, "grpc", "not_ready", "no active configuration", "")
		return
	}
	match := route.FromHTTPRequest(r)
	match.ClientID, match.Tenant = s.authenticateClient(snapshot.Config, r)
	match.Client = match.ClientID
	if requiresClientAuth(snapshot.Config) && match.ClientID == "" {
		s.writeError(w, http.StatusUnauthorized, "auth", "client_auth_required", "a valid client key is required", "")
		return
	}
	pinned := s.state.Load()
	match.Profile = strings.TrimSpace(r.Header.Get("X-Gateway-Profile"))
	if match.Profile != "" && !requiresClientAuth(snapshot.Config) && match.ClientID == "" && !pinned.allowsAnonymousOverride() {
		s.writeError(w, http.StatusForbidden, "auth", "profile_override_denied", "anonymous profile overrides are disabled by policy", "")
		return
	}
	profile, _, err := route.SelectProfile(snapshot.Config, match)
	if err != nil || profile == nil {
		s.writeError(w, http.StatusNotFound, "grpc", "profile_not_found", "no gRPC profile matched", "")
		return
	}
	if profile.Adapter != "" && !strings.EqualFold(profile.Adapter, "transparent") {
		s.writeError(w, http.StatusNotImplemented, "grpc", "typed_adapter_required", "gRPC semantic transforms require a typed adapter", "")
		return
	}
	upstream, ok := snapshot.Config.Upstreams[profile.Upstream]
	if !ok {
		s.writeError(w, http.StatusBadGateway, "grpc", "upstream_not_found", "gRPC upstream is not configured", "")
		return
	}
	if len(profile.AllowedMethods) > 0 && !methodAllowed(profile.AllowedMethods, r.Method) {
		s.writeError(w, http.StatusMethodNotAllowed, "grpc", "method_not_allowed", "profile does not allow "+r.Method, "")
		return
	}
	if profile.MaxBodyBytes > 0 && r.ContentLength > profile.MaxBodyBytes {
		s.writeError(w, http.StatusRequestEntityTooLarge, "grpc", "body_too_large", "gRPC message exceeds the profile limit", "")
		return
	}
	finish := s.metrics.Begin()
	defer finish()
	started := time.Now()
	_ = started
	// The final status is captured by the deferred closure; evaluating
	// s.observe's arguments at defer time would freeze http.StatusOK and
	// record upstream failures as successes.
	status := 0
	stage, code := "", ""
	defer func() {
		if status == 0 {
			status = http.StatusBadGateway
		}
		s.observe("", started, r, nil, profile, profile.Upstream, status, stage, code)
	}()
	rt := pinned.runtimes[profile.Name]
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
	body, err := io.ReadAll(io.LimitReader(r.Body, (16<<20)+1))
	if err != nil {
		status, stage, code = http.StatusBadRequest, "grpc", "body_read_failed"
		s.writeError(w, status, stage, code, err.Error(), "")
		return
	}
	if len(body) > 16<<20 {
		status, stage, code = http.StatusRequestEntityTooLarge, "grpc", "body_too_large"
		s.writeError(w, status, stage, code, "gRPC message exceeds the 16 MiB limit", "")
		return
	}
	// Chunked requests announce no ContentLength; enforce the profile cap
	// on the actual byte count too.
	if profile.MaxBodyBytes > 0 && int64(len(body)) > profile.MaxBodyBytes {
		status, stage, code = http.StatusRequestEntityTooLarge, "grpc", "body_too_large"
		s.writeError(w, status, stage, code, "gRPC message exceeds the profile limit", "")
		return
	}
	base, err := url.Parse(upstream.EffectiveURL())
	if err != nil {
		status, stage, code = http.StatusBadGateway, "grpc", "upstream_url_invalid"
		s.writeError(w, status, stage, code, err.Error(), "")
		return
	}
	if base.Scheme == "grpc" {
		base.Scheme = "http"
	} else if base.Scheme == "grpcs" {
		base.Scheme = "https"
	}
	base.Path = joinURLPath(base.Path, r.URL.Path)
	base.RawQuery = r.URL.RawQuery
	request, err := http.NewRequestWithContext(r.Context(), r.Method, base.String(), bytes.NewReader(body))
	if err != nil {
		status, stage, code = http.StatusBadGateway, "grpc", "request_build_failed"
		s.writeError(w, status, stage, code, err.Error(), "")
		return
	}
	request.Header = filteredRequestHeaders(r.Header)
	applyUpstreamAuth(request.Header, snapshot.Config, upstream)
	response, err := s.upstreamClient(upstream, false).Do(request)
	if err != nil {
		rt.recordUpstreamResult(false)
		status, stage, code = http.StatusBadGateway, "grpc", "upstream_request_failed"
		s.writeError(w, status, stage, code, err.Error(), "")
		return
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, (16<<20)+1))
	if err != nil {
		rt.recordUpstreamResult(false)
		status, stage, code = http.StatusBadGateway, "grpc", "response_read_failed"
		s.writeError(w, status, stage, code, err.Error(), "")
		return
	}
	if len(responseBody) > 16<<20 {
		rt.recordUpstreamResult(false)
		status, stage, code = http.StatusBadGateway, "grpc", "response_too_large"
		s.writeError(w, status, stage, code, "upstream gRPC message exceeds the 16 MiB limit", "")
		return
	}
	rt.recordUpstreamResult(response.StatusCode < 500)
	for key, values := range response.Header {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	status = response.StatusCode
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(responseBody)
}

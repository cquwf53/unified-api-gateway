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
	match.Profile = strings.TrimSpace(r.Header.Get("X-Gateway-Profile"))
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
	finish := s.metrics.Begin()
	defer finish()
	started := time.Now()
	rt := s.runtimeFor(profile.Name)
	release, rejection := rt.beginPolicy(r.Context(), r.Method, r.Header, true)
	if rejection != nil {
		if rejection.retryAfter > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%.0f", rejection.retryAfter.Seconds()))
		}
		s.writeError(w, rejection.status, "policy", rejection.code, "request refused by traffic policy: "+rejection.code, "")
		return
	}
	defer release()
	defer s.observe("", started, r, nil, profile, profile.Upstream, http.StatusOK, "", "")
	body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "grpc", "body_read_failed", err.Error(), "")
		return
	}
	base, err := url.Parse(upstream.EffectiveURL())
	if err != nil {
		s.writeError(w, http.StatusBadGateway, "grpc", "upstream_url_invalid", err.Error(), "")
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
		s.writeError(w, http.StatusBadGateway, "grpc", "request_build_failed", err.Error(), "")
		return
	}
	request.Header = filteredRequestHeaders(r.Header)
	for key, value := range upstream.Headers {
		request.Header.Set(key, resolveSecretValue(value))
	}
	response, err := s.upstreamClient(upstream, false).Do(request)
	if err != nil {
		rt.recordUpstreamResult(false)
		s.writeError(w, http.StatusBadGateway, "grpc", "upstream_request_failed", err.Error(), "")
		return
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		rt.recordUpstreamResult(false)
		s.writeError(w, http.StatusBadGateway, "grpc", "response_read_failed", err.Error(), "")
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
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(responseBody)
}

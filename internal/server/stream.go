package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

// streamUpstream forwards a transparent streaming response as it arrives.
// The caller must only use it when no response-body transform or protocol
// adapter needs the complete stream. Once headers are written, errors are
// reported to telemetry but cannot be replaced with a new HTTP status.
func (s *Server) streamUpstream(ctx context.Context, cfg *config.Config, upstream config.Upstream, profile config.Profile, in transform.Envelope, requestID string, w http.ResponseWriter) (status int, headersSent bool, err error) {
	request, client, err := s.buildUpstreamRequest(ctx, cfg, upstream, in, requestID)
	if err != nil {
		return 0, false, upstreamError{status: http.StatusBadGateway, code: "build_upstream_request", err: err}
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, false, upstreamError{status: http.StatusBadGateway, code: "upstream_request_failed", err: err}
	}
	defer response.Body.Close()
	copyResponseHeaders(w, response.Header)
	status = response.StatusCode
	w.WriteHeader(status)
	headersSent = true
	flusher, _ := w.(http.Flusher)
	max := defaultMaxBody * 4
	if profile.MaxBodyBytes > 0 && profile.MaxBodyBytes*4 < max {
		max = profile.MaxBodyBytes * 4
	}
	var total int64
	buffer := make([]byte, 32<<10)
	for {
		count, readErr := response.Body.Read(buffer)
		if count > 0 {
			total += int64(count)
			if total > max {
				return status, headersSent, upstreamError{status: status, code: "upstream_stream_too_large", err: errors.New("upstream stream exceeded configured limit")}
			}
			if _, writeErr := w.Write(buffer[:count]); writeErr != nil {
				return status, headersSent, upstreamError{status: status, code: "client_stream_write_failed", err: writeErr}
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return status, headersSent, nil
			}
			return status, headersSent, upstreamError{status: status, code: "upstream_stream_read_failed", err: readErr}
		}
	}
}

func (s *Server) buildUpstreamRequest(ctx context.Context, cfg *config.Config, upstream config.Upstream, in transform.Envelope, requestID string) (*http.Request, *http.Client, error) {
	base, err := url.Parse(upstream.EffectiveURL())
	if err != nil {
		return nil, nil, err
	}
	target := *base
	target.Path = joinURLPath(base.Path, in.Path)
	target.RawQuery = in.Query.Encode()
	request, err := http.NewRequestWithContext(ctx, in.Method, target.String(), bytes.NewReader(in.Body))
	if err != nil {
		return nil, nil, err
	}
	request.Header = filteredRequestHeaders(in.Headers)
	for key, value := range upstream.Headers {
		request.Header.Set(key, resolveSecretValue(value))
	}
	if upstream.AuthRef != "" {
		if credential, ok := cfg.Credentials[upstream.AuthRef]; ok {
			if value := resolveCredential(credential); value != "" {
				if strings.EqualFold(credential.Type, "api_key") {
					request.Header.Set("X-API-Key", value)
				} else {
					request.Header.Set("Authorization", "Bearer "+value)
				}
			}
		}
	}
	request.Header.Set("X-Gateway-Request-Id", requestID)
	request.ContentLength = int64(len(in.Body))
	client := s.client
	if timeout := upstream.EffectiveTimeout(); timeout > 0 {
		copyClient := *client
		copyClient.Timeout = timeout
		client = &copyClient
	}
	return request, client, nil
}

func copyResponseHeaders(w http.ResponseWriter, headers http.Header) {
	for key, values := range headers {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
}

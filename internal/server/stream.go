package server

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"unified-api-gateway/internal/adapter"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

// streamResponseHeaderTimeout bounds the time until an upstream sends response
// headers. Streaming bodies are deliberately not covered by a total timeout:
// long completions must be allowed to outlive any fixed client Timeout, which
// would otherwise kill the body read mid-stream.
const streamResponseHeaderTimeout = 30 * time.Second

// buildUpstreamRequest constructs the outbound request for in. Callers pick
// the HTTP client so streaming and buffered paths can use different timeout
// semantics.
func buildUpstreamRequest(ctx context.Context, cfg *config.Config, upstream config.Upstream, in transform.Envelope, requestID string) (*http.Request, error) {
	base, err := url.Parse(upstream.EffectiveURL())
	if err != nil {
		return nil, err
	}
	target := *base
	target.Path = joinURLPath(base.Path, in.Path)
	target.RawQuery = joinRawQuery(base.RawQuery, in.Query.Encode())
	request, err := http.NewRequestWithContext(ctx, in.Method, target.String(), strings.NewReader(string(in.Body)))
	if err != nil {
		return nil, err
	}
	request.Header = filteredRequestHeaders(in.Headers)
	if request.Header.Get("Content-Type") == "" && in.ContentType != "" {
		request.Header.Set("Content-Type", in.ContentType)
	}
	applyUpstreamAuth(request.Header, cfg, upstream)
	request.Header.Set("X-Gateway-Request-Id", requestID)
	if len(in.Body) > 0 {
		request.ContentLength = int64(len(in.Body))
	}
	return request, nil
}

func joinRawQuery(baseQuery, requestQuery string) string {
	baseQuery = strings.Trim(baseQuery, "&")
	requestQuery = strings.Trim(requestQuery, "&")
	if baseQuery == "" {
		return requestQuery
	}
	if requestQuery == "" {
		return baseQuery
	}
	return baseQuery + "&" + requestQuery
}

// streamUpstream forwards a transparent streaming response as it arrives.
// The caller must only use it when no response-body transform or protocol
// adapter needs the complete stream. Non-SSE responses are buffered before
// headers are written and use the normal response pipeline.
func (s *Server) streamUpstream(ctx context.Context, pinned *gatewayState, cfg *config.Config, upstream config.Upstream, profile config.Profile, in transform.Envelope, requestID string, w http.ResponseWriter) (status int, headersSent bool, err error) {
	request, err := buildUpstreamRequest(ctx, cfg, upstream, in, requestID)
	if err != nil {
		return 0, false, upstreamError{status: http.StatusBadGateway, code: "build_upstream_request", err: err}
	}
	response, err := s.upstreamClient(upstream, true).Do(request)
	if err != nil {
		return 0, false, upstreamError{status: http.StatusBadGateway, code: "upstream_request_failed", err: err}
	}
	defer response.Body.Close()
	if !isEventStreamResponse(response) {
		buffered, bufErr := readUpstreamResponse(cfg, profile, response)
		if bufErr != nil {
			return response.StatusCode, false, bufErr
		}
		buffered.Metadata = in.Metadata
		out, prepErr := s.prepareResponse(ctx, pinned, cfg, profile, buffered)
		if prepErr != nil {
			return http.StatusBadGateway, false, prepErr
		}
		if limitErr := enforceResponseLimit(cfg, profile, out); limitErr != nil {
			return http.StatusBadGateway, false, limitErr
		}
		s.writeEnvelope(w, out)
		return out.StatusCode, false, nil
	}
	copyResponseHeaders(w, response.Header)
	status = response.StatusCode
	w.WriteHeader(status)
	headersSent = true
	flusher, _ := w.(http.Flusher)
	max := responseLimit(cfg, profile)
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

// streamTranslatedUpstream serves streaming requests through a semantic
// adapter. Only a successful event-stream response is sent through the live
// translator; all other responses use the buffered conversion pipeline.
func (s *Server) streamTranslatedUpstream(ctx context.Context, pinned *gatewayState, cfg *config.Config, upstream config.Upstream, profile config.Profile, in transform.Envelope, requestID string, w http.ResponseWriter) (status int, headersSent bool, err error) {
	request, err := buildUpstreamRequest(ctx, cfg, upstream, in, requestID)
	if err != nil {
		return 0, false, upstreamError{status: http.StatusBadGateway, code: "build_upstream_request", err: err}
	}
	response, err := s.upstreamClient(upstream, true).Do(request)
	if err != nil {
		return 0, false, upstreamError{status: http.StatusBadGateway, code: "upstream_request_failed", err: err}
	}
	defer response.Body.Close()

	if !isEventStreamResponse(response) {
		buffered, bufErr := readUpstreamResponse(cfg, profile, response)
		if bufErr != nil {
			return response.StatusCode, false, bufErr
		}
		buffered.Metadata = in.Metadata
		out, prepErr := s.prepareResponse(ctx, pinned, cfg, profile, buffered)
		if prepErr != nil {
			return http.StatusBadGateway, false, prepErr
		}
		if limitErr := enforceResponseLimit(cfg, profile, out); limitErr != nil {
			return http.StatusBadGateway, false, limitErr
		}
		s.writeEnvelope(w, out)
		return out.StatusCode, false, nil
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	status = response.StatusCode
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	headersSent = true
	flusher, _ := w.(http.Flusher)
	translator := adapter.NewSSETranslator(profile.UpstreamContract, profile.ClientContract)
	max := responseLimit(cfg, profile)
	var total int64
	streamErr := adapter.ReadSSEStream(response.Body, func(event adapter.SSEEvent) error {
		outEvents, translateErr := translator.Translate(event)
		if translateErr != nil {
			return translateErr
		}
		if len(outEvents) == 0 {
			return nil
		}
		payload := adapter.EncodeSSE(outEvents)
		total += int64(len(payload))
		if total > max {
			return upstreamError{status: status, code: "upstream_stream_too_large", err: errors.New("translated stream exceeded configured limit")}
		}
		if _, writeErr := w.Write(payload); writeErr != nil {
			return upstreamError{status: status, code: "client_stream_write_failed", err: writeErr}
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
	if streamErr != nil {
		// A parser, translator, size, or upstream read failure is not a clean
		// completion. Emit an explicit target-protocol failure when the client is
		// still writable, but never call Finish and fabricate success.
		if upstreamCode(streamErr) != "client_stream_write_failed" {
			if failure := translator.Fail("upstream stream terminated unexpectedly"); len(failure) > 0 {
				_, _ = w.Write(adapter.EncodeSSE(failure))
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
		return status, headersSent, streamErr
	}
	if finishing := translator.Finish(); len(finishing) > 0 {
		if _, writeErr := w.Write(adapter.EncodeSSE(finishing)); writeErr != nil {
			return status, headersSent, upstreamError{status: status, code: "client_stream_write_failed", err: writeErr}
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	return status, headersSent, nil
}

func isEventStreamResponse(response *http.Response) bool {
	if response == nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		return false
	}
	return strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream")
}

// responseLimit is the bounded size an upstream response may reach before the
// gateway aborts with a recognisable error. Profile limits retain the existing
// four-times response allowance, while a configured global limit is also a cap.
func responseLimit(cfg *config.Config, profile config.Profile) int64 {
	max := defaultMaxBody * 4
	if cfg != nil && cfg.Limits.MaxBodyBytes > 0 {
		max = minPositiveLimit(max, cfg.Limits.MaxBodyBytes)
	}
	if profile.MaxBodyBytes > 0 {
		max = minPositiveLimit(max, safeMultiplyLimit(profile.MaxBodyBytes, 4))
	}
	return max
}

func safeMultiplyLimit(value, factor int64) int64 {
	if value <= 0 || factor <= 0 || value > math.MaxInt64/factor {
		return math.MaxInt64
	}
	return value * factor
}

func minPositiveLimit(a, b int64) int64 {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
}

// readUpstreamResponse buffers a bounded upstream response into an envelope.
func readUpstreamResponse(cfg *config.Config, profile config.Profile, response *http.Response) (transform.Envelope, error) {
	max := responseLimit(cfg, profile)
	body, err := io.ReadAll(io.LimitReader(response.Body, max+1))
	if err != nil {
		return transform.Envelope{}, upstreamError{status: http.StatusBadGateway, code: "upstream_body_failed", err: err}
	}
	if int64(len(body)) > max {
		return transform.Envelope{}, upstreamError{status: http.StatusBadGateway, code: "upstream_body_too_large", err: errors.New("upstream response exceeded configured limit")}
	}
	return transform.Envelope{
		Headers:     response.Header.Clone(),
		Body:        body,
		StatusCode:  response.StatusCode,
		ContentType: response.Header.Get("Content-Type"),
	}, nil
}

// applyUpstreamAuth stamps an outbound request with the upstream's static
// headers and credential. It is shared by the HTTP, WebSocket and gRPC planes.
func applyUpstreamAuth(header http.Header, cfg *config.Config, upstream config.Upstream) {
	for key, value := range upstream.Headers {
		header.Set(key, resolveSecretValue(value))
	}
	if upstream.AuthRef == "" || cfg == nil {
		return
	}
	credential, ok := cfg.Credentials[upstream.AuthRef]
	if !ok {
		return
	}
	if value := resolveCredential(credential); value != "" {
		if strings.EqualFold(credential.Type, "api_key") {
			header.Set("X-API-Key", value)
		} else {
			header.Set("Authorization", "Bearer "+value)
		}
	}
}

func copyResponseHeaders(w http.ResponseWriter, headers http.Header) {
	connectionHeaders := connectionHeaderNames(headers)
	for key, values := range headers {
		if isHopByHopHeader(key) || connectionHeaders[strings.ToLower(key)] || strings.EqualFold(key, "Set-Cookie") {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
}

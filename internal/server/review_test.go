package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"unified-api-gateway/internal/config"
)

// Regression tests for issues found in the post-hardening review.

func TestProfileMaxBodyBytesLimitsRequest(t *testing.T) {
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", TrafficMode: "dry-run", MaxBodyBytes: 16},
	}, Routes: []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/thing"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/thing", strings.NewReader(`{"a":"definitely longer than sixteen bytes"}`)))
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	small := httptest.NewRecorder()
	gateway.ServeDataHTTP(small, httptest.NewRequest(http.MethodPost, "/thing", strings.NewReader(`{"a":1}`)))
	if small.Code != http.StatusOK {
		t.Fatalf("small body status=%d", small.Code)
	}
}

func TestExplicitStreamSafeFalseIsHonoured(t *testing.T) {
	// No body rules (would default to streaming-safe) but an explicit false
	// must force buffered conversion.
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "live", StreamSafe: boolPtr(false)},
	}, Upstreams: map[string]config.Upstream{"u": {BaseURL: "http://127.0.0.1:1"}}}
	if cfg.Profiles["p"].IsStreamSafe() {
		t.Fatal("explicit stream_safe: false was flipped to true")
	}
	// Tri-state defaults: unset with body rules -> false, unset without -> true.
	profile := cfg.Profiles["p"]
	profile.StreamSafe = nil
	profile.Request.Body.Set = map[string]any{"x": 1}
	if profile.IsStreamSafe() {
		t.Fatal("body rules must disable streaming by default")
	}
	profile.Request.Body.Set = nil
	if !profile.IsStreamSafe() {
		t.Fatal("no body rules should default to streaming-safe")
	}
}

func TestExplicitStreamSafeFalseBypassesFrameStreaming(t *testing.T) {
	release := make(chan struct{})
	wroteFirst := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"pong\"}}\n\n"))
		flusher.Flush()
		close(wroteFirst)
		<-release
		_, _ = w.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "live", Upstream: "u", StreamSafe: boolPtr(false)},
	}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	gatewayServer := httptest.NewServer(http.HandlerFunc(gateway.ServeDataHTTP))
	defer gatewayServer.Close()
	request, _ := http.NewRequest(http.MethodPost, gatewayServer.URL+"/v1/chat/completions", strings.NewReader(`{"model":"claude","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	request.Header.Set("Content-Type", "application/json")
	type result struct {
		response *http.Response
		err      error
	}
	responseCh := make(chan result, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		responseCh <- result{response, err}
	}()
	<-wroteFirst
	// stream_safe: false buffers the whole upstream stream before replying;
	// while the upstream holds the terminator not even the headers may
	// arrive. (Reading synchronously would deadlock, so the request runs in
	// its own goroutine.)
	select {
	case got := <-responseCh:
		if got.err == nil {
			got.response.Body.Close()
		}
		t.Fatal("frame streaming leaked: response arrived before the upstream finished")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case got := <-responseCh:
		if got.err != nil {
			t.Fatal(got.err)
		}
		defer got.response.Body.Close()
		if deadliner, ok := got.response.Body.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = deadliner.SetReadDeadline(time.Now().Add(5 * time.Second))
		}
		if !strings.Contains(got.response.Header.Get("Content-Type"), "text/event-stream") {
			t.Fatalf("client asked for a stream, content-type=%s", got.response.Header.Get("Content-Type"))
		}
		rest, err := io.ReadAll(got.response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(rest), "pong") || !strings.Contains(string(rest), "[DONE]") {
			t.Fatalf("buffered stream body=%s", rest)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("response never arrived after the upstream completed")
	}
}

func TestAllowedMethodsEnforced(t *testing.T) {
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", TrafficMode: "dry-run", AllowedMethods: []string{"POST"}},
	}, Routes: []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/thing"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/thing", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", recorder.Code)
	}
	if allow := recorder.Header().Get("Allow"); allow != "POST" {
		t.Fatalf("405 must carry an Allow header (RFC 9110), got %q", allow)
	}
	allowed := httptest.NewRecorder()
	gateway.ServeDataHTTP(allowed, httptest.NewRequest(http.MethodPost, "/thing", strings.NewReader(`{}`)))
	if allowed.Code != http.StatusOK {
		t.Fatalf("allowed method status=%d", allowed.Code)
	}
}

func TestMetricsEndpointGatedByConfig(t *testing.T) {
	disabled := false
	gateway := testGateway(t, func(cfg *config.Config) {
		cfg.Observability.Metrics = &disabled
	})
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status=%d", recorder.Code)
	}
	enabled := testGateway(t, nil)
	okRecorder := httptest.NewRecorder()
	enabled.ServeDataHTTP(okRecorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if okRecorder.Code != http.StatusOK {
		t.Fatalf("default should expose metrics, status=%d", okRecorder.Code)
	}
}

func TestUpstreamRetriesRespectIdempotency(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Policies: map[string]any{
		"defaults": map[string]any{"retry": map[string]any{"max_attempts": 1, "idempotent_only": true}},
	}, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL, Retries: 3}}, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", TrafficMode: "live", Upstream: "u"},
	}, Routes: []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/thing"}, Profile: "p"}}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/thing", strings.NewReader(`{}`)))
	if calls != 1 {
		t.Fatalf("upstream retries must not bypass idempotency for POST, calls=%d", calls)
	}
	// The same override still applies to idempotent methods.
	putRecorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(putRecorder, httptest.NewRequest(http.MethodPut, "/thing", nil))
	if calls != 5 { // 1 POST + 4 PUT attempts (Retries=3 → 4 attempts)
		t.Fatalf("idempotent PUT should use upstream retries, calls=%d", calls)
	}
}

// boolPtr keeps StreamSafe literals in tests readable.
func boolPtr(value bool) *bool { return &value }

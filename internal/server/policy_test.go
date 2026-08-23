package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

func policyConfig(t *testing.T, upstreamURL string, policyYAML string) *config.Config {
	t.Helper()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"u": {BaseURL: upstreamURL}},
		Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "transparent", TrafficMode: "live", Upstream: "u",
			Request: transformSpec("path: /v1/thing")}},
		Routes: []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/thing", Methods: []string{"PUT"}}, Profile: "p"}},
	}
	if policyYAML != "" {
		if err := yaml.Unmarshal([]byte(policyYAML), &cfg.Policies); err != nil {
			t.Fatal(err)
		}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestRateLimitReturns429(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer upstream.Close()
	cfg := policyConfig(t, upstream.URL, `
defaults:
  requests_per_second: 1
  burst: 1
`)
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	call := func() int {
		recorder := httptest.NewRecorder()
		gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPut, "/thing", nil))
		return recorder.Code
	}
	if code := call(); code != http.StatusOK {
		t.Fatalf("first request status=%d", code)
	}
	if code := call(); code != http.StatusTooManyRequests {
		t.Fatalf("second request status=%d", code)
	}
}

func TestConcurrencyLimitReturns503(t *testing.T) {
	release := make(chan struct{})
	var seen sync.WaitGroup
	seen.Add(1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Done()
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	cfg := policyConfig(t, upstream.URL, `
defaults:
  max_concurrency: 1
`)
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() {
		recorder := httptest.NewRecorder()
		gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPut, "/thing", nil))
		done <- recorder.Code
	}()
	seen.Wait()
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPut, "/thing", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		close(release)
		t.Fatalf("second request status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	close(release)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("first request status=%d", code)
	}
}

func TestCircuitBreakerOpensAndRecovers(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer upstream.Close()
	cfg := policyConfig(t, upstream.URL, `
defaults:
  circuit_breaker:
    failure_threshold: 2
    reset_timeout_ms: 50
`)
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	call := func() (int, string) {
		recorder := httptest.NewRecorder()
		gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPut, "/thing", nil))
		var payload map[string]any
		_ = json.Unmarshal(recorder.Body.Bytes(), &payload)
		code := ""
		if errPayload, ok := payload["error"].(map[string]any); ok {
			code, _ = errPayload["code"].(string)
		}
		return recorder.Code, code
	}
	for i := 0; i < 2; i++ {
		if status, code := call(); status != http.StatusBadGateway || code == "circuit_open" {
			t.Fatalf("attempt %d should hit upstream: status=%d code=%s", i, status, code)
		}
	}
	if status, code := call(); status != http.StatusServiceUnavailable || code != "circuit_open" {
		t.Fatalf("breaker should be open: status=%d code=%s", status, code)
	}
	time.Sleep(60 * time.Millisecond)
	// After reset_timeout the breaker half-opens and the probe reaches the
	// upstream again (which still fails, re-opening the circuit).
	if status, code := call(); status == http.StatusServiceUnavailable && code == "circuit_open" {
		t.Fatalf("half-open probe should reach upstream: status=%d code=%s", status, code)
	}
	if status, code := call(); status != http.StatusServiceUnavailable || code != "circuit_open" {
		t.Fatalf("breaker should re-open: status=%d code=%s", status, code)
	}
}

func TestRetryRepeatsIdempotentRequestsOnServerError(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	cfg := policyConfig(t, upstream.URL, `
defaults:
  retry:
    max_attempts: 3
`)
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPut, "/thing", nil))
	if recorder.Code != http.StatusOK || calls != 3 {
		t.Fatalf("status=%d calls=%d body=%s", recorder.Code, calls, recorder.Body.String())
	}
}

func TestRetrySkipsNonIdempotentRequests(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()
	cfg := policyConfig(t, upstream.URL, `
defaults:
  retry:
    max_attempts: 3
`)
	cfg.Routes[0].Match.Methods = []string{"POST"}
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
		t.Fatalf("POST must not be retried, calls=%d", calls)
	}
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", recorder.Code)
	}
}

func TestTokenBucketRefillsOverTime(t *testing.T) {
	bucket := newTokenBucket(10, 1)
	now := time.Now()
	if !bucket.allow(now) {
		t.Fatal("initial token missing")
	}
	if bucket.allow(now) {
		t.Fatal("burst of 1 must not allow a second token")
	}
	if !bucket.allow(now.Add(150 * time.Millisecond)) {
		t.Fatal("bucket should refill at 10 tokens/second")
	}
}

func TestShadowUsesConfiguredShadowUpstream(t *testing.T) {
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls++
		_, _ = io.WriteString(w, `{"from":"primary"}`)
	}))
	defer primary.Close()
	shadowCalls := make(chan string, 1)
	shadow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		shadowCalls <- r.Method + " " + r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer shadow.Close()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"primary": {BaseURL: primary.URL}, "mirror": {BaseURL: shadow.URL}},
		Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "transparent", TrafficMode: "shadow", Upstream: "primary", ShadowUpstream: "mirror",
			Request: transformSpec("path: /v1/thing")}},
		Routes: []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/thing", Methods: []string{"PUT"}}, Profile: "p"}},
	}
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
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPut, "/thing", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "primary") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	select {
	case request := <-shadowCalls:
		if request != "PUT /v1/thing" {
			t.Fatalf("shadow saw %q", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shadow upstream was not called")
	}
	if primaryCalls != 1 {
		t.Fatalf("primary calls=%d", primaryCalls)
	}
}

func TestExecuteUpstreamAppliesRequestTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	cfg := policyConfig(t, upstream.URL, `
defaults:
  request_timeout_ms: 50
`)
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	rt := gateway.runtimeFor("p")
	envelope, err := gateway.executeUpstream(context.Background(), cfg, cfg.Upstreams["u"], cfg.Profiles["p"], transform.Envelope{Method: http.MethodGet, Path: "/v1/thing"}, "req", rt, http.MethodGet, http.Header{})
	if err == nil {
		t.Fatalf("expected timeout error, got envelope with status %d", envelope.StatusCode)
	}
	if upstreamCode(err) != "upstream_request_failed" && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("unexpected error: %v", err)
	}
}

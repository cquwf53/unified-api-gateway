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

// TestBreakerProbeNotWedgedByRateLimit: when the half-open probe is granted
// but a later admission check rejects, the probe must be released so the
// breaker does not stick open forever.
func TestBreakerProbeNotWedgedByRateLimit(t *testing.T) {
	breaker := newBreaker(breakerConfig{failureThreshold: 1, resetTimeout: time.Millisecond})
	breaker.recordFailure(time.Now())
	time.Sleep(5 * time.Millisecond) // half-open window elapses
	now := time.Now()
	if !breaker.allow(now) {
		t.Fatal("half-open probe should be granted")
	}
	breaker.releaseProbe()
	if !breaker.allow(now) {
		t.Fatal("released probe must be re-grantable")
	}
}

func TestProbeTokenCannotReleaseNewProbe(t *testing.T) {
	breaker := newBreaker(breakerConfig{failureThreshold: 1, resetTimeout: time.Millisecond})
	breaker.recordFailure(time.Now().Add(-time.Second))
	allowed, first := breaker.allowWithProbe(time.Now())
	if !allowed || first == 0 {
		t.Fatal("first half-open probe should be granted")
	}
	breaker.recordFailure(time.Now().Add(-time.Second))
	allowed, second := breaker.allowWithProbe(time.Now())
	if !allowed || second == 0 || second == first {
		t.Fatalf("second probe not granted with a new token: first=%d second=%d", first, second)
	}
	breaker.releaseProbeToken(first)
	if breaker.allow(time.Now()) {
		t.Fatal("stale release freed the newer active probe")
	}
	breaker.releaseProbeToken(second)
	if !breaker.allow(time.Now()) {
		t.Fatal("current probe token should release the half-open slot")
	}
}

func TestWebsocketReleaseClearsUnresolvedHalfOpenProbe(t *testing.T) {
	rt := &profileRuntime{breaker: newBreaker(breakerConfig{failureThreshold: 1, resetTimeout: time.Millisecond})}
	rt.breaker.recordFailure(time.Now().Add(-time.Second))
	release, rejection := rt.beginWebsocketPolicy(context.Background(), http.MethodGet, nil, nil)
	if rejection != nil {
		t.Fatalf("probe rejected: %+v", rejection)
	}
	release()
	if !rt.breaker.allow(time.Now()) {
		t.Fatal("closing before an upstream result must release the probe")
	}
}

// TestStreamUpstreamErrorCountsAsBreakerFailure pins that a forwarded 5xx
// stream status is a breaker failure, not a success.
func TestStreamUpstreamErrorCountsAsBreakerFailure(t *testing.T) {
	rt := &profileRuntime{policy: defaultPolicySet()}
	recordStreamResult(rt, true, http.StatusInternalServerError, nil)
	if rt.breaker != nil {
		t.Fatal("no breaker configured; setup error")
	}
	// Give the runtime a real breaker and repeat.
	rt.breaker = newBreaker(breakerConfig{failureThreshold: 1})
	recordStreamResult(rt, true, http.StatusInternalServerError, nil)
	if rt.breaker.state != breakerOpen {
		t.Fatalf("5xx stream must trip the breaker, state=%d", rt.breaker.state)
	}
	rt2 := &profileRuntime{breaker: newBreaker(breakerConfig{failureThreshold: 1})}
	recordStreamResult(rt2, true, http.StatusOK, nil)
	if rt2.breaker.state != breakerClosed || rt2.breaker.failures != 0 {
		t.Fatalf("2xx stream must be a clean success, state=%d failures=%d", rt2.breaker.state, rt2.breaker.failures)
	}
}

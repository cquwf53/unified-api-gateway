package server

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/store"
	"unified-api-gateway/internal/transform"
)

func policyRegressionGateway(t *testing.T, target, mode string, change transform.TransformSpec, policies map[string]any) *Server {
	t.Helper()
	t.Setenv("GATEWAY_CLIENT_API_KEY", "")
	cfg := &config.Config{Version: 1, DefaultsProfile: "p",
		Upstreams: map[string]config.Upstream{"u": {BaseURL: target, TimeoutMS: 1000}},
		Profiles:  map[string]config.Profile{"p": {Name: "p", Adapter: "transparent", TrafficMode: mode, Upstream: "u", Request: change}},
		Policies:  policies,
	}
	if mode == "record" {
		cfg.Observability.RecordBody = true
	}
	snap, err := config.NewSnapshot(cfg, "quality-audit")
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Options{Snapshot: snap})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestRetriesUseOutboundSemantics(t *testing.T) {
	for _, test := range []struct {
		name, method, key string
		change            transform.TransformSpec
	}{
		{"GET rewritten to POST", "GET", "", transform.TransformSpec{Method: "POST"}},
		{"idempotency key removed", "POST", "once-only", transform.TransformSpec{Headers: transform.HeaderSpec{Delete: []string{"Idempotency-Key"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				t.Logf("upstream call method=%s idempotency_key=%q", r.Method, r.Header.Get("Idempotency-Key"))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(500)
				_, _ = w.Write([]byte(`{"error":"failed after side effect"}`))
			}))
			defer upstream.Close()
			s := policyRegressionGateway(t, upstream.URL, "live", test.change, map[string]any{"defaults": map[string]any{"retry": map[string]any{"max_attempts": 2, "idempotent_only": true}}})
			r := httptest.NewRequest(test.method, "/side-effect", nil)
			if test.key != "" {
				r.Header.Set("Idempotency-Key", test.key)
			}
			w := httptest.NewRecorder()
			s.ServeDataHTTP(w, r)
			if calls.Load() != 1 {
				t.Errorf("non-idempotent outbound POST was executed %d times (client status %d)", calls.Load(), w.Code)
			}
		})
	}
}

func TestNonLiveModesNeverContactUpstreamOnOtherPlanes(t *testing.T) {
	for _, plane := range []string{"grpc", "websocket"} {
		for _, mode := range []string{"dry-run", "mock", "replay", "shadow", "record"} {
			t.Run(plane+"/"+mode, func(t *testing.T) {
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Content-Type", "application/grpc")
					w.WriteHeader(400)
				}))
				defer upstream.Close()
				s := policyRegressionGateway(t, upstream.URL, mode, transform.TransformSpec{}, nil)
				r := httptest.NewRequest("POST", "/svc/Call", strings.NewReader("frame"))
				w := httptest.NewRecorder()
				if plane == "grpc" {
					r.Header.Set("Content-Type", "application/grpc")
					s.ServeGRPCHTTP(w, r)
				} else {
					r.Method = "GET"
					r.Header.Set("Connection", "Upgrade")
					r.Header.Set("Upgrade", "websocket")
					r.Header.Set("Sec-WebSocket-Version", "13")
					r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
					s.ServeDataHTTP(w, r)
				}
				if calls.Load() != 0 {
					t.Errorf("%s %s contacted upstream %d times; status=%d", plane, mode, calls.Load(), w.Code)
				}
			})
		}
	}
}

func TestActivationPreservesConcurrencyAdmission(t *testing.T) {
	s := policyRegressionGateway(t, "http://127.0.0.1:1", "live", transform.TransformSpec{}, map[string]any{"defaults": map[string]any{"max_concurrency": 1}})
	before := s.state.Load()
	release, rejection := before.runtimes["p"].beginPolicy(context.Background(), "GET", nil, true)
	if rejection != nil {
		t.Fatal(rejection)
	}
	defer release()
	cfg := s.Snapshot().Config.Clone()
	cfg.Observability.LogLevel = "debug"
	next, err := config.NewSnapshot(cfg, "unrelated-log-change")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Activate(next); err != nil {
		t.Fatal(err)
	}
	extraRelease, second := s.state.Load().runtimes["p"].beginPolicy(context.Background(), "GET", nil, true)
	defer extraRelease()
	if second == nil {
		t.Error("hot activation admitted another request while the same profile concurrency limit was occupied")
	}
}

type unreadRequestBody struct{ reads int }

func (b *unreadRequestBody) Read(p []byte) (int, error) { b.reads++; return 0, io.EOF }
func (b *unreadRequestBody) Close() error               { return nil }

func TestUnauthorizedRequestBodyIsNotConsumed(t *testing.T) {
	s := policyRegressionGateway(t, "http://127.0.0.1:1", "live", transform.TransformSpec{}, nil)
	t.Setenv("GATEWAY_CLIENT_API_KEY", "required-key")
	body := &unreadRequestBody{}
	r := httptest.NewRequest("POST", "/x", nil)
	r.Body = body
	w := httptest.NewRecorder()
	s.ServeDataHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("want auth rejection, got %d", w.Code)
	}
	if body.reads != 0 {
		t.Errorf("unauthorized request body read %d times before auth rejection", body.reads)
	}
}

func TestGRPCPolicyTimeoutApplies(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		w.Header().Set("Content-Type", "application/grpc")
		_, _ = w.Write([]byte("frame"))
	}))
	defer upstream.Close()
	s := policyRegressionGateway(t, upstream.URL, "live", transform.TransformSpec{}, map[string]any{"defaults": map[string]any{"request_timeout_ms": 10}})
	r := httptest.NewRequest("POST", "/svc/Call", strings.NewReader("frame"))
	r.Header.Set("Content-Type", "application/grpc")
	w := httptest.NewRecorder()
	start := time.Now()
	s.ServeGRPCHTTP(w, r)
	if w.Code == 200 {
		t.Errorf("10ms nonstreaming policy timeout ignored; status=%d elapsed=%v", w.Code, time.Since(start))
	}
}
func TestSemanticSSEErrorsTripBreaker(t *testing.T) {
	for _, test := range []struct{ name, body string }{
		{"buffered", `{"model":"test","messages":[{"role":"user","content":"hello"}],"stream":false}`},
		{"streaming", `{"model":"test","messages":[{"role":"user","content":"hello"}],"stream":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"capacity exhausted\"}}\n\n"))
			}))
			defer upstream.Close()
			s := policyRegressionGateway(t, upstream.URL, "live", transform.TransformSpec{}, map[string]any{"defaults": map[string]any{"circuit_breaker": map[string]any{"failure_threshold": 1, "reset_timeout_ms": 60000}}})
			cfg := s.Snapshot().Config.Clone()
			p := cfg.Profiles["p"]
			p.Adapter = "openai-chat-to-anthropic"
			p.ClientContract = "openai-chat"
			p.UpstreamContract = "anthropic-messages"
			cfg.Profiles["p"] = p
			snap, err := config.NewSnapshot(cfg, "semantic")
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Activate(snap); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(test.body))
				r.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				s.ServeDataHTTP(w, r)
				t.Logf("request=%d status=%d response=%s", i+1, w.Code, w.Body.String())
			}
			metrics := s.Metrics()
			t.Logf("upstream_calls=%d successful=%d failed=%d", calls.Load(), metrics.RequestsOK, metrics.RequestsFailed)
			if calls.Load() != 1 {
				t.Errorf("semantic upstream errors did not open breaker; called %d times", calls.Load())
			}
			if metrics.RequestsOK != 0 {
				t.Errorf("semantic stream errors recorded as %d successes", metrics.RequestsOK)
			}
		})
	}
}

func TestActivationPreservesRateTokens(t *testing.T) {
	s := policyRegressionGateway(t, "http://127.0.0.1:1", "live", transform.TransformSpec{}, map[string]any{"defaults": map[string]any{"requests_per_second": 0.001, "burst": 1}})
	release, rejection := s.runtimeFor("p").beginPolicy(context.Background(), "GET", nil, true)
	if rejection != nil {
		t.Fatal(rejection)
	}
	release()
	cfg := s.Snapshot().Config
	cfg.Observability.LogLevel = "debug"
	next, err := config.NewSnapshot(cfg, "unrelated-change")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Activate(next); err != nil {
		t.Fatal(err)
	}
	release, rejection = s.runtimeFor("p").beginPolicy(context.Background(), "GET", nil, true)
	defer release()
	if rejection == nil || rejection.code != "rate_limited" {
		t.Fatalf("activation refilled the consumed token: %v", rejection)
	}
}

func TestAdmissionTracksOldRequestsWhenLimitsChange(t *testing.T) {
	s := policyRegressionGateway(t, "http://127.0.0.1:1", "live", transform.TransformSpec{}, nil)
	first, err1 := s.runtimeFor("p").beginPolicy(context.Background(), "GET", nil, true)
	if err1 != nil {
		t.Fatal(err1)
	}
	defer first()
	cfg := s.Snapshot().Config
	cfg.Policies = map[string]any{"defaults": map[string]any{"max_concurrency": 1}}
	next, err := config.NewSnapshot(cfg, "enable-limit")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Activate(next); err != nil {
		t.Fatal(err)
	}
	release, rejection := s.runtimeFor("p").beginPolicy(context.Background(), "GET", nil, true)
	release()
	if rejection == nil || rejection.code != "concurrency_limit" {
		t.Fatalf("new limit ignored existing request: %v", rejection)
	}
	first()
	release, rejection = s.runtimeFor("p").beginPolicy(context.Background(), "GET", nil, true)
	defer release()
	if rejection != nil {
		t.Fatalf("old request release did not free budget: %v", rejection)
	}
}

func TestIngressBudgetReleaseIsIdempotent(t *testing.T) {
	s := policyRegressionGateway(t, "http://127.0.0.1:1", "live", transform.TransformSpec{}, map[string]any{"defaults": map[string]any{"max_concurrency": 1}})
	release, rejection := s.beginIngress(s.state.Load())
	if rejection != nil {
		t.Fatal(rejection)
	}
	_, second := s.beginIngress(s.state.Load())
	if second == nil || second.code != "ingress_concurrency_limit" {
		t.Fatalf("second parser bypassed budget: %v", second)
	}
	release()
	release()
	if got := s.ingressActive.Load(); got != 0 {
		t.Fatalf("parser count after release=%d", got)
	}
	final, rejection := s.beginIngress(s.state.Load())
	defer final()
	if rejection != nil {
		t.Fatal(rejection)
	}
}

func TestConnectionNominatedIdempotencyKeyDoesNotEnableRetry(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Idempotency-Key") != "" {
			t.Error("hop-by-hop nominated key reached upstream")
		}
		w.WriteHeader(500)
	}))
	defer upstream.Close()
	s := policyRegressionGateway(t, upstream.URL, "live", transform.TransformSpec{}, map[string]any{"defaults": map[string]any{"retry": map[string]any{"max_attempts": 2, "idempotent_only": true}}})
	request := httptest.NewRequest("POST", "/x", nil)
	request.Header.Set("Idempotency-Key", "unsafe")
	request.Header.Set("Connection", "Idempotency-Key")
	s.ServeDataHTTP(httptest.NewRecorder(), request)
	if got := calls.Load(); got != 1 {
		t.Fatalf("POST retried without outbound key: %d calls", got)
	}
}

func TestTransparentPlanesRejectUnsupportedTransforms(t *testing.T) {
	for _, plane := range []string{"grpc", "websocket"} {
		t.Run(plane, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(400) }))
			defer upstream.Close()
			s := policyRegressionGateway(t, upstream.URL, "live", transform.TransformSpec{Path: "/rewritten"}, nil)
			request := httptest.NewRequest("POST", "/original", nil)
			response := httptest.NewRecorder()
			if plane == "grpc" {
				request.Header.Set("Content-Type", "application/grpc")
				s.ServeGRPCHTTP(response, request)
			} else {
				request.Method = "GET"
				request.Header.Set("Connection", "Upgrade")
				request.Header.Set("Upgrade", "websocket")
				request.Header.Set("Sec-WebSocket-Version", "13")
				request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
				s.ServeDataHTTP(response, request)
			}
			if response.Code != 501 || calls.Load() != 0 {
				t.Fatalf("unsupported transforms were silently skipped: status=%d calls=%d", response.Code, calls.Load())
			}
		})
	}
}

func TestFailedActivationDoesNotRefillRateTokens(t *testing.T) {
	seed := policyRegressionGateway(t, "http://127.0.0.1:1", "live", transform.TransformSpec{}, map[string]any{"defaults": map[string]any{"requests_per_second": 0.001, "burst": 1}}).Snapshot()
	dbPath := t.TempDir() + "/history.db"
	history, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	s, err := New(Options{Snapshot: seed, Store: history})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Activate(seed); err != nil {
		t.Fatal(err)
	}
	release, rejection := s.runtimeFor("p").beginPolicy(context.Background(), "GET", nil, true)
	if rejection != nil {
		t.Fatal(rejection)
	}
	release()
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`CREATE TRIGGER fail_activation BEFORE UPDATE OF active ON config_versions WHEN NEW.active = 1 BEGIN SELECT RAISE(FAIL, 'planned activation failure'); END;`); err != nil {
		t.Fatal(err)
	}
	cfg := seed.Config.Clone()
	cfg.Policies = map[string]any{}
	candidate, err := config.NewSnapshot(cfg, "disable-limiter")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Activate(candidate); err == nil {
		t.Fatal("activation must fail")
	}
	release, rejection = s.runtimeFor("p").beginPolicy(context.Background(), "GET", nil, true)
	defer release()
	if rejection == nil || rejection.code != "rate_limited" {
		t.Fatalf("failed activation refilled rate tokens: %v", rejection)
	}
	active, err := history.Active(context.Background())
	if err != nil || active.ID != seed.ID {
		t.Fatalf("active history changed: %v, %v", active.ID, err)
	}
}

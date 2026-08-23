package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
)

// TestUpstreamErrorBecomesProtocolErrorEnvelope pins that a 4xx/5xx upstream
// body is never adapted into a fake "successful" completion: the client gets
// its own protocol's error shape with the upstream message verbatim.
func TestUpstreamErrorBecomesProtocolErrorEnvelope(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"authentication_error","message":"bad key"}}`))
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "live", Upstream: "u"},
	}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", recorder.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	errPayload, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected an OpenAI error envelope, got %s", recorder.Body.String())
	}
	if errPayload["message"] != "bad key" {
		t.Fatalf("upstream message lost: %#v", errPayload)
	}
	if _, has := payload["choices"]; has {
		t.Fatalf("error must not look like a completion: %s", recorder.Body.String())
	}
}

func TestUpstreamErrorAnthropicClientEnvelope(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"upstream exploded"}}`))
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "anthropic-to-openai-chat", ClientContract: "anthropic-messages", UpstreamContract: "openai-chat", TrafficMode: "live", Upstream: "u"},
	}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/messages"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":8}`)))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", recorder.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["type"] != "error" {
		t.Fatalf("expected an Anthropic error envelope, got %s", recorder.Body.String())
	}
	if payload["error"].(map[string]any)["message"] != "upstream exploded" {
		t.Fatalf("upstream message lost: %s", recorder.Body.String())
	}
}

// TestNativeOpenAIErrorPassesThroughUnchanged pins that an upstream error
// already speaking the client's vocabulary is forwarded verbatim: rewriting
// it would collapse provider error codes (rate_limit_exceeded, ...) that
// retry middleware branches on.
func TestNativeOpenAIErrorPassesThroughUnchanged(t *testing.T) {
	upstreamBody := `{"error":{"message":"Rate limit reached","type":"tokens","param":null,"code":"rate_limit_exceeded"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()
	// The vercel-zai-glm-5-2 shape: transparent adapter, openai-chat client.
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", ClientContract: "openai-chat", UpstreamContract: "openai-chat", TrafficMode: "live", Upstream: "u"},
	}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)))
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d", recorder.Code)
	}
	if strings.TrimSpace(recorder.Body.String()) != upstreamBody {
		t.Fatalf("native error envelope was rewritten:\n got %s\nwant %s", recorder.Body.String(), upstreamBody)
	}
}

// TestErrorResponsesStillRunResponseTransforms pins that status_map and
// header rules apply to error responses - remapping upstream errors is their
// main use case.
func TestErrorResponsesStillRunResponseTransforms(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", ClientContract: "openai-chat", TrafficMode: "live", Upstream: "u",
			Response: transformSpec(`
status_map:
  500: 503
headers:
  set:
    Retry-After: "5"
`)},
	}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status_map did not remap the error: %d", recorder.Code)
	}
	if recorder.Header().Get("Retry-After") != "5" {
		t.Fatalf("header rule skipped on error response")
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["error"].(map[string]any)["message"] != "boom" {
		t.Fatalf("error message lost: %s", recorder.Body.String())
	}
}

// TestUnknownAdapterRejectedAtValidation closes the gap where a typo'd
// adapter name passed every precheck and only failed at request time.
func TestUnknownAdapterRejectedAtValidation(t *testing.T) {
	_, err := config.Parse([]byte(`
version: 1
profiles:
  p:
    adapter: openai-chat-to-anthropik
    upstream: u
upstreams:
  u:
    base_url: http://127.0.0.1:1
`))
	if err == nil {
		t.Fatal("unknown adapter accepted")
	}
	if !strings.Contains(err.Error(), "openai-chat-to-anthropik") {
		t.Fatalf("error should name the bad adapter: %v", err)
	}
}

// TestModelsEndpointRequiresClientAuth closes the profile-enumeration hole:
// /v1/models lists every profile and must honour client authentication.
func TestModelsEndpointRequiresClientAuth(t *testing.T) {
	t.Setenv("GATEWAY_MODELS_TEST_KEY", "secret-key")
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", TrafficMode: "dry-run"},
	}, Clients: map[string]config.ClientCredential{"local": {KeyEnv: "GATEWAY_MODELS_TEST_KEY"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	anonymous := httptest.NewRecorder()
	gateway.ServeDataHTTP(anonymous, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if anonymous.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous status=%d", anonymous.Code)
	}
	authenticated := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("X-API-Key", "secret-key")
	gateway.ServeDataHTTP(authenticated, request)
	if authenticated.Code != http.StatusOK {
		t.Fatalf("authenticated status=%d body=%s", authenticated.Code, authenticated.Body.String())
	}
}

// TestDryRunStreamUsesSeparateFinishChunk verifies the synthetic dry-run
// stream keeps content and finish_reason in separate chunks.
func TestDryRunStreamUsesSeparateFinishChunk(t *testing.T) {
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "transparent", ClientContract: "openai-chat", TrafficMode: "dry-run"},
	}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)))
	body := recorder.Body.String()
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: ") || strings.TrimSpace(line) == "data: [DONE]" {
			continue
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &chunk); err != nil {
			t.Fatalf("bad chunk %q: %v", line, err)
		}
		choice := chunk["choices"].([]any)[0].(map[string]any)
		delta, _ := choice["delta"].(map[string]any)
		content, hasContent := delta["content"]
		if choice["finish_reason"] != nil && hasContent && content != "" {
			t.Fatalf("content and finish_reason share a chunk: %s", line)
		}
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("missing sentinel: %s", body)
	}
}

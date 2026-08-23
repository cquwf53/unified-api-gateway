package server

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/store"
	"unified-api-gateway/internal/transform"
)

func TestGenericRewriteRoundTrip(t *testing.T) {
	var receivedPath string
	var receivedHeader string
	var receivedBody map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		receivedHeader = r.Header.Get("X-Api-Version")
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &receivedBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":42,"displayName":"Alice","extra":"kept"}`)
	}))
	defer upstream.Close()

	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"customer": {BaseURL: upstream.URL}},
		Profiles: map[string]config.Profile{
			"rewrite": {
				Name: "rewrite", Adapter: "transparent", TrafficMode: "live", Upstream: "customer",
				Request: transformSpec(`
path: /v2/customers
headers:
  set:
    X-Api-Version: "2"
body:
  rename:
    user_id: id
    name: displayName
`),
				Response: transformSpec(`
body:
  rename:
    id: user_id
    displayName: name
`),
			},
		},
		Routes: []config.Route{{ID: "users", Priority: 10, Match: config.RouteMatch{Path: "/api/v1/users", Methods: []string{"POST"}}, Profile: "rewrite"}},
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
	request := httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(`{"user_id":7,"name":"Alice"}`))
	request.Header.Set("Content-Type", "application/json")
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if receivedPath != "/v2/customers" || receivedHeader != "2" {
		t.Fatalf("upstream request path/header = %q/%q", receivedPath, receivedHeader)
	}
	if receivedBody["id"] != float64(7) || receivedBody["displayName"] != "Alice" {
		t.Fatalf("upstream body = %#v", receivedBody)
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["user_id"] != float64(42) || response["name"] != "Alice" || response["extra"] != "kept" {
		t.Fatalf("client response = %#v", response)
	}
}

func TestOpenAIAnthropicRoundTrip(t *testing.T) {
	var receivedPath string
	var received map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &received)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"claude","content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1}}`)
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"anthropic": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "live", Upstream: "anthropic", Request: transformSpec(`path: /v1/messages`)}}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"claude","messages":[{"role":"user","content":"ping"}],"max_tokens":8}`))
	request.Header.Set("Content-Type", "application/json")
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusOK || receivedPath != "/v1/messages" {
		t.Fatalf("status=%d path=%s body=%s", recorder.Code, receivedPath, recorder.Body.String())
	}
	if received["max_tokens"] != float64(8) || len(received["messages"].([]any)) != 1 {
		t.Fatalf("upstream body=%#v", received)
	}
	var output map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["object"] != "chat.completion" || output["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)["content"] != "pong" {
		t.Fatalf("client output=%#v", output)
	}
}

func TestSSEResponseIsTranslated(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"pong\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"anthropic": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "live", Upstream: "anthropic", StreamSafe: &[]bool{true}[0]}}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"claude","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	request.Header.Set("Accept", "text/event-stream")
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Header().Get("Content-Type"), "text/event-stream") || !strings.Contains(recorder.Body.String(), "chat.completion.chunk") {
		t.Fatalf("status=%d content-type=%s body=%s", recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String())
	}
}

func TestSSEIsAggregatedForNonStreamingClient(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"pong\"}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n")
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"anthropic": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "live", Upstream: "anthropic"}}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"claude","messages":[{"role":"user","content":"ping"}]}`))
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusOK || strings.Contains(recorder.Header().Get("Content-Type"), "event-stream") {
		t.Fatalf("status=%d content-type=%s body=%s", recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String())
	}
	var output map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)["content"] != "pong" {
		t.Fatalf("output=%#v", output)
	}
}

func TestTransparentStreamIsForwardedWithoutBufferingTransform(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "transparent", ClientContract: "openai-chat", UpstreamContract: "openai-chat", TrafficMode: "live", Upstream: "u", StreamSafe: &[]bool{true}[0]}}, Routes: []config.Route{{ID: "stream", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Header().Get("Content-Type"), "text/event-stream") || !strings.Contains(recorder.Body.String(), "[DONE]") {
		t.Fatalf("status=%d content-type=%s body=%s", recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String())
	}
}

func TestDryRunDoesNotCallUpstream(t *testing.T) {
	called := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer upstream.Close()
	cfg := &config.Config{
		Version:   1,
		Upstreams: map[string]config.Upstream{"remote": {BaseURL: upstream.URL}},
		Profiles:  map[string]config.Profile{"preview": {Name: "preview", Adapter: "transparent", TrafficMode: "dry-run", Upstream: "remote", Request: transformSpec(`path: /rewritten`)}},
		Routes:    []config.Route{{ID: "preview", Match: config.RouteMatch{Path: "/input"}, Profile: "preview"}},
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
	gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/input", strings.NewReader(`{"ok":true}`)))
	if recorder.Code != http.StatusOK || called {
		t.Fatalf("dry-run status=%d called=%v body=%s", recorder.Code, called, recorder.Body.String())
	}
	var preview map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	requestPreview, _ := preview["request"].(map[string]any)
	if requestPreview["path"] != "/rewritten" {
		t.Fatalf("preview path=%v", requestPreview["path"])
	}
}

func TestProfileOverrideRequiresAllowlist(t *testing.T) {
	cfg := &config.Config{
		Version:         1,
		DefaultsProfile: "default",
		Profiles: map[string]config.Profile{
			"default": {Name: "default", Adapter: "transparent", TrafficMode: "dry-run"},
			"secret":  {Name: "secret", Adapter: "transparent", TrafficMode: "dry-run"},
		},
		Clients: map[string]config.ClientCredential{
			"client": {Key: "test-key", Profiles: []string{"default"}},
		},
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
	request := httptest.NewRequest(http.MethodPost, "/anything", strings.NewReader(`{}`))
	request.Header.Set("X-API-Key", "test-key")
	request.Header.Set("X-Gateway-Profile", "secret")
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestLocalAnonymousProfileOverrideWhenNoKeyConfigured(t *testing.T) {
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"preview": {Name: "preview", Adapter: "transparent", TrafficMode: "dry-run"},
	}, Clients: map[string]config.ClientCredential{"local": {KeyRef: "env:UNSET_GATEWAY_TEST_KEY"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/anything", strings.NewReader(`{"hello":"world"}`))
	request.Header.Set("X-Gateway-Profile", "preview")
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestPrepareRequestAppliesProtocolAdapter(t *testing.T) {
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "dry-run"},
	}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	out, err := gateway.prepareRequest(context.Background(), cfg, cfg.Profiles["p"], transform.Envelope{Method: "POST", Path: "/v1/chat/completions", Headers: http.Header{"Content-Type": []string{"application/json"}}, Body: []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(out.Body, &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["messages"].([]any); !ok || body["max_tokens"] == nil {
		t.Fatalf("adapter did not run: %#v", body)
	}
}

func TestPreviewHandlerShowsAdaptedBody(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	cfg, err := config.LoadFile("../../configs/gateway.yaml")
	if err != nil {
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
	payload := `{"profile":"openai-to-anthropic-dry-run","method":"POST","path":"/v1/chat/completions","headers":{"Content-Type":["application/json"]},"body":{"model":"m","messages":[{"role":"user","content":"hi"}]}}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/preview", strings.NewReader(payload))
	request.Header.Set("X-Admin-Token", "test-admin")
	gateway.ServeAdminHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var output map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	previewRequest := output["request"].(map[string]any)
	body := previewRequest["body"].(map[string]any)
	if body["max_tokens"] == nil {
		t.Fatalf("preview body was not adapted: %#v", body)
	}
}

func TestRecordAndReplay(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"value":"from-upstream"}`)
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Observability: config.Observability{RecordBody: true}, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{"record": {Name: "record", Adapter: "transparent", TrafficMode: "record", Upstream: "u"}}, Routes: []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/record"}, Profile: "record"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gateway, err := New(Options{Snapshot: snapshot, Store: db})
	if err != nil {
		t.Fatal(err)
	}
	call := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		gateway.ServeDataHTTP(recorder, httptest.NewRequest(http.MethodPost, "/record", strings.NewReader(`{"hello":"world"}`)))
		return recorder
	}
	first := call()
	if first.Code != http.StatusOK || calls != 1 {
		t.Fatalf("record status=%d calls=%d body=%s", first.Code, calls, first.Body.String())
	}
	cfg.Profiles["record"] = config.Profile{Name: "record", Adapter: "transparent", TrafficMode: "replay", Upstream: "u"}
	replaySnapshot, err := config.NewSnapshot(cfg, "replay")
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.Activate(replaySnapshot); err != nil {
		t.Fatal(err)
	}
	second := call()
	if second.Code != http.StatusOK || calls != 1 || second.Body.String() != first.Body.String() {
		t.Fatalf("replay status=%d calls=%d first=%s second=%s", second.Code, calls, first.Body.String(), second.Body.String())
	}
}

func TestQuickSwitchPersistsNewSnapshot(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{
		"one": {Name: "one", Adapter: "transparent", TrafficMode: "dry-run"},
		"two": {Name: "two", Adapter: "transparent", TrafficMode: "dry-run"},
	}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/chat"}, Profile: "one"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gateway, err := New(Options{Snapshot: snapshot, Store: db})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/switch", strings.NewReader(`{"route_id":"chat","profile":"two"}`))
	request.Header.Set("X-Admin-Token", "test-admin")
	gateway.ServeAdminHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if gateway.Snapshot().Config.Routes[0].Profile != "two" {
		t.Fatalf("route was not switched: %#v", gateway.Snapshot().Config.Routes)
	}
	version, err := db.GetVersion(context.Background(), gateway.Snapshot().ID)
	if err != nil || len(version.YAML) == 0 {
		t.Fatalf("quick switch did not persist YAML: %v", err)
	}
}

func TestTranslatedStreamingIsIncremental(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\n")
		io.WriteString(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"first \"}}\n\n")
		flusher.Flush()
		<-release
		io.WriteString(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"second\"}}\n\n")
		io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "live", Upstream: "u", StreamSafe: &[]bool{true}[0]}}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
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
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if !strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("content-type=%s", response.Header.Get("Content-Type"))
	}
	reader := bufio.NewReader(response.Body)
	readLine := func() string {
		if deadliner, ok := response.Body.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = deadliner.SetReadDeadline(time.Now().Add(5 * time.Second))
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read line: %v", err)
		}
		return strings.TrimRight(line, "\r\n")
	}
	var collected strings.Builder
	for {
		line := readLine()
		collected.WriteString(line)
		if strings.Contains(line, "first ") {
			break
		}
		if !strings.Contains(collected.String(), "chat.completion.chunk") && line == "" {
			t.Fatalf("stream ended before first frame: %s", collected.String())
		}
	}
	// The first frame must arrive while the upstream is still holding the
	// second half of the response; if it were buffered this select hangs.
	select {
	case <-release:
		t.Fatal("upstream released early; test setup is wrong")
	default:
	}
	close(release)
	var full strings.Builder
	full.WriteString(collected.String())
	if deadliner, ok := response.Body.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = deadliner.SetReadDeadline(time.Now().Add(5 * time.Second))
	}
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	full.Write(rest)
	if !strings.Contains(full.String(), "second") || !strings.Contains(full.String(), "chat.completion.chunk") {
		t.Fatalf("stream body=%s", full.String())
	}
}

func TestTranslatedStreamingFallsBackOnUpstreamError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"type":"error","error":{"type":"authentication_error","message":"bad key"}}`)
	}))
	defer upstream.Close()
	cfg := &config.Config{Version: 1, Upstreams: map[string]config.Upstream{"u": {BaseURL: upstream.URL}}, Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "openai-chat-to-anthropic", ClientContract: "openai-chat", UpstreamContract: "anthropic-messages", TrafficMode: "live", Upstream: "u", StreamSafe: &[]bool{true}[0]}}, Routes: []config.Route{{ID: "chat", Match: config.RouteMatch{Path: "/v1/chat/completions"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"claude","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	gateway.ServeDataHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Header().Get("Content-Type"), "event-stream") {
		t.Fatalf("error fallback should not be a stream: %s", recorder.Header().Get("Content-Type"))
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("fallback body is not JSON: %s", recorder.Body.String())
	}
}

func transformSpec(value string) transform.TransformSpec {
	var spec transform.TransformSpec
	if err := yaml.Unmarshal([]byte(value), &spec); err != nil {
		panic(err)
	}
	return spec
}

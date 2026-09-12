package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unified-api-gateway/internal/adapter"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

func TestQualityBufferedGatewayParallelCalls(t *testing.T) {
	s := &Server{registry: adapter.NewRegistry()}
	body := []byte(`{"id":"c1","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_a","type":"function","function":{"name":"a","arguments":"{}"}},{"id":"call_b","type":"function","function":{"name":"b","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`)
	env := transform.Envelope{StatusCode: 200, Headers: http.Header{"Content-Type": {"application/json"}}, ContentType: "application/json", Body: body, Metadata: map[string]any{"client_stream": true}}
	profile := config.Profile{Adapter: "transparent", ClientContract: "openai-chat", UpstreamContract: "openai-chat"}
	out, err := s.prepareResponse(t.Context(), nil, &config.Config{}, profile, env)
	if err != nil {
		t.Fatal(err)
	}
	events, err := adapter.ParseSSE(strings.NewReader(string(out.Body)))
	if err != nil {
		t.Fatal(err)
	}
	var chunk map[string]any
	if err := json.Unmarshal([]byte(events[0].Data), &chunk); err != nil {
		t.Fatal(err)
	}
	calls := chunk["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)["tool_calls"].([]any)
	for i, call := range calls {
		if call.(map[string]any)["index"] != float64(i) {
			t.Errorf("gateway emitted unaddressed call %d: %s", i, out.Body)
		}
	}
}

func TestTransparentSSESemanticErrorsRemainFailures(t *testing.T) {
	for _, contract := range []string{"openai-chat", "anthropic-messages", "openai-responses", "generic-http"} {
		t.Run(contract, func(t *testing.T) {
			payload := ": keep this comment\n\ndata: {\"error\":{\"type\":\"server_error\",\"message\":\"provider failed\"}}\n\n"
			if contract == "anthropic-messages" {
				payload = ": keep this comment\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"provider failed\"}}\n\n"
			}
			if contract == "openai-responses" {
				payload = ": keep this comment\n\nevent: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"provider failed\"}}}\n\n"
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(payload))
			}))
			defer upstream.Close()
			s := &Server{client: upstream.Client(), registry: adapter.NewRegistry()}
			w := httptest.NewRecorder()
			_, sent, err := s.streamUpstream(t.Context(), nil, &config.Config{}, config.Upstream{URL: upstream.URL}, config.Profile{Adapter: "transparent", UpstreamContract: contract, ClientContract: contract}, transform.Envelope{Method: "POST", Path: "/", Headers: make(http.Header)}, "semantic", w)
			if !sent || w.Body.String() != payload {
				t.Fatalf("transparent bytes changed: got=%q want=%q", w.Body.String(), payload)
			}
			if contract == "generic-http" {
				if err != nil {
					t.Fatalf("generic content interpreted as LLM error: %v", err)
				}
			} else if err == nil || upstreamCode(err) != "upstream_stream_error" {
				t.Fatalf("semantic failure counted as success: %v", err)
			}
		})
	}
}

func TestStreamJSONFallbackRetainsUpstreamFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"error":{"message":"provider failed"}}`))
	}))
	defer upstream.Close()
	profile := config.Profile{Adapter: "transparent", UpstreamContract: "openai-chat", ClientContract: "openai-chat", Response: transform.TransformSpec{StatusMap: map[int]int{503: 200}}}
	for _, translated := range []bool{false, true} {
		s := &Server{client: upstream.Client(), registry: adapter.NewRegistry()}
		out := httptest.NewRecorder()
		var status int
		var sent bool
		var err error
		if translated {
			status, sent, err = s.streamTranslatedUpstream(t.Context(), nil, &config.Config{}, config.Upstream{URL: upstream.URL}, profile, transform.Envelope{Method: "POST", Path: "/", Headers: make(http.Header)}, "fallback", out)
		} else {
			status, sent, err = s.streamUpstream(t.Context(), nil, &config.Config{}, config.Upstream{URL: upstream.URL}, profile, transform.Envelope{Method: "POST", Path: "/", Headers: make(http.Header)}, "fallback", out)
		}
		if status != 200 || !sent || err == nil {
			t.Fatalf("translated=%v: status transform disguised upstream failure: status=%d sent=%v err=%v", translated, status, sent, err)
		}
		if strings.Count(out.Body.String(), "provider failed") != 1 {
			t.Fatalf("duplicate/missing response: %s", out.Body.String())
		}
	}
}

func TestBufferedSemanticFailureSurvivesStatusMapping(t *testing.T) {
	s := &Server{registry: adapter.NewRegistry()}
	for _, contract := range []string{"openai-chat", "anthropic-messages", "openai-responses"} {
		profile := config.Profile{Adapter: "transparent", UpstreamContract: "anthropic-messages", ClientContract: contract, Response: transform.TransformSpec{StatusMap: map[int]int{502: 200}}}
		in := transform.Envelope{StatusCode: 200, Headers: http.Header{"Content-Type": {"text/event-stream"}}, ContentType: "text/event-stream", Body: []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"provider failed\"}}\n\n")}
		out, err := s.prepareResponse(t.Context(), nil, &config.Config{}, profile, in)
		if err != nil {
			t.Fatal(err)
		}
		if failed, _ := out.Metadata["upstream_failed"].(bool); !failed || out.StatusCode != 200 {
			t.Fatalf("%s: lost failure marker on transformed error: %+v", contract, out)
		}
	}
}

func TestTranslatedEOFOverflowIsRecordedAsFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for index, size := range []int{2 << 20, (2 << 20) + 1} {
			call := map[string]any{"index": 0, "function": map[string]any{"arguments": strings.Repeat("x", size)}}
			if index == 0 {
				call["id"] = "t"
				call["function"].(map[string]any)["name"] = "f"
			}
			chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"tool_calls": []any{call}}}}})
			_, _ = w.Write(append(append([]byte("data: "), chunk...), []byte("\n\n")...))
		}
	}))
	defer upstream.Close()
	s := &Server{client: upstream.Client(), registry: adapter.NewRegistry()}
	out := httptest.NewRecorder()
	_, _, err := s.streamTranslatedUpstream(t.Context(), nil, &config.Config{}, config.Upstream{URL: upstream.URL}, config.Profile{Adapter: "anthropic-to-openai-chat", ClientContract: "anthropic-messages", UpstreamContract: "openai-chat"}, transform.Envelope{Method: "POST", Path: "/", Headers: make(http.Header)}, "overflow", out)
	if err == nil || upstreamCode(err) != "upstream_stream_error" {
		t.Fatalf("EOF-generated overflow counted as success: %v", err)
	}
	if strings.Count(out.Body.String(), "event: error") != 1 || strings.Contains(out.Body.String(), "message_stop") {
		t.Fatalf("overflow terminal was duplicated or followed by success: %s", out.Body.String())
	}
}

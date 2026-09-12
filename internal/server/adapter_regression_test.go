package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unified-api-gateway/internal/adapter"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

func TestBufferedAdapterErrorsRemainHTTPFailures(t *testing.T) {
	s := &Server{registry: adapter.NewRegistry()}
	for _, contract := range []string{"openai-chat", "anthropic-messages", "openai-responses"} {
		t.Run(contract, func(t *testing.T) {
			profile := config.Profile{Adapter: "transparent", ClientContract: contract, UpstreamContract: "anthropic-messages"}
			in := transform.Envelope{StatusCode: http.StatusOK, Headers: http.Header{"Content-Type": {"text/event-stream"}}, ContentType: "text/event-stream", Body: []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"capacity exhausted\"}}\n\n")}
			out, err := s.prepareResponse(t.Context(), nil, &config.Config{}, profile, in)
			if err != nil {
				t.Fatal(err)
			}
			if out.StatusCode != http.StatusBadGateway || !strings.Contains(string(out.Body), "capacity exhausted") || !strings.Contains(string(out.Body), `"error"`) {
				t.Fatalf("stream error became success: status=%d body=%s", out.StatusCode, out.Body)
			}
		})
	}
}

func TestTranslatedStreamBoundsUnemittedInput(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Repeat(": ping\n", 1000)))
	}))
	defer upstream.Close()
	s := &Server{client: upstream.Client(), registry: adapter.NewRegistry()}
	profile := config.Profile{Adapter: "openai-chat-to-anthropic", UpstreamContract: "anthropic-messages", ClientContract: "openai-chat"}
	cfg := &config.Config{Limits: config.Limits{MaxBodyBytes: 1024}}
	w := httptest.NewRecorder()
	_, _, err := s.streamTranslatedUpstream(t.Context(), nil, cfg, config.Upstream{URL: upstream.URL}, profile, transform.Envelope{Method: "POST", Path: "/", Headers: make(http.Header)}, "regression", w)
	if err == nil || upstreamCode(err) != "upstream_stream_too_large" {
		t.Fatalf("raw stream bypassed limit: err=%v body=%s", err, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"error"`) {
		t.Fatalf("missing client failure: %s", w.Body.String())
	}
}

func TestTranslatedStreamBoundsSynthesizedCompletion(t *testing.T) {
	raw := "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"m\"}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"" + strings.Repeat("x", 128) + "\"}}\n\n"
	translator := adapter.NewSSETranslator("anthropic-messages", "openai-responses")
	var beforeFinish []adapter.SSEEvent
	if err := adapter.ReadSSEStream(strings.NewReader(raw), func(event adapter.SSEEvent) error {
		translated, err := translator.Translate(event)
		beforeFinish = append(beforeFinish, translated...)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	limit := int64(len(adapter.EncodeSSE(beforeFinish))) + 1
	if int64(len(raw)) > limit {
		t.Fatal("test fixture must fit the raw input limit")
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(raw))
	}))
	defer upstream.Close()
	s := &Server{client: upstream.Client(), registry: adapter.NewRegistry()}
	cfg := &config.Config{Limits: config.Limits{MaxBodyBytes: limit}}
	profile := config.Profile{Adapter: "openai-responses-to-anthropic", UpstreamContract: "anthropic-messages", ClientContract: "openai-responses"}
	w := httptest.NewRecorder()
	_, _, err := s.streamTranslatedUpstream(t.Context(), nil, cfg, config.Upstream{URL: upstream.URL}, profile, transform.Envelope{Method: "POST", Path: "/", Headers: make(http.Header)}, "regression", w)
	if err == nil || upstreamCode(err) != "upstream_stream_too_large" {
		t.Fatalf("completion bypassed output limit: %v", err)
	}
	if strings.Contains(w.Body.String(), "response.completed") || !strings.Contains(w.Body.String(), "response.failed") {
		t.Fatalf("client did not receive terminal failure: %s", w.Body.String())
	}
}

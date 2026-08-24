package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"unified-api-gateway/internal/adapter"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

// Golden tests pin the data-plane behaviour to the documented fixtures in
// tests/fixtures. When one of these tests fails, either the adapters drifted
// from the published contract (fix the code) or the contract intentionally
// changed (update the fixture in the same commit).

const fixturesDir = "../../tests/fixtures"

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixturesDir, name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func gatewayFromExampleConfig(t *testing.T) (*Server, *config.Config) {
	t.Helper()
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
	return gateway, cfg
}

func fixtureEnvelope(t *testing.T, name string) transform.Envelope {
	t.Helper()
	var request struct {
		Method  string            `json:"method"`
		Path    string            `json:"path"`
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	}
	if err := json.Unmarshal(loadFixture(t, name), &request); err != nil {
		t.Fatalf("parse fixture %s: %v", name, err)
	}
	headers := make(http.Header)
	for key, value := range request.Headers {
		headers.Set(key, value)
	}
	envelope := transform.Envelope{Method: request.Method, Path: request.Path, Headers: headers, Body: append([]byte(nil), request.Body...), Query: map[string][]string{}}
	if envelope.Method == "" {
		envelope.Method = http.MethodPost
	}
	return envelope
}

func expectUpstreamFixture(t *testing.T, name string, prepared transform.Envelope) {
	t.Helper()
	var expected struct {
		Method  string          `json:"method"`
		Path    string          `json:"path"`
		Headers map[string]any  `json:"headers"`
		Body    json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(loadFixture(t, name), &expected); err != nil {
		t.Fatalf("parse fixture %s: %v", name, err)
	}
	if expected.Method != "" && expected.Method != prepared.Method {
		t.Errorf("method: got %s want %s", prepared.Method, expected.Method)
	}
	if expected.Path != "" && expected.Path != prepared.Path {
		t.Errorf("path: got %s want %s", prepared.Path, expected.Path)
	}
	for key, want := range expected.Headers {
		if key == "X-Api-Key" || key == "Authorization" {
			continue // injected at transport time, not part of the transform contract
		}
		got := prepared.Headers.Get(key)
		if fmt.Sprint(want) != got {
			t.Errorf("header %s: got %q want %v", key, got, want)
		}
	}
	var gotBody any
	if err := json.Unmarshal(prepared.Body, &gotBody); err != nil {
		t.Fatalf("prepared body is not JSON: %v\n%s", err, prepared.Body)
	}
	var wantBody any
	if err := json.Unmarshal(expected.Body, &wantBody); err != nil {
		t.Fatalf("fixture body is not JSON: %v", err)
	}
	if !reflect.DeepEqual(gotBody, wantBody) {
		t.Errorf("body mismatch:\ngot  %s\nwant %s", mustMarshal(t, gotBody), expected.Body)
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestGoldenOpenAIChatToAnthropicRequest(t *testing.T) {
	gateway, cfg := gatewayFromExampleConfig(t)
	profile := cfg.Profiles["openai-to-anthropic"]
	prepared, err := gateway.prepareRequest(t.Context(), gateway.state.Load(), cfg, profile, fixtureEnvelope(t, "openai-chat-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	// buildUpstreamRequest injects the upstream's static headers at transport
	// time; mirror that so the fixture comparison covers the full contract.
	for key, value := range cfg.Upstreams[profile.Upstream].Headers {
		prepared.Headers.Set(key, value)
	}
	expectUpstreamFixture(t, "anthropic-messages-upstream.json", prepared)
}

func TestGoldenAnthropicResponseToOpenAI(t *testing.T) {
	gateway, cfg := gatewayFromExampleConfig(t)
	profile := cfg.Profiles["openai-to-anthropic"]
	upstream := transform.Envelope{StatusCode: http.StatusOK, Headers: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: loadFixture(t, "anthropic-messages-response.json")}
	response, err := gateway.prepareResponse(t.Context(), gateway.state.Load(), cfg, profile, upstream)
	if err != nil {
		t.Fatal(err)
	}
	var got, want map[string]any
	if err := json.Unmarshal(response.Body, &got); err != nil {
		t.Fatalf("response is not JSON: %v\n%s", err, response.Body)
	}
	if err := json.Unmarshal(loadFixture(t, "openai-chat-response.json"), &want); err != nil {
		t.Fatal(err)
	}
	// created is a server timestamp when the upstream omits it.
	delete(got, "created")
	delete(want, "created")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("response mismatch:\ngot  %s\nwant %s", mustMarshal(t, got), mustMarshal(t, want))
	}
}

func TestGoldenGenericUserRewrite(t *testing.T) {
	gateway, cfg := gatewayFromExampleConfig(t)
	profile := cfg.Profiles["user-v1-to-v2"]
	prepared, err := gateway.prepareRequest(t.Context(), gateway.state.Load(), cfg, profile, fixtureEnvelope(t, "generic-user-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	expectUpstreamFixture(t, "generic-user-upstream.json", prepared)
}

func TestGoldenAnthropicSSETranslation(t *testing.T) {
	events, err := adapter.ParseSSE(strings.NewReader(string(loadFixture(t, "sse-anthropic-events.txt"))))
	if err != nil {
		t.Fatal(err)
	}
	translated, err := adapter.TranslateSSE(events, "anthropic-messages", "openai-chat")
	if err != nil {
		t.Fatal(err)
	}
	var lastChunk map[string]any
	sawRole, sawText, sawFinish, sawDone := false, false, false, false
	finishCount, emptyDeltaFrames := 0, 0
	for _, event := range translated {
		if event.Data == "[DONE]" {
			sawDone = true
			continue
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(event.Data), &chunk); err != nil {
			t.Fatalf("event is not JSON: %s", event.Data)
		}
		// OpenAI ChatCompletionChunk requires non-null id/created/model;
		// strict client SDKs (Pydantic) reject null fields.
		if chunk["id"] == nil || chunk["model"] == nil || chunk["created"] == nil {
			t.Fatalf("chunk with null required fields: %s", event.Data)
		}
		if choices, ok := chunk["choices"].([]any); ok && len(choices) > 0 {
			choice := choices[0].(map[string]any)
			if choice["finish_reason"] != nil {
				finishCount++
			}
			delta, _ := choice["delta"].(map[string]any)
			if len(delta) == 0 && choice["finish_reason"] == nil {
				emptyDeltaFrames++
			}
		}
		if choices, ok := chunk["choices"].([]any); ok && len(choices) > 0 {
			delta, _ := choices[0].(map[string]any)["delta"].(map[string]any)
			if delta["role"] == "assistant" {
				sawRole = true
			}
			if text, _ := delta["content"].(string); strings.Contains(text, "Hello") || strings.Contains(text, "stream") {
				sawText = true
			}
			if reason := choices[0].(map[string]any)["finish_reason"]; reason != nil {
				sawFinish = true
				if reason != "stop" {
					t.Errorf("finish_reason=%v, want stop (OpenAI vocabulary)", reason)
				}
			}
		}
		if chunk["usage"] != nil {
			lastChunk = chunk
		}
	}
	if !sawRole || !sawText || !sawFinish || !sawDone {
		t.Fatalf("missing events: role=%v text=%v finish=%v done=%v", sawRole, sawText, sawFinish, sawDone)
	}
	if finishCount != 1 {
		t.Fatalf("exactly one finish_reason chunk allowed, got %d", finishCount)
	}
	if emptyDeltaFrames != 0 {
		t.Fatalf("OpenAI streams must not contain empty-delta frames, got %d", emptyDeltaFrames)
	}
	usage, ok := lastChunk["usage"].(map[string]any)
	if !ok {
		t.Fatalf("usage chunk missing: %#v", lastChunk)
	}
	// Prompt tokens come from message_start, completion tokens from
	// message_delta; the translator merges both.
	if usage["prompt_tokens"] != float64(12) || usage["completion_tokens"] != float64(6) || usage["total_tokens"] != float64(18) {
		t.Errorf("usage=%#v", usage)
	}
}

package adapter

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
)

func TestOpenAIChatToAnthropicRequest(t *testing.T) {
	input := Envelope{Method: http.MethodPost, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: []byte(`{"model":"claude","messages":[{"role":"system","content":"Be concise"},{"role":"user","content":"Hi"}],"max_tokens":32,"stream":true}`), ContentType: "application/json"}
	output, err := (OpenAIChatToAnthropic{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body["system"] != "Be concise" || body["max_tokens"] != float64(32) || body["stream"] != true {
		t.Fatalf("unexpected body: %#v", body)
	}
	messages := body["messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["role"] != "user" {
		t.Fatalf("messages=%#v", messages)
	}
}

func TestAnthropicResponseToOpenAI(t *testing.T) {
	input := Envelope{Header: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: []byte(`{"id":"msg_1","model":"claude","stop_reason":"end_turn","content":[{"type":"text","text":"Hello"}],"usage":{"input_tokens":3,"output_tokens":2}}`)}
	output, err := (OpenAIChatToAnthropic{}).AdaptResponse(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	choices := body["choices"].([]any)
	message := choices[0].(map[string]any)["message"].(map[string]any)
	if message["content"] != "Hello" || message["role"] != "assistant" {
		t.Fatalf("message=%#v", message)
	}
	if body["object"] != "chat.completion" {
		t.Fatalf("object=%v", body["object"])
	}
}

func TestSSETranslation(t *testing.T) {
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	events, err := ParseSSE(strings.NewReader(raw))
	if err != nil || len(events) != 3 {
		t.Fatalf("events=%#v err=%v", events, err)
	}
	translated, err := TranslateSSE(events, "anthropic-messages", "openai-chat")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(EncodeSSE(translated)), "chat.completion.chunk") {
		t.Fatalf("translated=%s", EncodeSSE(translated))
	}
	if translated[len(translated)-1].Data != "[DONE]" {
		t.Fatalf("stream must end with [DONE], got %#v", translated[len(translated)-1])
	}
}

func TestAnthropicToOpenAIChatResponseDirection(t *testing.T) {
	// The adapter's contract: the client speaks Anthropic Messages while the
	// upstream speaks OpenAI Chat, so an upstream chat.completion must be
	// converted into a Messages object (the old implementation converted in
	// the wrong direction and dropped the choices).
	input := Envelope{Header: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: []byte(`{"id":"chatcmpl_abc123","object":"chat.completion","model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"pong","tool_calls":[{"id":"call_1","type":"function","function":{"name":"ping","arguments":"{\"host\":\"example\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":4,"completion_tokens":9,"total_tokens":13}}`)}
	output, err := (AnthropicToOpenAIChat{}).AdaptResponse(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body["type"] != "message" || body["role"] != "assistant" || body["id"] != "msg_abc123" {
		t.Fatalf("envelope fields wrong: %#v", body)
	}
	content, ok := body["content"].([]any)
	if !ok || len(content) != 2 {
		t.Fatalf("content=%#v", body["content"])
	}
	if text := content[0].(map[string]any); text["type"] != "text" || text["text"] != "pong" {
		t.Fatalf("text block=%#v", text)
	}
	tool := content[1].(map[string]any)
	if tool["type"] != "tool_use" || tool["id"] != "call_1" || tool["name"] != "ping" {
		t.Fatalf("tool block=%#v", tool)
	}
	if toolInput := tool["input"].(map[string]any); toolInput["host"] != "example" {
		t.Fatalf("tool input=%#v", toolInput)
	}
	if body["stop_reason"] != "tool_use" {
		t.Fatalf("stop_reason=%v", body["stop_reason"])
	}
	usage := body["usage"].(map[string]any)
	if usage["input_tokens"] != float64(4) || usage["output_tokens"] != float64(9) {
		t.Fatalf("usage=%#v", usage)
	}
}

func TestOpenAIResponsesAdapterStillConvertsAnthropicUpstream(t *testing.T) {
	// Regression guard: the Responses adapter used to ride on the reversed
	// AnthropicToOpenAIChat response path; it must convert Anthropic Messages
	// via the OpenAI Chat shape instead.
	input := Envelope{Header: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: []byte(`{"id":"msg_9","type":"message","role":"assistant","model":"claude","content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1}}`)}
	output, err := (OpenAIResponsesToAnthropic{}).AdaptResponse(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body["object"] != "response" || body["status"] != "completed" {
		t.Fatalf("envelope=%#v", body)
	}
	message := body["output"].([]any)[0].(map[string]any)
	text := message["content"].([]any)[0].(map[string]any)
	if text["type"] != "output_text" || text["text"] != "pong" {
		t.Fatalf("output=%#v", body["output"])
	}
}

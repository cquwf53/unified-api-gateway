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
}

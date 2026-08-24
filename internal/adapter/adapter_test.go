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

func TestToolCallIndexUsesToolArraySpace(t *testing.T) {
	// Anthropic block indexes span text and tool blocks together; OpenAI
	// tool_calls[].index is the position inside the tool_calls array. A text
	// block before the tool used to shift every tool index by one.
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"let me check\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"ping\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\"}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_2\",\"name\":\"pong\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"[]\"}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-chat")
	toolIndexes := map[float64]bool{}
	for _, event := range translated {
		if event.Data == "[DONE]" {
			continue
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(event.Data), &chunk); err != nil {
			t.Fatal(err)
		}
		choices, _ := chunk["choices"].([]any)
		if len(choices) == 0 {
			continue
		}
		delta, _ := choices[0].(map[string]any)["delta"].(map[string]any)
		calls, _ := delta["tool_calls"].([]any)
		if calls == nil {
			continue
		}
		for _, raw := range calls {
			call := raw.(map[string]any)
			toolIndexes[call["index"].(float64)] = true
		}
	}
	if !toolIndexes[0] || !toolIndexes[1] || len(toolIndexes) != 2 {
		t.Fatalf("tool positions=%v, want exactly {0 1}", toolIndexes)
	}
}

func TestToolChoiceShapeConversion(t *testing.T) {
	// OpenAI strings must become Anthropic objects ("required" -> {"type":"any"}).
	input := Envelope{Method: http.MethodPost, Header: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: []byte(`{"model":"claude","tool_choice":"required","tools":[{"type":"function","function":{"name":"ping","parameters":{"type":"object"}}}],"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)}
	output, err := (OpenAIChatToAnthropic{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	if choice, ok := body["tool_choice"].(map[string]any); !ok || choice["type"] != "any" {
		t.Fatalf("tool_choice=%#v, want {\"type\":\"any\"}", body["tool_choice"])
	}

	// Named function selection maps onto {"type":"tool","name":...}.
	input.Body = []byte(`{"model":"claude","tool_choice":{"type":"function","function":{"name":"ping"}},"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)
	output, err = (OpenAIChatToAnthropic{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	if choice, ok := body["tool_choice"].(map[string]any); !ok || choice["type"] != "tool" || choice["name"] != "ping" {
		t.Fatalf("tool_choice=%#v, want {\"type\":\"tool\",\"name\":\"ping\"}", body["tool_choice"])
	}

	// Reverse: Anthropic objects become OpenAI strings / function objects.
	input.Body = []byte(`{"model":"gpt","tool_choice":{"type":"any"},"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)
	output, err = (AnthropicToOpenAIChat{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body["tool_choice"] != "required" {
		t.Fatalf("tool_choice=%#v, want \"required\"", body["tool_choice"])
	}
}

func TestDataURIImageBecomesBase64Source(t *testing.T) {
	input := Envelope{Method: http.MethodPost, Header: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: []byte(`{"model":"claude","max_tokens":8,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}},{"type":"text","text":"what is this"}]}]}`)}
	output, err := (OpenAIChatToAnthropic{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	content := body["messages"].([]any)[0].(map[string]any)["content"].([]any)
	image := content[0].(map[string]any)
	source := image["source"].(map[string]any)
	if source["type"] != "base64" || source["media_type"] != "image/png" || source["data"] != "aGVsbG8=" {
		t.Fatalf("source=%#v", source)
	}
	// http(s) URLs keep the url form.
	input.Body = []byte(`{"model":"claude","max_tokens":8,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.test/cat.png"}}]}]}`)
	output, err = (OpenAIChatToAnthropic{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	content = body["messages"].([]any)[0].(map[string]any)["content"].([]any)
	source = content[0].(map[string]any)["source"].(map[string]any)
	if source["type"] != "url" || source["url"] != "https://example.test/cat.png" {
		t.Fatalf("source=%#v", source)
	}
}

func TestParallelToolCallsFlagMapped(t *testing.T) {
	input := Envelope{Method: http.MethodPost, Header: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: []byte(`{"model":"claude","max_tokens":8,"parallel_tool_calls":false,"tool_choice":"auto","messages":[{"role":"user","content":"hi"}]}`)}
	output, err := (OpenAIChatToAnthropic{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	choice, ok := body["tool_choice"].(map[string]any)
	if !ok || choice["type"] != "auto" || choice["disable_parallel_tool_use"] != true {
		t.Fatalf("tool_choice=%#v", body["tool_choice"])
	}
	if _, leaked := body["parallel_tool_calls"]; leaked {
		t.Fatal("parallel_tool_calls must not be forwarded to Anthropic")
	}
}

func TestAnthropicToolHistoryConvertedToOpenAI(t *testing.T) {
	// assistant tool_use + user tool_result must become tool_calls and a
	// tool message, or multi-turn tool conversations lose their history.
	input := Envelope{Method: http.MethodPost, Header: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: []byte(`{"model":"gpt","max_tokens":8,"messages":[
		{"role":"user","content":"weather?"},
		{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Oslo"}}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"12C"}]},
		{"role":"user","content":"thanks"}
	]}`)}
	output, err := (AnthropicToOpenAIChat{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	messages := body["messages"].([]any)
	if len(messages) != 4 {
		t.Fatalf("messages=%d", len(messages))
	}
	assistant := messages[1].(map[string]any)
	calls := assistant["tool_calls"].([]any)
	call := calls[0].(map[string]any)
	if call["id"] != "toolu_1" || call["function"].(map[string]any)["name"] != "get_weather" {
		t.Fatalf("tool call=%#v", call)
	}
	if !strings.Contains(call["function"].(map[string]any)["arguments"].(string), "Oslo") {
		t.Fatalf("arguments lost: %#v", call["function"])
	}
	toolMessage := messages[2].(map[string]any)
	if toolMessage["role"] != "tool" || toolMessage["tool_call_id"] != "toolu_1" || toolMessage["content"] != "12C" {
		t.Fatalf("tool message=%#v", toolMessage)
	}
}

func TestResponsesFunctionCallOutputBecomesToolResult(t *testing.T) {
	input := Envelope{Method: http.MethodPost, Header: http.Header{"Content-Type": []string{"application/json"}}, ContentType: "application/json", Body: []byte(`{"model":"claude","max_output_tokens":8,"input":[
		{"type":"function_call","call_id":"call_1","name":"ping","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_1","output":"pong"}
	]}`)}
	output, err := (OpenAIResponsesToAnthropic{}).AdaptRequest(input, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(output.Body, &body); err != nil {
		t.Fatal(err)
	}
	messages := body["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("messages=%d", len(messages))
	}
	assistant := messages[0].(map[string]any)
	block := assistant["content"].([]any)[0].(map[string]any)
	if block["type"] != "tool_use" || block["id"] != "call_1" || block["name"] != "ping" {
		t.Fatalf("tool_use block=%#v", block)
	}
	user := messages[1].(map[string]any)
	result := user["content"].([]any)[0].(map[string]any)
	if result["type"] != "tool_result" || result["tool_use_id"] != "call_1" || result["content"] != "pong" {
		t.Fatalf("tool_result=%#v", result)
	}
}

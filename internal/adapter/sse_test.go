package adapter

import (
	"encoding/json"
	"strings"
	"testing"
)

func mustTranslate(t *testing.T, raw, from, to string) []SSEEvent {
	t.Helper()
	events, err := ParseSSE(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	translated, err := TranslateSSE(events, from, to)
	if err != nil {
		t.Fatal(err)
	}
	return translated
}

func TestSSEUsageOnMessageDeltaIsTranslated(t *testing.T) {
	raw := "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":11,\"output_tokens\":7}}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-chat")
	var chunk map[string]any
	if err := json.Unmarshal([]byte(translated[0].Data), &chunk); err != nil {
		t.Fatal(err)
	}
	usage, ok := chunk["usage"].(map[string]any)
	if !ok {
		t.Fatalf("usage missing from translated chunk: %#v", chunk)
	}
	if usage["prompt_tokens"] != float64(11) || usage["completion_tokens"] != float64(7) || usage["total_tokens"] != float64(18) {
		t.Fatalf("usage=%#v", usage)
	}
}

func TestSSEToolUseBlocksTranslateToOpenAIToolCalls(t *testing.T) {
	raw := "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"get_weather\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"Oslo\\\"}\"}}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-chat")
	start, delta1, delta2 := translated[0], translated[1], translated[2]
	var startChunk, deltaChunk map[string]any
	if err := json.Unmarshal([]byte(start.Data), &startChunk); err != nil {
		t.Fatal(err)
	}
	tool, ok := startChunk["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)["tool_calls"].([]any)[0].(map[string]any)
	if !ok || tool["id"] != "toolu_1" {
		t.Fatalf("start tool call=%#v", tool)
	}
	if err := json.Unmarshal([]byte(delta1.Data), &deltaChunk); err != nil {
		t.Fatal(err)
	}
	got := deltaChunk["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)["tool_calls"].([]any)[0].(map[string]any)["function"].(map[string]any)["arguments"]
	if got != `{"city":` {
		t.Fatalf("first delta arguments=%v", got)
	}
	if err := json.Unmarshal([]byte(delta2.Data), &deltaChunk); err != nil {
		t.Fatal(err)
	}
	got = deltaChunk["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)["tool_calls"].([]any)[0].(map[string]any)["function"].(map[string]any)["arguments"]
	if got != `"Oslo"}` {
		t.Fatalf("second delta arguments=%v", got)
	}
}

func TestSSEPingIsDropped(t *testing.T) {
	raw := "event: ping\ndata: {\"type\":\"ping\"}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-chat")
	// The ping itself must not become a chunk; the trailing event is the
	// [DONE] sentinel Finish() appends to an unterminated stream.
	for _, event := range translated {
		if event.Data != "[DONE]" {
			t.Fatalf("ping should be dropped, got %#v", translated)
		}
	}
	if len(translated) == 0 {
		t.Fatal("expected a [DONE] sentinel for the unterminated stream")
	}
}

func TestTranslateOpenAIToAnthropicProducesValidEventSequence(t *testing.T) {
	raw := "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"ping\",\"arguments\":\"\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":6}}\n\n" +
		"data: [DONE]\n\n"
	translated := mustTranslate(t, raw, "openai-chat", "anthropic-messages")
	var types []string
	for _, event := range translated {
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			t.Fatalf("event %q is not JSON: %v", event.Event, err)
		}
		types = append(types, payload["type"].(string))
	}
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_delta", "content_block_stop", "content_block_start", "content_block_delta", "message_delta", "message_delta", "message_stop"}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("sequence=%v", types)
	}
	var finish map[string]any
	if err := json.Unmarshal([]byte(translated[7].Data), &finish); err != nil {
		t.Fatal(err)
	}
	if finish["delta"].(map[string]any)["stop_reason"] != "tool_calls" {
		t.Fatalf("finish=%#v", finish)
	}
	var usageEvent map[string]any
	if err := json.Unmarshal([]byte(translated[8].Data), &usageEvent); err != nil {
		t.Fatal(err)
	}
	if usageEvent["usage"].(map[string]any)["input_tokens"] != float64(4) {
		t.Fatalf("usage=%#v", usageEvent)
	}
}

func TestAggregateSSEKeepsToolCalls(t *testing.T) {
	raw := "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"ping\",\"arguments\":\"{\\\"a\\\":\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	events, err := ParseSSE(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := AggregateSSE(events, "openai-chat")
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	choice := result["choices"].([]any)[0].(map[string]any)
	if choice["finish_reason"] != "tool_calls" {
		t.Fatalf("finish=%v", choice["finish_reason"])
	}
	message := choice["message"].(map[string]any)
	if message["content"] != "Hi" {
		t.Fatalf("content=%v", message["content"])
	}
	call := message["tool_calls"].([]any)[0].(map[string]any)
	fn := call["function"].(map[string]any)
	if fn["name"] != "ping" || fn["arguments"] != `{"a":1}` {
		t.Fatalf("tool call=%#v", call)
	}
}

func TestAggregateSSEAnthropicToolUse(t *testing.T) {
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"ping\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\"}}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"input_tokens\":2,\"output_tokens\":3}}\n\n"
	events, err := ParseSSE(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := AggregateSSE(events, "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	content := result["content"].([]any)
	if content[0].(map[string]any)["text"] != "Hi" {
		t.Fatalf("content=%#v", content)
	}
	tool := content[1].(map[string]any)
	if tool["type"] != "tool_use" || tool["id"] != "toolu_1" || tool["name"] != "ping" || string(mustJSON(t, tool["input"])) != "{}" {
		t.Fatalf("tool=%#v", tool)
	}
	if result["stop_reason"] != "tool_use" {
		t.Fatalf("stop_reason=%v", result["stop_reason"])
	}
}

func TestReadSSEStreamEmitsIncrementally(t *testing.T) {
	raw := "data: one\n\ndata: two\n\n"
	var events []SSEEvent
	err := ReadSSEStream(strings.NewReader(raw), func(event SSEEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Data != "one" || events[1].Data != "two" {
		t.Fatalf("events=%#v", events)
	}
}

func TestUnterminatedAnthropicStreamStillGetsDone(t *testing.T) {
	// An Anthropic stream closed without message_stop (for example after an
	// error) must still hand OpenAI clients the [DONE] sentinel.
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-chat")
	if translated[len(translated)-1].Data != "[DONE]" {
		t.Fatalf("missing [DONE] sentinel: %#v", translated)
	}
}

func TestUnterminatedOpenAIStreamStillGetsMessageStop(t *testing.T) {
	// Symmetric: an OpenAI stream that never sent [DONE] still owes Anthropic
	// clients message_stop.
	raw := "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	translated := mustTranslate(t, raw, "openai-chat", "anthropic-messages")
	last := translated[len(translated)-1]
	if !strings.Contains(last.Data, "message_stop") {
		t.Fatalf("missing message_stop: %s", EncodeSSE(translated))
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

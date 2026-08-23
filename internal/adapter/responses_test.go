package adapter

import (
	"encoding/json"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
)

// TestRegistryMatchesKnownAdapters guards the config-side adapter name list
// against registry drift (config cannot import adapter without a cycle).
func TestRegistryMatchesKnownAdapters(t *testing.T) {
	registry := NewRegistry()
	for _, name := range registry.Names() {
		if !config.KnownAdapters[strings.ToLower(name)] {
			t.Errorf("adapter %q is registered but missing from config.KnownAdapters", name)
		}
	}
	for name := range config.KnownAdapters {
		if _, ok := registry.Get(name); !ok {
			t.Errorf("config.KnownAdapters lists %q but the registry does not provide it", name)
		}
	}
}

// TestAnthropicToResponsesStream verifies the Responses streaming protocol:
// created/item.added/delta/completed events and no [DONE] sentinel.
func TestAnthropicToResponsesStream(t *testing.T) {
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-responses")
	var types []string
	for _, event := range translated {
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			t.Fatalf("event %q is not JSON: %v", event.Event, err)
		}
		types = append(types, payload["type"].(string))
		if event.Data == "[DONE]" {
			t.Fatal("Responses streams must not carry a [DONE] sentinel")
		}
	}
	want := "response.created,response.in_progress,response.output_item.added,response.content_part.added,response.output_text.delta,response.output_text.delta,response.output_text.done,response.content_part.done,response.output_item.done,response.completed"
	if strings.Join(types, ",") != want {
		t.Fatalf("sequence=%v", types)
	}
	// Every Responses protocol event carries an incrementing sequence_number.
	for index, event := range translated {
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["sequence_number"] != float64(index+1) {
			t.Fatalf("event %d has sequence_number %v", index, payload["sequence_number"])
		}
	}
	var completed map[string]any
	if err := json.Unmarshal([]byte(translated[len(translated)-1].Data), &completed); err != nil {
		t.Fatal(err)
	}
	response := completed["response"].(map[string]any)
	if response["status"] != "completed" || response["id"] != "resp_1" {
		t.Fatalf("completed response=%#v", response)
	}
	// Clients that read event.response instead of accumulating deltas must
	// still receive the full message text.
	item := response["output"].([]any)[0].(map[string]any)
	if item["id"] != "msg_0" {
		t.Fatalf("output item id must match the delta item_id, got %v", item["id"])
	}
	text := item["content"].([]any)[0].(map[string]any)["text"]
	if text != "Hello" {
		t.Fatalf("completed output text=%v, want accumulated \"Hello\"", text)
	}
	usage := response["usage"].(map[string]any)
	if usage["input_tokens"] != float64(5) || usage["output_tokens"] != float64(2) || usage["total_tokens"] != float64(7) {
		t.Fatalf("usage=%#v", usage)
	}
}

func TestResponsesToAnthropicStream(t *testing.T) {
	raw := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":4}}}\n\n"
	translated := mustTranslate(t, raw, "openai-responses", "anthropic-messages")
	var types []string
	for _, event := range translated {
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			t.Fatalf("event %q is not JSON: %v", event.Event, err)
		}
		if payload["type"] == nil {
			t.Fatalf("event without type: %s", event.Data)
		}
		types = append(types, payload["type"].(string))
	}
	joined := strings.Join(types, ",")
	if !strings.Contains(joined, "message_start") || !strings.Contains(joined, "content_block_delta") || !strings.Contains(joined, "message_delta") || !strings.Contains(joined, "message_stop") {
		t.Fatalf("sequence incomplete: %v", types)
	}
	for _, event := range translated {
		if strings.Contains(event.Data, "output_text") || strings.Contains(event.Data, "response.completed") {
			t.Fatalf("responses protocol leaked to anthropic client: %s", event.Data)
		}
	}
}

// TestAnthropicBlockIndexesStartAtZero pins the content block numbering: the
// first block must be index 0 for clients that assemble content by index.
func TestAnthropicBlockIndexesStartAtZero(t *testing.T) {
	raw := "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	translated := mustTranslate(t, raw, "openai-chat", "anthropic-messages")
	for _, event := range translated {
		if !strings.Contains(event.Data, "content_block_start") {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			t.Fatal(err)
		}
		index, ok := payload["index"].(float64)
		if !ok || index != 0 {
			t.Fatalf("first content_block_start must be index 0, got %v", payload["index"])
		}
		return
	}
	t.Fatal("no content_block_start in translated stream")
}

func TestResponsesIncompleteOnMaxTokens(t *testing.T) {
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":9}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-responses")
	last := translated[len(translated)-1]
	var payload map[string]any
	if err := json.Unmarshal([]byte(last.Data), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["type"] != "response.incomplete" {
		t.Fatalf("length-limited stream must end with response.incomplete, got %v", payload["type"])
	}
	response := payload["response"].(map[string]any)
	if response["status"] != "incomplete" || response["incomplete_details"].(map[string]any)["reason"] != "max_output_tokens" {
		t.Fatalf("incomplete response=%#v", response)
	}
	if response["usage"].(map[string]any)["output_tokens"] != float64(9) {
		t.Fatalf("usage lost on incomplete: %#v", response["usage"])
	}
}

func TestStreamErrorAnthropicToOpenAIChat(t *testing.T) {
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"half a sent\"}}\n\n" +
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-chat")
	var errorChunk map[string]any
	sawDone, sawFinish := false, false
	for _, event := range translated {
		if event.Data == "[DONE]" {
			sawDone = true
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			t.Fatal(err)
		}
		if _, ok := payload["error"]; ok {
			errorChunk = payload
		}
		if choices, ok := payload["choices"].([]any); ok && len(choices) > 0 {
			if choices[0].(map[string]any)["finish_reason"] != nil {
				sawFinish = true
			}
		}
	}
	if errorChunk == nil {
		t.Fatalf("upstream error vanished: %s", EncodeSSE(translated))
	}
	errObj := errorChunk["error"].(map[string]any)
	if errObj["message"] != "Overloaded" || errObj["type"] != "overloaded_error" {
		t.Fatalf("error payload=%#v", errObj)
	}
	if !sawDone {
		t.Fatal("stream must terminate with [DONE] after the error frame")
	}
	if sawFinish {
		t.Fatal("a normal finish_reason chunk after the error would disguise the failure")
	}
}

func TestStreamErrorOpenAIChatToAnthropic(t *testing.T) {
	raw := "data: {\"id\":\"c1\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"half a sent\"}}]}\n\n" +
		"data: {\"error\":{\"message\":\"The server had an error\",\"type\":\"server_error\",\"code\":null}}\n\n" +
		"data: [DONE]\n\n"
	translated := mustTranslate(t, raw, "openai-chat", "anthropic-messages")
	sawErrorEvent, sawMessageStop := false, false
	for _, event := range translated {
		if event.Event == "error" || strings.Contains(event.Data, "\"type\":\"error\"") {
			if !strings.Contains(event.Data, "The server had an error") {
				t.Fatalf("error message lost: %s", event.Data)
			}
			sawErrorEvent = true
		}
		if strings.Contains(event.Data, "message_stop") {
			sawMessageStop = true
		}
	}
	if !sawErrorEvent {
		t.Fatalf("upstream error vanished: %s", EncodeSSE(translated))
	}
	if sawMessageStop {
		t.Fatal("a normal message_stop after the error would disguise the failure")
	}
}

// TestUnterminatedAnthropicStreamToResponsesGetsTerminal pins the Finish()
// ordering: an openai-responses target must not be captured by the generic
// "openai" branch (which would emit a bare [DONE]).
func TestUnterminatedAnthropicStreamToResponsesGetsTerminal(t *testing.T) {
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_9\",\"model\":\"claude\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n"
	translator := NewSSETranslator("anthropic-messages", "openai-responses")
	var out []SSEEvent
	for _, event := range eventsFromRaw(t, raw) {
		translated, err := translator.Translate(event)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, translated...)
	}
	out = append(out, translator.Finish()...)
	last := out[len(out)-1]
	if !strings.Contains(last.Data, "response.completed") {
		t.Fatalf("unterminated stream must end with response.completed, got %s", EncodeSSE(out))
	}
	for _, event := range out {
		if event.Data == "[DONE]" {
			t.Fatal("[DONE] leaked into a Responses stream")
		}
	}
}

// TestResponsesFunctionCallForwarding: Anthropic tool_use blocks become
// function_call items with argument deltas and terminal done events.
func TestResponsesFunctionCallForwarding(t *testing.T) {
	raw := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude\"}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"ping\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"a\\\":\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"1}\"}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	translated := mustTranslate(t, raw, "anthropic-messages", "openai-responses")
	var types []string
	for _, event := range translated {
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			t.Fatal(err)
		}
		types = append(types, payload["type"].(string))
	}
	joined := strings.Join(types, ",")
	for _, needed := range []string{"response.output_item.added", "response.function_call_arguments.delta", "response.function_call_arguments.done", "response.completed"} {
		if !strings.Contains(joined, needed) {
			t.Fatalf("missing %s in sequence=%v", needed, types)
		}
	}
	// The completed response carries the assembled function call item.
	var completed map[string]any
	if err := json.Unmarshal([]byte(translated[len(translated)-1].Data), &completed); err != nil {
		t.Fatal(err)
	}
	output := completed["response"].(map[string]any)["output"].([]any)
	var callItem map[string]any
	for _, raw := range output {
		if item, ok := raw.(map[string]any); ok && item["type"] == "function_call" {
			callItem = item
		}
	}
	if callItem == nil {
		t.Fatalf("completed output has no function_call item: %#v", output)
	}
	if callItem["name"] != "ping" || callItem["arguments"] != `{"a":1}` || callItem["call_id"] != "toolu_1" {
		t.Fatalf("function_call item=%#v", callItem)
	}
}

func eventsFromRaw(t *testing.T, raw string) []SSEEvent {
	t.Helper()
	events, err := ParseSSE(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return events
}

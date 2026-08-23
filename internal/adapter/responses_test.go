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
	want := "response.created,response.in_progress,response.output_item.added,response.content_part.added,response.output_text.delta,response.output_text.delta,response.content_part.done,response.output_item.done,response.completed"
	if strings.Join(types, ",") != want {
		t.Fatalf("sequence=%v", types)
	}
	var completed map[string]any
	if err := json.Unmarshal([]byte(translated[len(translated)-1].Data), &completed); err != nil {
		t.Fatal(err)
	}
	response := completed["response"].(map[string]any)
	if response["status"] != "completed" || response["id"] != "msg_1" {
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

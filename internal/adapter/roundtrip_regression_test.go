package adapter

import (
	"encoding/json"
	"strings"
	"testing"
	"unified-api-gateway/internal/config"
)

func TestLargeIntegerSurvivesAllToolJSONDirections(t *testing.T) {
	const integer = "9007199254740993"
	cases := []struct {
		name    string
		adapter Adapter
		request bool
		body    string
	}{
		{"anthropic request", AnthropicToOpenAIChat{}, true, `{"model":"m","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t","name":"f","input":{"account":9007199254740993}}]}]}`},
		{"chat response", AnthropicToOpenAIChat{}, false, `{"model":"m","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"t","type":"function","function":{"name":"f","arguments":"{\"account\":9007199254740993}"}}]},"finish_reason":"tool_calls"}]}`},
		{"responses request", OpenAIResponsesToAnthropic{}, true, `{"model":"m","input":[{"type":"function_call","call_id":"t","name":"f","arguments":"{\"account\":9007199254740993}"}]}`},
		{"responses response", OpenAIResponsesToAnthropic{}, false, `{"model":"m","content":[{"type":"tool_use","id":"t","name":"f","input":{"account":9007199254740993}}],"stop_reason":"tool_use"}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			in := Envelope{Header: map[string][]string{}, Body: []byte(test.body)}
			var out Envelope
			var err error
			if test.request {
				out, err = test.adapter.AdaptRequest(in, config.Profile{})
			} else {
				out, err = test.adapter.AdaptResponse(in, config.Profile{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out.Body), integer) {
				t.Fatalf("integer changed: %s", out.Body)
			}
		})
	}
}

func TestLargeIntegerSurvivesSSEAndAggregation(t *testing.T) {
	body := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[{"type":"tool_use","id":"t","name":"f","input":{"account":9007199254740993}}],"stop_reason":"tool_use","usage":{"input_tokens":9007199254740993,"output_tokens":4}}`)
	raw, err := EncodeBufferedSSE(body, "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	events, err := ParseSSE(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"openai-chat", "openai-responses"} {
		t.Run(target, func(t *testing.T) {
			translated, err := TranslateSSE(events, "anthropic-messages", target)
			if err != nil {
				t.Fatal(err)
			}
			result, _, err := AggregateSSE(translated, target)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(result), "9007199254740993") || !strings.Contains(string(result), `"total_tokens":9007199254740997`) {
				t.Fatalf("SSE numbers changed: %s", result)
			}
			reversed, err := TranslateSSE(translated, target, "anthropic-messages")
			if err != nil {
				t.Fatal(err)
			}
			final, _, err := AggregateSSE(reversed, "anthropic-messages")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(final), `"account":9007199254740993`) || !strings.Contains(string(final), `"input_tokens":9007199254740993`) {
				t.Fatalf("reverse SSE numbers changed: %s", final)
			}
		})
	}
}

func TestCachedTokensSurviveStreamingRoundTrip(t *testing.T) {
	input := []SSEEvent{{Data: `{"type":"message_start","message":{"id":"msg_1","model":"m","usage":{"input_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":100,"output_tokens":0}}}`}, {Data: `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`}, {Data: `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}`}, {Data: `{"type":"message_stop"}`}}
	for _, target := range []string{"openai-chat", "openai-responses"} {
		t.Run(target, func(t *testing.T) {
			translated, err := TranslateSSE(input, "anthropic-messages", target)
			if err != nil {
				t.Fatal(err)
			}
			result, _, err := AggregateSSE(translated, target)
			if err != nil {
				t.Fatal(err)
			}
			var value map[string]any
			if err := json.Unmarshal(result, &value); err != nil {
				t.Fatal(err)
			}
			usage := value["usage"].(map[string]any)
			promptKey := "prompt_tokens"
			if target == "openai-responses" {
				promptKey = "input_tokens"
			}
			if usage[promptKey] != float64(105) || usage["total_tokens"] != float64(109) {
				t.Fatalf("cache totals lost: %s", result)
			}
			reverse, err := TranslateSSE(translated, target, "anthropic-messages")
			if err != nil {
				t.Fatal(err)
			}
			restored, _, err := AggregateSSE(reverse, "anthropic-messages")
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(restored, &value); err != nil {
				t.Fatal(err)
			}
			usage = value["usage"].(map[string]any)
			if usage["input_tokens"] != float64(2) || usage["cache_read_input_tokens"] != float64(100) || usage["cache_creation_input_tokens"] != float64(3) || usage["output_tokens"] != float64(4) {
				t.Fatalf("cache counters did not round-trip: %s", restored)
			}
		})
	}
}

func TestToolFinishReasonSurvivesSameFrameUsage(t *testing.T) {
	raw := []SSEEvent{{Data: `{"id":"c","model":"m","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`}, {Data: "[DONE]"}}
	anthropic, err := TranslateSSE(raw, "openai-chat", "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	chat, err := TranslateSSE(anthropic, "anthropic-messages", "openai-chat")
	if err != nil {
		t.Fatal(err)
	}
	final, _, err := AggregateSSE(chat, "openai-chat")
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(final, &value); err != nil {
		t.Fatal(err)
	}
	if value["choices"].([]any)[0].(map[string]any)["finish_reason"] != "tool_calls" || value["usage"].(map[string]any)["total_tokens"] != float64(10) {
		t.Fatalf("finish/usage changed on roundtrip: %s", final)
	}
}

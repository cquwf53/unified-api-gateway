package adapter

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"
	"unified-api-gateway/internal/config"
)

func TestQualityBufferedChatParallelToolCalls(t *testing.T) {
	body := []byte(`{"id":"c1","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_a","type":"function","function":{"name":"a","arguments":"{\"x\":1}"}},{"id":"call_b","type":"function","function":{"name":"b","arguments":"{\"y\":2}"}}]},"finish_reason":"tool_calls"}]}`)
	stream, err := EncodeBufferedSSE(body, "openai-chat")
	if err != nil {
		t.Fatal(err)
	}
	events, err := ParseSSE(strings.NewReader(string(stream)))
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := AggregateSSE(events, "openai-chat")
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(got, &value); err != nil {
		t.Fatal(err)
	}
	calls := value["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)["tool_calls"].([]any)
	if len(calls) != 2 {
		t.Fatalf("two calls collapsed: JSON=%s; SSE=%s; aggregate=%s", body, stream, got)
	}
}

func TestQualityMaxTokensWithToolCall(t *testing.T) {
	body := []byte(`{"id":"msg_1","model":"m","content":[{"type":"tool_use","id":"t1","name":"f","input":{}}],"stop_reason":"max_tokens","usage":{"input_tokens":1,"output_tokens":8}}`)
	output, err := (OpenAIResponsesToAnthropic{}).AdaptResponse(Envelope{Header: map[string][]string{}, Body: body}, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(output.Body, &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "incomplete" {
		t.Fatalf("truncated tool marked completed: input=%s output=%s", body, output.Body)
	}
}

func TestQualityLargeIntegerToolArguments(t *testing.T) {
	const want = "9007199254740993"
	for name, input := range map[string]string{
		"request":  `{"model":"m","messages":[{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"lookup","arguments":"{\"account_id\":9007199254740993}"}}]}]}`,
		"response": `{"model":"m","content":[{"type":"tool_use","id":"t1","name":"lookup","input":{"account_id":9007199254740993}}],"stop_reason":"tool_use"}`,
	} {
		t.Run(name, func(t *testing.T) {
			var out Envelope
			var err error
			if name == "request" {
				out, err = (OpenAIChatToAnthropic{}).AdaptRequest(Envelope{Header: map[string][]string{}, Body: []byte(input)}, config.Profile{})
			} else {
				out, err = (OpenAIChatToAnthropic{}).AdaptResponse(Envelope{Header: map[string][]string{}, Body: []byte(input)}, config.Profile{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out.Body), want) {
				t.Fatalf("integer changed: input=%s output=%s", input, out.Body)
			}
		})
	}
}

func TestQualityResponsesOmittedStream(t *testing.T) {
	input := `{"model":"m","input":"hi"}`
	out, err := (OpenAIResponsesToAnthropic{}).AdaptRequest(Envelope{Header: map[string][]string{}, Body: []byte(input)}, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(out.Body, &value); err != nil {
		t.Fatal(err)
	}
	if stream, present := value["stream"]; present {
		if _, ok := stream.(bool); !ok {
			t.Fatalf("optional stream became invalid null: input=%s output=%s", input, out.Body)
		}
	}
}

func TestQualityChatUsageInBufferedChunk(t *testing.T) {
	input := []byte(`{"id":"c1","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`)
	raw, err := EncodeBufferedSSE(input, "openai-chat")
	if err != nil {
		t.Fatal(err)
	}
	events, err := ParseSSE(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	translated, err := TranslateSSE(events, "openai-chat", "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := AggregateSSE(translated, "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(got, &value); err != nil {
		t.Fatal(err)
	}
	usage := value["usage"].(map[string]any)
	if usage["input_tokens"] != float64(7) || usage["output_tokens"] != float64(3) {
		t.Fatalf("usage lost: input=%s output=%s", input, got)
	}
}

func TestQualityAnthropicBufferedBlockOrder(t *testing.T) {
	input := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[{"type":"text","text":"before"},{"type":"tool_use","id":"t1","name":"f","input":{}},{"type":"text","text":"after"}],"stop_reason":"tool_use"}`)
	stream, err := EncodeBufferedSSE(input, "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	events, err := ParseSSE(strings.NewReader(string(stream)))
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := AggregateSSE(events, "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(got, &value); err != nil {
		t.Fatal(err)
	}
	content := value["content"].([]any)
	if len(content) != 3 || content[1].(map[string]any)["type"] != "tool_use" {
		t.Fatalf("content order changed: input=%s output=%s", input, got)
	}
}

func TestQualityCachedPromptTokens(t *testing.T) {
	input := []byte(`{"id":"msg_1","model":"m","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":100,"output_tokens":4}}`)
	out, err := (OpenAIChatToAnthropic{}).AdaptResponse(Envelope{Header: map[string][]string{}, Body: input}, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Body, &got); err != nil {
		t.Fatal(err)
	}
	if got["usage"].(map[string]any)["prompt_tokens"] != float64(105) {
		t.Fatalf("cache input accounting discarded: input=%s output=%s", input, out.Body)
	}
}

func TestQualityAnthropicToResponsesArgumentAllocation(t *testing.T) {
	var previousAllocated uint64
	for _, count := range []int{8000, 16000} {
		translator := NewSSETranslator("anthropic-messages", "openai-responses")
		for _, event := range []SSEEvent{{Data: `{"type":"message_start","message":{"id":"m","model":"m"}}`}, {Data: `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"f"}}`}, {Data: `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"text\":\""}}`}} {
			if _, err := translator.Translate(event); err != nil {
				t.Fatal(err)
			}
		}
		event := SSEEvent{Data: `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"x"}}`}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		for i := 0; i < count; i++ {
			if _, err := translator.Translate(event); err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(started)
		runtime.ReadMemStats(&after)
		allocated := after.TotalAlloc - before.TotalAlloc
		if previousAllocated > 0 && allocated*10 > previousAllocated*28 {
			t.Fatalf("doubling tool arguments should grow allocation approximately linearly: previous=%d next=%d", previousAllocated, allocated)
		}
		previousAllocated = allocated
		t.Logf("argument_bytes=%d input_data_bytes=%d allocated_bytes=%d elapsed=%s", count, count*len(event.Data), allocated, elapsed)
	}
}

package adapter

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
)

func TestAnthropicStringUserContentAndStopSurvive(t *testing.T) {
	out, err := (AnthropicToOpenAIChat{}).AdaptRequest(Envelope{Header: map[string][]string{}, Body: []byte(`{"model":"m","messages":[{"role":"user","content":"你好"}],"stop_sequences":["END"]}`)}, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(out.Body, &body); err != nil {
		t.Fatal(err)
	}
	if got := body["messages"].([]any)[0].(map[string]any)["content"]; got != "你好" {
		t.Errorf("user text lost: %s", out.Body)
	}
	if got, ok := body["stop"].([]any); !ok || len(got) != 1 || got[0] != "END" {
		t.Errorf("stop lost: %s", out.Body)
	}
}

func TestResponsesParallelToolConstraintSurvives(t *testing.T) {
	out, err := (OpenAIResponsesToAnthropic{}).AdaptRequest(Envelope{Header: map[string][]string{}, Body: []byte(`{"model":"m","input":"hi","parallel_tool_calls":false}`)}, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(out.Body, &body); err != nil {
		t.Fatal(err)
	}
	choice, _ := body["tool_choice"].(map[string]any)
	if choice["disable_parallel_tool_use"] != true {
		t.Fatalf("parallel constraint lost: %s", out.Body)
	}
}

// Repeated empty data lines must consume the event budget, even though each
// individual value is empty. Feed them without constructing a huge input.
type emptyDataReader struct{ remaining int }

func (r *emptyDataReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p)/6, r.remaining)
	for i := 0; i < n; i++ {
		copy(p[i*6:], "data:\n")
	}
	r.remaining -= n
	return n * 6, nil
}

func TestSSEEmptyDataLinesAreBounded(t *testing.T) {
	err := ReadSSEStream(&emptyDataReader{remaining: (64 << 10) + 1}, func(SSEEvent) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "event exceeds") {
		t.Fatalf("empty data lines bypassed limit: %v", err)
	}
}

func TestAggregateSSERejectsUpstreamErrors(t *testing.T) {
	for _, contract := range []string{"openai-chat", "anthropic-messages", "openai-responses"} {
		t.Run(contract, func(t *testing.T) {
			events := []SSEEvent{{Event: "error", Data: `{"type":"error","error":{"type":"overloaded_error","message":"capacity exhausted"}}`}}
			if contract == "openai-responses" {
				events[0] = SSEEvent{Event: "response.failed", Data: `{"type":"response.failed","response":{"status":"failed","error":{"type":"server_error","message":"capacity exhausted"}}}`}
			}
			body, _, err := AggregateSSE(events, contract)
			if err == nil || !strings.Contains(err.Error(), "capacity exhausted") || len(body) != 0 {
				t.Fatalf("failure became success: body=%s err=%v", body, err)
			}
		})
	}
}

func TestResponsesToolBeforeTextIndexesAndOutputOrder(t *testing.T) {
	events := []SSEEvent{
		{Data: `{"type":"message_start","message":{"id":"msg_1","model":"m"}}`},
		{Data: `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_1","name":"f"}}`},
		{Data: `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}`},
		{Data: `{"type":"content_block_stop","index":0}`},
		{Data: `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"done"}}`},
		{Data: `{"type":"message_stop"}`},
	}
	out, err := TranslateSSE(events, "anthropic-messages", "openai-responses")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range out {
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			t.Fatal(err)
		}
		if event.Event == "response.output_text.delta" && payload["output_index"] != float64(1) {
			t.Errorf("text delta addressed tool item: %s", event.Data)
		}
		if event.Event == "response.completed" {
			items := payload["response"].(map[string]any)["output"].([]any)
			if len(items) != 2 || items[0].(map[string]any)["type"] != "function_call" || items[1].(map[string]any)["type"] != "message" {
				t.Errorf("final output order differs from announced indexes: %s", event.Data)
			}
		}
	}
}

func TestBufferedResponsesToolAndReasoningRoundTrip(t *testing.T) {
	body := []byte(`{"id":"resp_1","object":"response","status":"completed","model":"m","output":[{"id":"reason_1","type":"reasoning","summary":[{"type":"summary_text","text":"consider"}]},{"id":"fc_1","type":"function_call","call_id":"call_1","name":"f","arguments":"{\"x\":1}"}]}`)
	encoded, err := EncodeBufferedSSE(body, "openai-responses")
	if err != nil {
		t.Fatal(err)
	}
	events, err := ParseSSE(strings.NewReader(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	result, _, err := AggregateSSE(events, "openai-responses")
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(result, &response); err != nil {
		t.Fatal(err)
	}
	items := response["output"].([]any)
	if len(items) != 2 {
		t.Fatalf("reasoning or tool lost: %s", result)
	}
	if items[0].(map[string]any)["type"] != "reasoning" || !strings.Contains(string(result), "consider") {
		t.Errorf("reasoning shape/content lost: %s", result)
	}
	if items[1].(map[string]any)["arguments"] != `{"x":1}` {
		t.Errorf("tool arguments repeated: %s", result)
	}
}

func TestResponsesThinkingStreamsBothDirections(t *testing.T) {
	input := []SSEEvent{{Data: `{"type":"message_start","message":{"id":"msg_1","model":"m"}}`}, {Data: `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"consider"}}`}, {Data: `{"type":"message_stop"}`}}
	responses, err := TranslateSSE(input, "anthropic-messages", "openai-responses")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(EncodeSSE(responses)), "response.reasoning_summary_text.delta") {
		t.Fatalf("reasoning disappeared: %s", EncodeSSE(responses))
	}
	reversed, err := TranslateSSE(responses, "openai-responses", "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(EncodeSSE(reversed)), `"thinking":"consider"`) {
		t.Fatalf("reverse reasoning disappeared: %s", EncodeSSE(reversed))
	}
}

func TestResponsesIncompleteUsageAndOverflowTermination(t *testing.T) {
	for _, terminal := range []string{"response.completed", "response.incomplete"} {
		t.Run(terminal+"/overflow", func(t *testing.T) {
			translator := NewSSETranslator("openai-responses", "anthropic-messages")
			translator.toolOverflow = true
			out, err := translator.Translate(sseEvent(terminal, map[string]any{"type": terminal, "response": map[string]any{"usage": map[string]any{"input_tokens": 3, "output_tokens": 4}}}))
			if err != nil {
				t.Fatal(err)
			}
			encoded := string(EncodeSSE(out))
			if !strings.Contains(encoded, `"type":"error"`) || strings.Contains(encoded, `"type":"message_stop"`) || strings.Contains(encoded, `"type":"message_delta"`) {
				t.Fatalf("overflow followed by normal termination: %s", encoded)
			}
		})
	}
	translator := NewSSETranslator("openai-responses", "anthropic-messages")
	out, err := translator.Translate(SSEEvent{Data: `{"type":"response.incomplete","response":{"usage":{"input_tokens":3,"output_tokens":4}}}`})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(EncodeSSE(out)), `"output_tokens":4`) {
		t.Fatalf("incomplete lost usage: %s", EncodeSSE(out))
	}
}

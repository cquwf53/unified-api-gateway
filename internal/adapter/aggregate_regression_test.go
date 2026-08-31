package adapter

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAggregateResponsesKeepsFunctionCallAndStatus(t *testing.T) {
	raw := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"ping\",\"arguments\":\"\"}}\n\n" +
		"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"delta\":\"{}\"}\n\n" +
		"event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt\",\"status\":\"incomplete\"}}\n\n"
	events, err := ParseSSE(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := AggregateSSE(events, "openai-responses")
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response["status"] != "incomplete" || response["id"] != "resp_1" {
		t.Fatalf("response=%#v", response)
	}
	output := response["output"].([]any)
	if len(output) != 1 || output[0].(map[string]any)["type"] != "function_call" || output[0].(map[string]any)["arguments"] != "{}" {
		t.Fatalf("output=%#v", output)
	}
}

func TestEncodeBufferedSSEUsesContractTerminators(t *testing.T) {
	anthropicBody := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn"}`)
	anthropic, err := EncodeBufferedSSE(anthropicBody, "anthropic-messages")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(anthropic), "data: [DONE]") || !strings.Contains(string(anthropic), "event: message_stop") {
		t.Fatalf("anthropic stream=%s", anthropic)
	}
	responsesBody := []byte(`{"id":"resp_1","object":"response","status":"completed","model":"gpt","output":[]}`)
	responses, err := EncodeBufferedSSE(responsesBody, "openai-responses")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(responses), "data: [DONE]") || !strings.Contains(string(responses), "event: response.completed") {
		t.Fatalf("responses stream=%s", responses)
	}
}

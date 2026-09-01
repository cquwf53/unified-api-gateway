package adapter

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
)

func TestTransparentAdapterReturnsIndependentClones(t *testing.T) {
	in := Envelope{Method: "POST", Header: http.Header{"X-A": []string{"1"}}, Body: []byte(`{}`)}
	out, err := Transparent{}.AdaptRequest(in, config.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	out.Header.Set("X-A", "2")
	out.Body[0] = 'x'
	if in.Header.Get("X-A") != "1" || string(in.Body) != "{}" {
		t.Fatalf("input envelope was mutated: %v %s", in.Header, in.Body)
	}
	back, err := Transparent{}.AdaptResponse(out, config.Profile{})
	if err != nil || back.Body[0] != 'x' {
		t.Fatalf("adapt response: %v %s", err, back.Body)
	}
}

func TestJSONEnvelopeHelpers(t *testing.T) {
	if !isJSONEnvelope(Envelope{ContentType: "application/json; charset=utf-8"}) {
		t.Fatal("json content type must match")
	}
	if !isJSONEnvelope(Envelope{ContentType: "application/vnd.api+json"}) {
		t.Fatal("structured suffix must match")
	}
	if isJSONEnvelope(Envelope{ContentType: "text/plain"}) {
		t.Fatal("text/plain must not match")
	}
	if !isJSONEnvelope(Envelope{}) {
		t.Fatal("empty content type stays compatible")
	}
	if _, err := decodeJSON([]byte("not json")); err == nil {
		t.Fatal("invalid JSON must fail")
	}
	if got := headerOrDefault(http.Header{}, "X-Missing", "fallback"); got != "fallback" {
		t.Fatalf("headerOrDefault=%q", got)
	}
	if got := headerOrDefault(http.Header{"X-Missing": {"v"}}, "X-Missing", "fallback"); got != "v" {
		t.Fatalf("headerOrDefault=%q", got)
	}
}

func TestReadSSEStreamJoinsDataAndKeepsMetadata(t *testing.T) {
	events, err := ParseSSE(strings.NewReader("id: 1\nretry: 120\n: comment\ndata: a\ndata: b\n\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Data != "a\nb" || events[0].ID != "1" || events[0].Retry != "120" {
		t.Fatalf("events=%+v", events)
	}
}

func TestReadSSEStreamBoundsLineAndEventSizes(t *testing.T) {
	bigLine := "data: " + strings.Repeat("a", maxSSELine+1) + "\n\n"
	if _, err := ParseSSE(strings.NewReader(bigLine)); err == nil || !strings.Contains(err.Error(), "line exceeds") {
		t.Fatalf("expected line bound error, got %v", err)
	}
	var builder strings.Builder
	for i := 0; i < (maxSSEEvent/(1<<20))+2; i++ {
		builder.WriteString("data: ")
		builder.WriteString(strings.Repeat("a", 1<<20))
		builder.WriteString("\n")
	}
	if _, err := ParseSSE(strings.NewReader(builder.String())); err == nil || !strings.Contains(err.Error(), "event exceeds") {
		t.Fatalf("expected event bound error, got %v", err)
	}
}

func TestEncodeSSERepeatsMetadataFields(t *testing.T) {
	encoded := EncodeSSE([]SSEEvent{{ID: "7", Event: "ping", Retry: "3000", Data: "one\ntwo"}})
	text := string(encoded)
	for _, want := range []string{"id: 7\n", "event: ping\n", "retry: 3000\n", "data: one\n", "data: two\n"} {
		if !strings.Contains(text, want) {
			t.Fatalf("encoded=%q missing %q", text, want)
		}
	}
	parsed, err := ParseSSE(bytes.NewReader(encoded))
	if err != nil || len(parsed) != 1 || parsed[0].Data != "one\ntwo" {
		t.Fatalf("round trip=%+v err=%v", parsed, err)
	}
}

func TestTranslatorFailSpeaksEachTargetContract(t *testing.T) {
	selfTargeted := NewSSETranslator("openai-chat", "openai-chat")
	events := selfTargeted.Fail("boom")
	if len(events) != 2 || !strings.Contains(events[0].Data, "boom") || events[1].Data != "[DONE]" {
		t.Fatalf("openai fail events=%+v", events)
	}
	if finish := selfTargeted.Finish(); finish != nil {
		t.Fatalf("Finish after Fail must be nil, got %+v", finish)
	}
	anthropicTarget := NewSSETranslator("openai-chat", "anthropic-messages")
	events = anthropicTarget.Fail("")
	if len(events) != 1 || events[0].Event != "error" || !strings.Contains(events[0].Data, "upstream stream terminated unexpectedly") {
		t.Fatalf("anthropic fail events=%+v", events)
	}
	responsesTarget := NewSSETranslator("openai-chat", "openai-responses")
	events = responsesTarget.Fail("boom")
	if len(events) != 1 || events[0].Event != "response.failed" || !strings.Contains(events[0].Data, "boom") {
		t.Fatalf("responses fail events=%+v", events)
	}
}

func TestOpenAIToAnthropicThinkingAndTextBlocks(t *testing.T) {
	translator := NewSSETranslator("openai-chat", "anthropic-messages")
	translate := func(data string) []SSEEvent {
		t.Helper()
		out, err := translator.Translate(SSEEvent{Data: data})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if names(translate(`{"id":"c1","model":"m1","choices":[{"index":0,"delta":{"role":"assistant"}}]}`))[0] != "message_start" {
		t.Fatal("first delta must open the message")
	}
	reasoning := translate(`{"choices":[{"index":0,"delta":{"reasoning_content":"th"}}]}`)
	if names(reasoning)[0] != "content_block_start" || !strings.Contains(reasoning[0].Data, `"thinking"`) ||
		names(reasoning)[1] != "content_block_delta" || !strings.Contains(reasoning[1].Data, "thinking_delta") {
		t.Fatalf("reasoning events=%+v", reasoning)
	}
	text := translate(`{"choices":[{"index":0,"delta":{"content":"hi"}}]}`)
	if names(text)[0] != "content_block_stop" || names(text)[1] != "content_block_start" ||
		!strings.Contains(text[1].Data, `"text"`) || names(text)[2] != "content_block_delta" {
		t.Fatalf("text events=%+v", text)
	}
	finish := translate(`{"choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}`)
	if names(finish)[0] != "content_block_stop" || names(finish)[1] != "message_delta" ||
		!strings.Contains(finish[1].Data, `"refusal"`) {
		t.Fatalf("finish events=%+v", finish)
	}
	// translateDone synthesises the Anthropic terminator for this direction.
	if final := translator.Finish(); len(final) != 1 || final[0].Data != `{"type":"message_stop"}` {
		t.Fatalf("finish events=%+v", final)
	}
}

func TestAnthropicErrorEventTerminatesOpenAIStream(t *testing.T) {
	translator := NewSSETranslator("anthropic-messages", "openai-chat")
	out, err := translator.Translate(SSEEvent{Event: "error", Data: `{"type":"error","error":{"type":"overloaded_error","message":"boom"}}`})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || !strings.Contains(out[0].Data, `"boom"`) || out[1].Data != "[DONE]" {
		t.Fatalf("error events=%+v", out)
	}
	if more, _ := translator.Translate(SSEEvent{Data: `{"type":"message_stop"}`}); more != nil {
		t.Fatalf("post-terminal events=%+v", more)
	}
}

func TestResponsesToAnthropicStreamLifecycle(t *testing.T) {
	translator := NewSSETranslator("openai-responses", "anthropic-messages")
	translate := func(data string) []SSEEvent {
		t.Helper()
		out, err := translator.Translate(SSEEvent{Data: data})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if names(translate(`{"type":"response.created","response":{"id":"resp_9","model":"m"}}`))[0] != "message_start" {
		t.Fatal("response.created must open the anthropic message")
	}
	// The first text delta also opens the text block (index 0).
	if out := translate(`{"type":"response.output_text.delta","delta":"A"}`); len(out) != 2 ||
		out[0].Event != "content_block_start" || out[1].Event != "content_block_delta" || !strings.Contains(out[1].Data, `"A"`) {
		t.Fatalf("text delta=%+v", out)
	}
	// OpenAI->Anthropic tool calls buffer until the stream completes, so the
	// item announcement and argument deltas emit nothing yet.
	if out := translate(`{"type":"response.output_item.added","item":{"id":"fc_1","call_id":"call_1","type":"function_call","name":"f"}}`); len(out) != 0 {
		t.Fatalf("tool start must buffer, got %+v", out)
	}
	if out := translate(`{"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{}"}`); len(out) != 0 {
		t.Fatalf("tool args must buffer, got %+v", out)
	}
	done := translate(`{"type":"response.completed","response":{"usage":{"input_tokens":3,"output_tokens":4}}}`)
	joined := strings.Join(names(done), ",")
	if !strings.Contains(joined, "content_block_start") || !strings.Contains(joined, "message_delta") {
		t.Fatalf("completed events=%+v", done)
	}
	allData := strings.Join(eventData(done), " ")
	if !strings.Contains(allData, `"call_1"`) || !strings.Contains(allData, `"tool_use"`) || !strings.Contains(allData, `"input_tokens":3`) {
		t.Fatalf("completed tool replay=%+v", done)
	}
	if !strings.Contains(joined, "message_stop") {
		t.Fatalf("completed must close the stream: %v", done)
	}
}

func TestResponsesIncompleteMapsToMaxTokensStop(t *testing.T) {
	translator := NewSSETranslator("openai-responses", "anthropic-messages")
	out, err := translator.Translate(SSEEvent{Data: `{"type":"response.incomplete"}`})
	if err != nil {
		t.Fatal(err)
	}
	// A length-limited Responses stream still owes Anthropic clients the
	// message prologue, the max_tokens stop reason and the terminator.
	if len(out) != 3 || out[0].Event != "message_start" ||
		!strings.Contains(out[1].Data, `"max_tokens"`) || out[2].Data != `{"type":"message_stop"}` {
		t.Fatalf("incomplete events=%+v", out)
	}
}

func TestEncodeBufferedSSEOpenAIChat(t *testing.T) {
	body := []byte(`{"id":"c1","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1}}`)
	events, err := ParseSSE(bytes.NewReader(mustEncodeBufferedSSE(t, body, "openai-chat")))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Data != "[DONE]" {
		t.Fatalf("events=%+v", events)
	}
	var chunk map[string]any
	if err := json.Unmarshal([]byte(events[0].Data), &chunk); err != nil {
		t.Fatal(err)
	}
	if chunk["object"] != "chat.completion.chunk" || chunk["usage"] == nil {
		t.Fatalf("chunk=%v", chunk)
	}
}

func TestEncodeBufferedSSEResponsesLifecycle(t *testing.T) {
	body := []byte(`{"id":"r1","object":"response","status":"incomplete","model":"m","output":[{"id":"i1","type":"message","role":"assistant","content":[{"type":"output_text","text":"hi","annotations":[]}]}]}`)
	events, err := ParseSSE(bytes.NewReader(mustEncodeBufferedSSE(t, body, "openai-responses")))
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(names(events), ",")
	for _, want := range []string{"response.created", "response.output_item.added", "response.output_text.delta", "response.incomplete"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in %s", want, joined)
		}
	}
}

func TestEncodeBufferedSSEAnthropicMessage(t *testing.T) {
	body := []byte(`{"id":"m1","type":"message","model":"m","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":1}}`)
	events, err := ParseSSE(bytes.NewReader(mustEncodeBufferedSSE(t, body, "anthropic-messages")))
	if err != nil {
		t.Fatal(err)
	}
	if joined := strings.Join(names(events), ","); !strings.Contains(joined, "message_start,content_block_start,content_block_delta,content_block_stop,message_delta,message_stop") {
		t.Fatalf("events=%s", joined)
	}
}

func TestAggregateSSEResponsesStatuses(t *testing.T) {
	events := []SSEEvent{
		{Event: "response.created", Data: `{"type":"response.created","response":{"id":"r1","model":"m"}}`},
		{Event: "response.output_text.delta", Data: `{"type":"response.output_text.delta","delta":"hi"}`},
		{Event: "response.incomplete", Data: `{"type":"response.incomplete"}`},
	}
	body, contentType, err := AggregateSSE(events, "openai-responses")
	if err != nil || contentType != "application/json" {
		t.Fatalf("aggregate err=%v ct=%q", err, contentType)
	}
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatal(err)
	}
	if value["status"] != "incomplete" || value["output_text"] != "hi" {
		t.Fatalf("aggregate=%v", value)
	}
	failed, _, err := AggregateSSE([]SSEEvent{{Event: "response.failed", Data: `{"type":"response.failed"}`}}, "openai-responses")
	if err != nil {
		t.Fatal(err)
	}
	var failedValue map[string]any
	if err := json.Unmarshal(failed, &failedValue); err != nil || failedValue["status"] != "failed" {
		t.Fatalf("failed aggregate=%v err=%v", failedValue, err)
	}
}

func TestOpenAIStreamErrorEventDefaults(t *testing.T) {
	event := openAIStreamErrorEvent(map[string]any{})
	var value map[string]any
	if err := json.Unmarshal([]byte(event.Data), &value); err != nil {
		t.Fatal(err)
	}
	errObj, _ := value["error"].(map[string]any)
	if errObj == nil || errObj["message"] != "upstream stream error" {
		t.Fatalf("error frame=%v", value)
	}
}

func TestMappingHelpers(t *testing.T) {
	if n, ok := numberValueOK(json.Number("3.5")); !ok || n != 3.5 {
		t.Fatalf("numberValueOK=%v %v", n, ok)
	}
	if _, ok := numberValueOK("3"); ok {
		t.Fatal("strings are not numeric")
	}
	if got := contentText([]any{"a", map[string]any{"text": "b"}, 3}); got != "a\nb" {
		t.Fatalf("contentText=%q", got)
	}
	if got := normalizeStop("x"); got == nil {
		t.Fatal("scalar stop must become a list")
	}
	if got := toolChoiceToOpenAI("auto"); got != "auto" {
		t.Fatalf("toolChoiceToOpenAI passthrough=%v", got)
	}
	if got := anthropicMessageID("chatcmpl-9"); got != "msg_9" {
		t.Fatalf("anthropicMessageID=%q", got)
	}
	if got := anthropicMessageID(""); got != "msg-gateway" {
		t.Fatalf("anthropicMessageID=%q", got)
	}
	if got := openAIResponseID("msg_9"); got != "chatcmpl_9" {
		t.Fatalf("openAIResponseID=%q", got)
	}
	if got := responsesResponseID("msg_9"); got != "resp_9" {
		t.Fatalf("responsesResponseID=%q", got)
	}
	if got := decodeJSONObject("not json"); got == nil {
		t.Fatal("invalid JSON must fall back to a raw wrapper")
	}
	if source := anthropicImageSource("https://example.com/x.png"); source["type"] != "url" {
		t.Fatalf("remote image source=%v", source)
	}
}

func names(events []SSEEvent) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, event.Event)
	}
	return out
}

func eventData(events []SSEEvent) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, event.Data)
	}
	return out
}

func mustEncodeBufferedSSE(t *testing.T, body []byte, contract string) []byte {
	t.Helper()
	encoded, err := EncodeBufferedSSE(body, contract)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

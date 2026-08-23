package adapter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// SSEEvent is a parsed Server-Sent Event frame. Data may contain newlines;
// parsing joins multiple data: lines according to the SSE specification.
type SSEEvent struct {
	ID    string
	Event string
	Data  string
	Retry string
}

// maxSSELine bounds a single SSE line so a hostile or corrupted upstream
// cannot make the gateway accumulate memory without limit.
const maxSSELine = 4 << 20

// ReadSSEStream parses events as they arrive and hands each completed event to
// emit. It returns as soon as emit fails, which lets callers translate and
// flush frame by frame instead of buffering the whole upstream response.
func ReadSSEStream(r io.Reader, emit func(SSEEvent) error) error {
	reader := bufio.NewReaderSize(r, 32<<10)
	var id, event, retry string
	var data []string
	flush := func() error {
		if id == "" && event == "" && retry == "" && len(data) == 0 {
			return nil
		}
		err := emit(SSEEvent{ID: id, Event: event, Data: strings.Join(data, "\n"), Retry: retry})
		id, event, retry = "", "", ""
		data = nil
		return err
	}
	for {
		line, readErr := reader.ReadString('\n')
		if len(line) > maxSSELine {
			return errors.New("sse: line exceeds maximum event size")
		}
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
		} else if !strings.HasPrefix(line, ":") {
			key, value, ok := strings.Cut(line, ":")
			if ok {
				value = strings.TrimPrefix(value, " ")
			}
			switch key {
			case "id":
				id = value
			case "event":
				event = value
			case "data":
				data = append(data, value)
			case "retry":
				retry = value
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return flush()
			}
			return readErr
		}
	}
}

func ParseSSE(r io.Reader) ([]SSEEvent, error) {
	var events []SSEEvent
	err := ReadSSEStream(r, func(event SSEEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func EncodeSSE(events []SSEEvent) []byte {
	var out bytes.Buffer
	for _, event := range events {
		if event.ID != "" {
			fmt.Fprintf(&out, "id: %s\n", event.ID)
		}
		if event.Event != "" {
			fmt.Fprintf(&out, "event: %s\n", event.Event)
		}
		if event.Retry != "" {
			fmt.Fprintf(&out, "retry: %s\n", event.Retry)
		}
		for _, line := range strings.Split(event.Data, "\n") {
			fmt.Fprintf(&out, "data: %s\n", line)
		}
		out.WriteString("\n")
	}
	return out.Bytes()
}

// SSETranslator converts one vendor's streaming events into another
// contract's. A translator is stateful because the OpenAI wire format is a
// flat sequence of deltas while Anthropic requires block open/delta/stop
// framing; create one translator per response stream.
type SSETranslator struct {
	from, to string

	// openai -> anthropic state
	started     bool
	textOpen    bool
	nextBlock   int
	textIndex   int
	blockByTool map[int]int // OpenAI tool_call index -> Anthropic block index
	streamID    string
	model       string
	finishSent  bool
	// createdUnix backs the OpenAI "created" field, which Anthropic streams
	// never carry; it is stamped when the translator is created.
	createdUnix int64

	// doneSent records that the client contract's stream terminator has been
	// emitted ([DONE] for OpenAI clients, message_stop for Anthropic).
	doneSent bool

	// anthropic -> openai state: message_start carries the prompt token count
	// while usage totals only arrive with message_delta.
	promptTokens    float64
	promptTokensSet bool

	// responses-target state: the Responses stream protocol uses
	// response.created/delta/completed events and no [DONE] sentinel.
	respID        string
	respModel     string
	respStarted   bool
	respCompleted bool
	respUsage     map[string]any
	respText      strings.Builder
	respStop      string
	respSeq       int
}

// respEvent stamps the sequence number every Responses protocol event
// carries and wraps the payload in a named SSE event.
func (t *SSETranslator) respEvent(name string, payload map[string]any) SSEEvent {
	t.respSeq++
	payload["sequence_number"] = t.respSeq
	return sseEvent(name, payload)
}

// NewSSETranslator returns a translator between two contract names such as
// "anthropic-messages" and "openai-chat". Identical or unknown contracts
// return a pass-through translator.
func NewSSETranslator(from, to string) *SSETranslator {
	return &SSETranslator{from: strings.ToLower(from), to: strings.ToLower(to), blockByTool: map[int]int{}, nextBlock: -1, createdUnix: time.Now().Unix()}
}

// Translate converts one upstream event into zero or more client events.
func (t *SSETranslator) Translate(event SSEEvent) ([]SSEEvent, error) {
	if t.from == t.to || t.from == "" || t.to == "" {
		return []SSEEvent{event}, nil
	}
	if strings.EqualFold(event.Data, "[DONE]") {
		if t.doneSent {
			return nil, nil
		}
		return t.translateDone(), nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
		// A non-JSON event can still be safely forwarded as an opaque frame.
		return []SSEEvent{event}, nil
	}
	// "openai-responses" contains "openai", so responses contracts must be
	// matched before the generic Chat Completions branches.
	if strings.Contains(t.from, "anthropic") && strings.Contains(t.to, "responses") {
		return t.anthropicToResponses(event, payload), nil
	}
	if strings.Contains(t.from, "responses") && strings.Contains(t.to, "anthropic") {
		return t.responsesToAnthropic(event, payload), nil
	}
	if strings.Contains(t.from, "anthropic") && strings.Contains(t.to, "openai") {
		if t.doneSent {
			return nil, nil // the stream already terminated (error or [DONE])
		}
		if eventType(event, payload) == "error" {
			// OpenAI streams surface mid-stream errors as a {"error":{...}}
			// data frame; the stream then ends. The error must not vanish
			// behind a normal-looking finish.
			t.doneSent = true
			return []SSEEvent{openAIStreamErrorEvent(payload), {Data: "[DONE]"}}, nil
		}
		out, err := t.anthropicEventToOpenAI(event, payload)
		if err != nil {
			return nil, err
		}
		var events []SSEEvent
		if out.Data != "" {
			events = append(events, out)
		}
		// Anthropic streams end with message_stop and never send [DONE], so
		// the sentinel has to be synthesised here; OpenAI SDKs that strictly
		// wait for it would otherwise hang.
		if eventType(event, payload) == "message_stop" && !t.doneSent {
			t.doneSent = true
			events = append(events, SSEEvent{Data: "[DONE]"})
		}
		if len(events) == 0 {
			return nil, nil
		}
		return events, nil
	}
	if strings.Contains(t.from, "openai") && strings.Contains(t.to, "anthropic") {
		return t.openAIEventToAnthropic(event, payload), nil
	}
	return []SSEEvent{event}, nil
}

func (t *SSETranslator) translateDone() []SSEEvent {
	t.doneSent = true
	if strings.Contains(t.to, "responses") {
		// Responses streams terminate with response.completed, not [DONE].
		return t.responsesTerminal()
	}
	if !strings.Contains(t.from, "openai") || !strings.Contains(t.to, "anthropic") {
		return []SSEEvent{{Data: "[DONE]"}}
	}
	var out []SSEEvent
	out = append(out, t.closeTextBlock()...)
	out = append(out, sseEvent("message_stop", map[string]any{"type": "message_stop"}))
	return out
}

// Finish emits the client contract's stream terminator when the upstream
// ended without one (an Anthropic stream closed after message_stop already
// sent [DONE]; an OpenAI stream that never sent [DONE] still owes Anthropic
// clients a message_stop). Call it once after the last upstream event.
func (t *SSETranslator) Finish() []SSEEvent {
	if t.from == t.to || t.from == "" || t.to == "" || t.doneSent {
		return nil
	}
	if strings.Contains(t.from, "anthropic") && strings.Contains(t.to, "openai") {
		t.doneSent = true
		return []SSEEvent{{Data: "[DONE]"}}
	}
	if strings.Contains(t.from, "openai") && strings.Contains(t.to, "anthropic") {
		return t.translateDone()
	}
	if strings.Contains(t.to, "responses") {
		t.doneSent = true
		return t.responsesTerminal()
	}
	return nil
}

func eventType(event SSEEvent, payload map[string]any) string {
	if event.Event != "" {
		return event.Event
	}
	return asString(payload["type"])
}

// anthropicToResponses converts Anthropic events into the OpenAI Responses
// streaming protocol, including the full item/part lifecycle and a terminal
// response.completed that carries the complete assembled message (many
// clients read event.response instead of accumulating deltas).
func (t *SSETranslator) anthropicToResponses(event SSEEvent, payload map[string]any) []SSEEvent {
	switch eventType(event, payload) {
	case "message_start":
		if message, ok := payload["message"].(map[string]any); ok {
			if id := asString(message["id"]); id != "" {
				t.respID = id
			}
			if model := asString(message["model"]); model != "" {
				t.respModel = model
			}
			if usage, ok := message["usage"].(map[string]any); ok {
				t.respUsage = map[string]any{"input_tokens": numberOrZero(usage["input_tokens"]), "output_tokens": numberOrZero(usage["output_tokens"])}
			}
		}
		if t.respStarted {
			return nil
		}
		t.respStarted = true
		return []SSEEvent{
			t.respEvent("response.created", map[string]any{"type": "response.created", "response": t.responsesResponse("in_progress")}),
			t.respEvent("response.in_progress", map[string]any{"type": "response.in_progress", "response": t.responsesResponse("in_progress")}),
			t.respEvent("response.output_item.added", map[string]any{
				"type": "response.output_item.added", "output_index": 0,
				"item": t.responsesMessageItem("in_progress"),
			}),
			t.respEvent("response.content_part.added", map[string]any{
				"type": "response.content_part.added", "item_id": responsesItemID, "output_index": 0, "content_index": 0,
				"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
			}),
		}
	case "content_block_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			if text := asString(d["text"]); text != "" {
				t.respText.WriteString(text)
				return []SSEEvent{t.respEvent("response.output_text.delta", map[string]any{
					"type": "response.output_text.delta", "item_id": responsesItemID, "output_index": 0, "content_index": 0, "delta": text,
				})}
			}
		}
		return nil
	case "message_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			if reason := asString(d["stop_reason"]); reason != "" {
				t.respStop = reason
			}
		}
		if usage, ok := payload["usage"].(map[string]any); ok {
			// message_start carries the prompt tokens while message_delta
			// only reports the completion side; merge instead of overwriting.
			merged := map[string]any{}
			for key, value := range t.respUsage {
				merged[key] = value
			}
			if n, ok := numberValueOK(usage["input_tokens"]); ok {
				merged["input_tokens"] = n
			}
			if n, ok := numberValueOK(usage["output_tokens"]); ok {
				merged["output_tokens"] = n
			}
			t.respUsage = merged
		}
		return nil
	case "message_stop":
		t.doneSent = true
		return t.responsesTerminal()
	case "error":
		return t.responsesFailed(payload)
	default:
		return nil
	}
}

// responsesItemID is the stable identifier linking output_item events with
// their content parts and text deltas.
const responsesItemID = "msg_0"

// responsesMessageItem builds the message output item for item.added/done and
// the completed response's output array.
func (t *SSETranslator) responsesMessageItem(status string) map[string]any {
	item := map[string]any{"id": responsesItemID, "type": "message", "role": "assistant", "status": status}
	if status == "completed" {
		item["content"] = []any{map[string]any{"type": "output_text", "text": t.respText.String(), "annotations": []any{}}}
	} else {
		item["content"] = []any{}
	}
	return item
}

// responsesFailed maps an upstream Anthropic error event onto the Responses
// failure event.
func (t *SSETranslator) responsesFailed(payload map[string]any) []SSEEvent {
	t.doneSent = true
	response := t.responsesResponse("failed")
	if errPayload, ok := payload["error"].(map[string]any); ok {
		response["error"] = errPayload
	}
	return []SSEEvent{t.respEvent("response.failed", map[string]any{"type": "response.failed", "response": response})}
}

// responsesResponse builds the response object embedded in Responses events.
func (t *SSETranslator) responsesResponse(status string) map[string]any {
	id := t.respID
	if id == "" {
		id = "resp-gateway"
	}
	model := t.respModel
	if model == "" {
		model = "gateway"
	}
	return map[string]any{"id": id, "object": "response", "status": status, "model": model}
}

// responsesTerminal emits the closing output_text.done, content_part.done,
// output_item.done and the final completed/incomplete event (with the
// accumulated text and usage). It is idempotent per translator.
func (t *SSETranslator) responsesTerminal() []SSEEvent {
	if t.respCompleted {
		return nil
	}
	t.respCompleted = true
	usage := t.respUsage
	response := t.responsesResponse("completed")
	finalEvent := "response.completed"
	if t.respStop == "max_tokens" {
		// A length-limited generation terminates with response.incomplete.
		finalEvent = "response.incomplete"
		response["status"] = "incomplete"
		response["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
	}
	if usage != nil {
		total := numberOrZero(usage["input_tokens"]) + numberOrZero(usage["output_tokens"])
		usage = map[string]any{"input_tokens": numberOrZero(usage["input_tokens"]), "output_tokens": numberOrZero(usage["output_tokens"]), "total_tokens": total}
		response["usage"] = usage
	}
	item := t.responsesMessageItem("completed")
	response["output"] = []any{item}
	return []SSEEvent{
		t.respEvent("response.output_text.done", map[string]any{
			"type": "response.output_text.done", "item_id": responsesItemID, "output_index": 0, "content_index": 0,
			"text": t.respText.String(),
		}),
		t.respEvent("response.content_part.done", map[string]any{
			"type": "response.content_part.done", "item_id": responsesItemID, "output_index": 0, "content_index": 0,
			"part": map[string]any{"type": "output_text", "text": t.respText.String(), "annotations": []any{}},
		}),
		t.respEvent("response.output_item.done", map[string]any{
			"type": "response.output_item.done", "output_index": 0, "item": item,
		}),
		t.respEvent(finalEvent, map[string]any{"type": finalEvent, "response": response}),
	}
}

// responsesToAnthropic converts a Responses stream into Anthropic events by
// reshaping each Responses event into a synthetic Chat chunk and reusing the
// well-tested Chat -> Anthropic state machine.
func (t *SSETranslator) responsesToAnthropic(event SSEEvent, payload map[string]any) []SSEEvent {
	chunk := map[string]any{"choices": []any{}}
	switch eventType(event, payload) {
	case "response.created":
		if response, ok := payload["response"].(map[string]any); ok {
			chunk["id"] = response["id"]
			chunk["model"] = response["model"]
		}
		chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant"}}}
	case "response.output_text.delta":
		chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{"content": payload["delta"]}}}
	case "response.completed":
		if response, ok := payload["response"].(map[string]any); ok {
			if usage, ok := response["usage"].(map[string]any); ok {
				chunk["usage"] = map[string]any{"prompt_tokens": numberOrZero(usage["input_tokens"]), "completion_tokens": numberOrZero(usage["output_tokens"])}
			}
		}
		return append(t.openAIEventToAnthropic(SSEEvent{Data: ""}, map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}}),
			t.openAIEventToAnthropic(SSEEvent{Data: ""}, chunk)...)
	case "response.failed":
		message := "upstream request failed"
		if response, ok := payload["response"].(map[string]any); ok {
			if errPayload, ok := response["error"].(map[string]any); ok {
				if text := asString(errPayload["message"]); text != "" {
					message = text
				}
			}
		}
		return []SSEEvent{sseEvent("error", map[string]any{"type": "error", "error": map[string]any{"type": "upstream_error", "message": message}})}
	default:
		return nil
	}
	return t.openAIEventToAnthropic(event, chunk)
}

func (t *SSETranslator) openAIEventToAnthropic(event SSEEvent, payload map[string]any) []SSEEvent {
	if errPayload, ok := payload["error"].(map[string]any); ok {
		// Anthropic streams report failures as an error event and close the
		// stream; a normal message_stop afterwards would disguise the failure.
		t.doneSent = true
		t.finishSent = true
		errType := asString(errPayload["type"])
		if errType == "" {
			errType = "api_error"
		}
		message := asString(errPayload["message"])
		if message == "" {
			message = "upstream error"
		}
		return []SSEEvent{sseEvent("error", map[string]any{"type": "error", "error": map[string]any{"type": errType, "message": message}})}
	}
	var out []SSEEvent
	if asString(payload["id"]) != "" {
		t.streamID = asString(payload["id"])
	}
	if asString(payload["model"]) != "" {
		t.model = asString(payload["model"])
	}
	choices := asSlice(payload["choices"])
	var delta, finish any
	if len(choices) > 0 {
		if choice, ok := choices[0].(map[string]any); ok {
			delta = choice["delta"]
			finish = choice["finish_reason"]
		}
	}
	if usage, ok := payload["usage"].(map[string]any); ok && len(choices) == 0 {
		out = append(out, t.ensureStarted(payload)...)
		out = append(out, sseEvent("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": nil},
			"usage": openAIUsageToAnthropic(usage),
		}))
		return out
	}
	if d, ok := delta.(map[string]any); ok {
		if role := asString(d["role"]); role != "" {
			out = append(out, t.ensureStarted(payload)...)
		}
		if content := asString(d["content"]); content != "" {
			out = append(out, t.ensureStarted(payload)...)
			out = append(out, t.openTextBlock()...)
			out = append(out, sseEvent("content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": t.currentTextIndex(),
				"delta": map[string]any{"type": "text_delta", "text": content},
			}))
		}
		if calls, ok := d["tool_calls"].([]any); ok {
			out = append(out, t.translateToolCallDeltas(calls)...)
		}
	}
	if finish != nil && !t.finishSent {
		out = append(out, t.ensureStarted(payload)...)
		out = append(out, t.closeTextBlock()...)
		out = append(out, sseEvent("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": mapFinishReason(asString(finish)), "stop_sequence": nil},
		}))
		t.finishSent = true
	}
	return out
}

func (t *SSETranslator) translateToolCallDeltas(calls []any) []SSEEvent {
	var out []SSEEvent
	for _, raw := range calls {
		call, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		index := 0
		if n, ok := numberValueOK(call["index"]); ok {
			index = int(n)
		}
		fn, _ := call["function"].(map[string]any)
		block, seen := t.blockByTool[index]
		if !seen {
			t.nextBlock++
			block = t.nextBlock
			t.blockByTool[index] = block
			out = append(out, t.closeTextBlock()...)
			start := map[string]any{
				"type":          "content_block_start",
				"index":         block,
				"content_block": map[string]any{"type": "tool_use", "id": asString(call["id"]), "name": asString(fn["name"]), "input": map[string]any{}},
			}
			out = append(out, sseEvent("content_block_start", start))
		}
		if args := asString(fn["arguments"]); args != "" {
			out = append(out, sseEvent("content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": block,
				"delta": map[string]any{"type": "input_json_delta", "partial_json": args},
			}))
		}
	}
	return out
}

func (t *SSETranslator) ensureStarted(payload map[string]any) []SSEEvent {
	if t.started {
		return nil
	}
	t.started = true
	// Anthropic block indexes start at 0; openTextBlock/translateToolCallDeltas
	// increment before use, so the counter starts at -1.
	t.nextBlock = -1
	id := t.streamID
	if id == "" {
		id = "gateway-stream"
	}
	model := t.model
	if model == "" {
		model = asString(payload["model"])
	}
	return []SSEEvent{sseEvent("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": id, "type": "message", "role": "assistant", "model": model,
			"content": []any{}, "usage": map[string]any{"input_tokens": 0},
		},
	})}
}

func (t *SSETranslator) openTextBlock() []SSEEvent {
	if t.textOpen {
		return nil
	}
	t.textOpen = true
	t.nextBlock++
	t.textIndex = t.nextBlock
	return []SSEEvent{sseEvent("content_block_start", map[string]any{
		"type": "content_block_start", "index": t.textIndex,
		"content_block": map[string]any{"type": "text", "text": ""},
	})}
}

func (t *SSETranslator) closeTextBlock() []SSEEvent {
	if !t.textOpen {
		return nil
	}
	t.textOpen = false
	return []SSEEvent{sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": t.textIndex})}
}

func (t *SSETranslator) currentTextIndex() int {
	if t.textIndex >= 0 {
		return t.textIndex
	}
	return 0
}

func sseEvent(name string, payload map[string]any) SSEEvent {
	b, _ := json.Marshal(payload)
	return SSEEvent{Event: name, Data: string(b)}
}

func mapFinishReason(reason string) string {
	switch reason {
	case "stop", "":
		return "end_turn"
	case "length":
		return "max_tokens"
	case "content_filter":
		return "refusal"
	case "tool_calls":
		return "tool_calls"
	default:
		return reason
	}
}

// openAIStreamErrorEvent reshapes an upstream error payload into the
// {"error":{...}} data frame OpenAI streams use for mid-stream failures.
func openAIStreamErrorEvent(payload map[string]any) SSEEvent {
	errObj, _ := payload["error"].(map[string]any)
	out := map[string]any{}
	if errObj != nil {
		if message := asString(errObj["message"]); message != "" {
			out["message"] = message
		}
		if errType := asString(errObj["type"]); errType != "" {
			out["type"] = errType
		}
		if code := errObj["code"]; code != nil {
			out["code"] = code
		}
	}
	if out["type"] == nil {
		if errType := asString(payload["type"]); errType != "" && errType != "error" {
			out["type"] = errType
		}
	}
	if out["message"] == nil {
		out["message"] = "upstream stream error"
	}
	b, _ := json.Marshal(map[string]any{"error": out})
	return SSEEvent{Data: string(b)}
}

func openAIUsageToAnthropic(usage map[string]any) map[string]any {
	out := map[string]any{}
	if v, ok := usage["prompt_tokens"]; ok {
		out["input_tokens"] = v
	}
	if v, ok := usage["completion_tokens"]; ok {
		out["output_tokens"] = v
	}
	return out
}

func anthropicUsageToOpenAI(usage map[string]any) map[string]any {
	out := map[string]any{
		"prompt_tokens":     numberOrZero(usage["input_tokens"]),
		"completion_tokens": numberOrZero(usage["output_tokens"]),
	}
	out["total_tokens"] = sumNumbers(out["prompt_tokens"], out["completion_tokens"])
	return out
}

func numberOrZero(value any) float64 {
	n, _ := numberValueOK(value)
	return n
}

// numberValueOK reports whether a decoded JSON value is numeric, unlike
// numberValue which silently returns zero for other types.
func numberValueOK(value any) (float64, bool) {
	switch n := value.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		parsed, err := n.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

// anthropicEventToOpenAI converts one Anthropic event into an OpenAI chunk.
// A zero Data result means the event has no OpenAI equivalent and is dropped
// (pings, text block framing, a second finish marker).
func (t *SSETranslator) anthropicEventToOpenAI(event SSEEvent, payload map[string]any) (SSEEvent, error) {
	typ := event.Event
	if typ == "" {
		typ = asString(payload["type"])
	}
	if typ == "ping" || typ == "content_block_stop" {
		return SSEEvent{}, nil
	}
	// OpenAI chunks require non-null id/created/model, but only message_start
	// carries them in an Anthropic stream; latch once and reuse.
	id, model := payload["id"], payload["model"]
	if id == nil {
		if t.streamID != "" {
			id = t.streamID
		} else {
			id = "chatcmpl-gateway"
		}
	}
	if model == nil {
		if t.model != "" {
			model = t.model
		} else {
			model = "gateway"
		}
	}
	created := payload["created"]
	if created == nil {
		created = t.createdUnix
	}
	chunk := map[string]any{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []any{map[string]any{
			"index": 0,
			"delta": map[string]any{},
		}},
	}
	choices := chunk["choices"].([]any)
	choice := choices[0].(map[string]any)
	delta := choice["delta"].(map[string]any)
	switch typ {
	case "message_start":
		if message, ok := payload["message"].(map[string]any); ok {
			if streamID := asString(message["id"]); streamID != "" {
				t.streamID = streamID
				chunk["id"] = streamID
			}
			if streamModel := asString(message["model"]); streamModel != "" {
				t.model = streamModel
				chunk["model"] = streamModel
			}
			if usage, ok := message["usage"].(map[string]any); ok {
				if n, ok := numberValueOK(usage["input_tokens"]); ok {
					t.promptTokens = n
					t.promptTokensSet = true
				}
			}
		}
		delta["role"] = "assistant"
	case "content_block_start":
		block, _ := payload["content_block"].(map[string]any)
		if block == nil || asString(block["type"]) != "tool_use" {
			// Text block framing has no OpenAI equivalent; only tool_use
			// starts carry a tool_call delta.
			return SSEEvent{}, nil
		}
		index := 0
		if n, ok := numberValueOK(payload["index"]); ok {
			index = int(n)
		}
		delta["tool_calls"] = []any{map[string]any{
			"index": index, "id": block["id"], "type": "function",
			"function": map[string]any{"name": block["name"], "arguments": ""},
		}}
	case "content_block_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			if text := asString(d["text"]); text != "" {
				delta["content"] = text
			}
			if d["type"] == "input_json_delta" {
				index := 0
				if n, ok := numberValueOK(payload["index"]); ok {
					index = int(n)
				}
				delta["tool_calls"] = []any{map[string]any{
					"index": index, "function": map[string]any{"arguments": asString(d["partial_json"])},
				}}
			}
		}
	case "message_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			choice["finish_reason"] = openAIFinishReason(asString(d["stop_reason"]))
			t.finishSent = true
		}
		if usage, ok := payload["usage"].(map[string]any); ok {
			if _, present := usage["input_tokens"]; !present && t.promptTokensSet {
				usage["input_tokens"] = t.promptTokens
			}
			chunk["usage"] = anthropicUsageToOpenAI(usage)
		}
	case "message_stop":
		// The OpenAI protocol allows exactly one finish marker; message_delta
		// already produced one, so only synthesize "stop" when it did not.
		if t.finishSent {
			return SSEEvent{}, nil
		}
		t.finishSent = true
		choice["finish_reason"] = "stop"
	}
	b, _ := json.Marshal(chunk)
	return SSEEvent{Data: string(b)}, nil
}

// TranslateSSE translates a complete event slice. Unknown events are preserved
// with their data so vendor extensions do not disappear silently.
func TranslateSSE(events []SSEEvent, from, to string) ([]SSEEvent, error) {
	translator := NewSSETranslator(from, to)
	var result []SSEEvent
	for _, event := range events {
		out, err := translator.Translate(event)
		if err != nil {
			return nil, err
		}
		result = append(result, out...)
	}
	result = append(result, translator.Finish()...)
	return result, nil
}

type aggregatedTool struct {
	id   string
	name string
	args strings.Builder
}

// AggregateSSE reconstructs a non-streaming JSON response from translated
// events. Text, tool calls and finish/usage metadata are retained, while
// unknown event payloads are ignored rather than invented.
func AggregateSSE(events []SSEEvent, contract string) ([]byte, string, error) {
	contract = strings.ToLower(contract)
	var text strings.Builder
	var id, model, finish string
	var usage map[string]any
	tools := map[int]*aggregatedTool{}
	anthropicBlocks := map[int]*aggregatedTool{}

	for _, event := range events {
		if event.Data == "" || event.Data == "[DONE]" {
			continue
		}
		var payload map[string]any
		if json.Unmarshal([]byte(event.Data), &payload) != nil {
			continue
		}
		if id == "" {
			id = asString(payload["id"])
		}
		if model == "" {
			model = asString(payload["model"])
		}
		if value, ok := payload["usage"].(map[string]any); ok {
			// Streams report token counts across separate events (prompt in
			// message_start, completion in message_delta); merge instead of
			// overwriting so totals survive.
			if usage == nil {
				usage = map[string]any{}
				for k, v := range value {
					usage[k] = v
				}
			} else {
				for k, v := range value {
					if _, exists := usage[k]; !exists || v != nil {
						usage[k] = v
					}
				}
			}
		}
		aggregateOpenAIChunk(payload, &text, tools, &finish)
		aggregateAnthropicEvent(payload, &text, anthropicBlocks, &finish)
	}
	if id == "" {
		id = "gateway-stream"
	}
	if model == "" {
		model = "gateway"
	}
	toolCalls := orderedToolCalls(tools)
	if strings.Contains(contract, "anthropic") {
		content := []any{}
		if text.Len() > 0 {
			content = append(content, map[string]any{"type": "text", "text": text.String()})
		}
		for _, tool := range orderedTools(anthropicBlocks) {
			var input any = map[string]any{}
			if args := tool.args.String(); args != "" {
				decoded := json.Unmarshal([]byte(args), &input)
				if decoded != nil {
					input = map[string]any{"raw": args}
				}
			}
			content = append(content, map[string]any{"type": "tool_use", "id": tool.id, "name": tool.name, "input": input})
		}
		if finish == "" && len(content) == 0 {
			finish = "end_turn"
		}
		body := map[string]any{"id": id, "type": "message", "role": "assistant", "model": model, "content": content, "stop_reason": finish, "usage": usage}
		data, err := json.Marshal(body)
		return data, "application/json", err
	}
	if strings.Contains(contract, "responses") {
		output := []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": text.String()}}}}
		body := map[string]any{"id": id, "object": "response", "status": "completed", "model": model, "output": output, "usage": usage}
		data, err := json.Marshal(body)
		return data, "application/json", err
	}
	message := map[string]any{"role": "assistant", "content": text.String()}
	if len(toolCalls) > 0 {
		message["tool_calls"] = toolCalls
		if finish == "" {
			finish = "tool_calls"
		}
	}
	if finish == "" {
		finish = "stop"
	}
	body := map[string]any{"id": id, "object": "chat.completion", "model": model, "choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}}, "usage": usage}
	data, err := json.Marshal(body)
	return data, "application/json", err
}

func aggregateOpenAIChunk(payload map[string]any, text *strings.Builder, tools map[int]*aggregatedTool, finish *string) {
	choices, ok := payload["choices"].([]any)
	if !ok || len(choices) == 0 {
		return
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return
	}
	if value := asString(choice["finish_reason"]); value != "" {
		*finish = value
	}
	delta, ok := choice["delta"].(map[string]any)
	if !ok {
		return
	}
	text.WriteString(asString(delta["content"]))
	for _, raw := range asSlice(delta["tool_calls"]) {
		call, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		index := 0
		if n, ok := numberValueOK(call["index"]); ok {
			index = int(n)
		}
		tool := tools[index]
		if tool == nil {
			tool = &aggregatedTool{id: asString(call["id"])}
			tools[index] = tool
		}
		if call["id"] != nil && asString(call["id"]) != "" {
			tool.id = asString(call["id"])
		}
		if fn, ok := call["function"].(map[string]any); ok {
			if name := asString(fn["name"]); name != "" {
				tool.name = name
			}
			tool.args.WriteString(asString(fn["arguments"]))
		}
	}
}

func aggregateAnthropicEvent(payload map[string]any, text *strings.Builder, blocks map[int]*aggregatedTool, finish *string) {
	typ := asString(payload["type"])
	switch typ {
	case "content_block_start":
		if block, ok := payload["content_block"].(map[string]any); ok && asString(block["type"]) == "tool_use" {
			index := 0
			if n, ok := numberValueOK(payload["index"]); ok {
				index = int(n)
			}
			blocks[index] = &aggregatedTool{id: asString(block["id"]), name: asString(block["name"])}
		}
	case "content_block_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			if asString(d["type"]) == "text_delta" {
				text.WriteString(asString(d["text"]))
			}
			if asString(d["type"]) == "input_json_delta" {
				index := 0
				if n, ok := numberValueOK(payload["index"]); ok {
					index = int(n)
				}
				if block := blocks[index]; block != nil {
					block.args.WriteString(asString(d["partial_json"]))
				}
			}
		}
	case "message_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			if reason := asString(d["stop_reason"]); reason != "" {
				*finish = reason
			}
		}
	}
}

func orderedTools(tools map[int]*aggregatedTool) []*aggregatedTool {
	indexes := make([]int, 0, len(tools))
	for index := range tools {
		indexes = append(indexes, index)
	}
	for i := 1; i < len(indexes); i++ {
		for j := i; j > 0 && indexes[j-1] > indexes[j]; j-- {
			indexes[j-1], indexes[j] = indexes[j], indexes[j-1]
		}
	}
	out := make([]*aggregatedTool, 0, len(indexes))
	for _, index := range indexes {
		out = append(out, tools[index])
	}
	return out
}

func orderedToolCalls(tools map[int]*aggregatedTool) []any {
	out := make([]any, 0, len(tools))
	for _, tool := range orderedTools(tools) {
		out = append(out, map[string]any{
			"id": tool.id, "type": "function",
			"function": map[string]any{"name": tool.name, "arguments": tool.args.String()},
		})
	}
	return out
}

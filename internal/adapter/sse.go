package adapter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// toolCallBuffer accumulates one OpenAI tool call before it is replayed as
// a complete Anthropic tool_use block.
type toolCallBuffer struct {
	id   string
	name string
	args strings.Builder
}

// maxBufferedToolArgs caps one tool call while maxBufferedToolArgsTotal bounds
// all calls in one stream so parallel tools cannot multiply memory usage.
const (
	maxBufferedToolArgs      = 4 << 20
	maxBufferedToolArgsTotal = 16 << 20
)

// SSEEvent is a parsed Server-Sent Event frame. Data may contain newlines;
// parsing joins multiple data: lines according to the SSE specification.
type SSEEvent struct {
	ID    string
	Event string
	Data  string
	Retry string
}

// maxSSELine bounds a single SSE line and maxSSEEvent the total data one
// event may accumulate across its data: lines, so a hostile or corrupted
// upstream cannot make the gateway buffer without limit.
const (
	maxSSELine  = 4 << 20
	maxSSEEvent = 16 << 20
	// Bound per-line bookkeeping as well as payload bytes; empty data values
	// still allocate a string slot and contribute a joining newline.
	maxSSEDataLines = 64 << 10
)

// ReadSSEStream parses events as they arrive and hands each completed event
// to emit. It returns as soon as emit fails, which lets callers translate and
// flush frame by frame instead of buffering the whole upstream response.
// Lines and accumulated events are size-bounded so a hostile upstream cannot
// exhaust memory.
func ReadSSEStream(r io.Reader, emit func(SSEEvent) error) error {
	reader := bufio.NewReaderSize(r, 32<<10)
	var id, event, retry string
	var data []string
	dataBytes := 0
	flush := func() error {
		if id == "" && event == "" && retry == "" && len(data) == 0 {
			return nil
		}
		err := emit(SSEEvent{ID: id, Event: event, Data: strings.Join(data, "\n"), Retry: retry})
		id, event, retry = "", "", ""
		data, dataBytes = nil, 0
		return err
	}
	var line []byte
	for {
		fragment, readErr := reader.ReadSlice('\n')
		line = append(line, fragment...)
		if len(line) > maxSSELine {
			return errors.New("sse: line exceeds maximum event size")
		}
		if readErr == bufio.ErrBufferFull {
			continue // long line, keep accumulating within the cap
		}
		processed := strings.TrimSuffix(string(line), "\n")
		processed = strings.TrimSuffix(processed, "\r")
		line = line[:0]
		if processed == "" {
			if err := flush(); err != nil {
				return err
			}
		} else if !strings.HasPrefix(processed, ":") {
			key, value, ok := strings.Cut(processed, ":")
			if ok {
				value = strings.TrimPrefix(value, " ")
			}
			switch key {
			case "id":
				id = value
			case "event":
				event = value
			case "data":
				dataBytes += len(value) + 1
				if dataBytes > maxSSEEvent || len(data) >= maxSSEDataLines {
					return errors.New("sse: event exceeds maximum accumulated size")
				}
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
	started       bool
	textOpen      bool
	thinkingOpen  bool
	nextBlock     int
	textIndex     int
	thinkingIndex int
	// toolBuffers accumulates every tool call (OpenAI tool_calls index ->
	// call). Anthropic's block format is strictly sequential, so tool calls
	// are replayed as complete blocks at the end of the stream in index
	// order; this stays lossless even for interleaved parallel tool deltas.
	toolBuffers      map[int]*toolCallBuffer
	toolOverflow     bool
	bufferedToolArgs int
	streamID         string
	model            string
	finishSent       bool
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
	// toolCallIndexByBlock maps Anthropic block indexes (text and tool blocks
	// share one sequence) onto OpenAI tool_calls array positions, which are a
	// separate numbering space.
	toolCallIndexByBlock map[int]int
	toolCallCount        int

	// responses-target state: the Responses stream protocol uses
	// response.created/delta/completed events and no [DONE] sentinel.
	respID             string
	respModel          string
	respStarted        bool
	respCompleted      bool
	respUsage          map[string]any
	respText           strings.Builder
	respReasoning      strings.Builder
	respReasoningID    string
	respReasoningIndex int
	respReasoningAdded bool
	respStop           string
	respSeq            int
	// respToolItems holds the function_call output items assembled from
	// Anthropic tool_use blocks, in output order.
	respToolItems []map[string]any
	// respPartAdded records whether the text content part was announced;
	// pure-tool streams skip the part lifecycle entirely.
	respPartAdded bool
	// itemID is this stream's message item identifier.
	itemID string
	// Output indexes are assigned in actual item-announcement order. This keeps
	// pure-tool streams zero-based and avoids collisions when tools precede text.
	respNextOutputIndex int
	respMessageIndex    int
	respToolIndexes     []int
	// respItemAdded records whether the assistant message output item was
	// announced; pure-tool streams skip it entirely.
	respItemAdded bool
	// toolCallByItem maps Responses function-call item ids onto OpenAI
	// tool_calls array positions (responses -> anthropic direction), and
	// toolArgsSeen records whether deltas already delivered the arguments
	// (the done event alone must otherwise carry them).
	toolCallByItem map[string]int
	toolArgsSeen   map[string]bool
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
var itemIDSequence atomic.Uint64

// nextResponsesItemID returns a stream-unique message item id; consumers
// that key on item ids across streams cannot collide on a fixed value.
func nextResponsesItemID() string {
	return fmt.Sprintf("msg_%d", itemIDSequence.Add(1))
}

func NewSSETranslator(from, to string) *SSETranslator {
	return &SSETranslator{
		from: strings.ToLower(from), to: strings.ToLower(to),
		toolBuffers: map[int]*toolCallBuffer{}, toolCallIndexByBlock: map[int]int{},
		toolCallByItem: map[string]int{}, toolArgsSeen: map[string]bool{}, nextBlock: -1, createdUnix: time.Now().Unix(),
		respMessageIndex: -1, itemID: nextResponsesItemID(),
	}
}

// Translate converts one upstream event into zero or more client events.
func (t *SSETranslator) Translate(event SSEEvent) ([]SSEEvent, error) {
	if t.doneSent {
		return nil, nil
	}
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
	out = append(out, t.closeThinkingBlock()...)
	out = append(out, t.flushBufferedTools()...)
	if t.toolOverflow {
		// The overflow error is terminal. A normal message_stop afterwards
		// would disguise the failed tool call as a successful completion.
		t.finishSent = true
		return out
	}
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
	if strings.Contains(t.to, "responses") {
		t.doneSent = true
		return t.responsesTerminal()
	}
	if strings.Contains(t.from, "anthropic") && strings.Contains(t.to, "openai") {
		t.doneSent = true
		return []SSEEvent{{Data: "[DONE]"}}
	}
	if strings.Contains(t.from, "openai") && strings.Contains(t.to, "anthropic") {
		return t.translateDone()
	}
	return nil
}

// Fail terminates a stream after an abnormal read/translation failure. It
// deliberately emits only the target protocol's failure vocabulary and marks
// the translator terminal so Finish cannot fabricate a successful completion.
func (t *SSETranslator) Fail(message string) []SSEEvent {
	if t == nil || t.doneSent {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		message = "upstream stream terminated unexpectedly"
	}
	t.doneSent = true
	t.finishSent = true
	switch {
	case strings.Contains(t.to, "responses"):
		response := t.responsesResponse("failed")
		response["error"] = map[string]any{"type": "upstream_error", "message": message}
		return []SSEEvent{t.respEvent("response.failed", map[string]any{"type": "response.failed", "response": response})}
	case strings.Contains(t.to, "anthropic"):
		return []SSEEvent{sseEvent("error", map[string]any{"type": "error", "error": map[string]any{"type": "upstream_error", "message": message}})}
	case strings.Contains(t.to, "openai"):
		b, _ := json.Marshal(map[string]any{"error": map[string]any{"type": "upstream_error", "message": message}})
		return []SSEEvent{{Data: string(b)}, {Data: "[DONE]"}}
	default:
		return nil
	}
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
	if t.doneSent {
		return nil
	}
	switch eventType(event, payload) {
	case "message_start":
		if message, ok := payload["message"].(map[string]any); ok {
			if id := asString(message["id"]); id != "" {
				t.respID = responsesResponseID(id)
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
		// The assistant message output item is announced lazily with the
		// first text delta; pure-tool streams carry only function_call
		// items and no empty message.
		return []SSEEvent{
			t.respEvent("response.created", map[string]any{"type": "response.created", "response": t.responsesResponse("in_progress")}),
			t.respEvent("response.in_progress", map[string]any{"type": "response.in_progress", "response": t.responsesResponse("in_progress")}),
		}
		// The text content part is announced lazily with the first delta so
		// pure-tool streams never open a part they cannot close.
	case "content_block_start":
		block, _ := payload["content_block"].(map[string]any)
		if block == nil || asString(block["type"]) != "tool_use" {
			return nil // text framing was already emitted in the prologue
		}
		callID := asString(block["id"])
		itemID := "fc_" + callID
		item := map[string]any{
			"id": itemID, "call_id": callID, "type": "function_call",
			"name": block["name"], "arguments": "", "status": "in_progress",
		}
		t.respToolItems = append(t.respToolItems, item)
		outputIndex := t.respNextOutputIndex
		t.respNextOutputIndex++
		t.respToolIndexes = append(t.respToolIndexes, outputIndex)
		return []SSEEvent{t.respEvent("response.output_item.added", map[string]any{
			"type": "response.output_item.added", "output_index": outputIndex, "item": item,
		})}
	case "content_block_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			if asString(d["type"]) == "thinking_delta" {
				return t.responsesReasoningDelta(asString(d["thinking"]))
			}
			if asString(d["type"]) == "input_json_delta" && len(t.respToolItems) > 0 {
				item := t.respToolItems[len(t.respToolItems)-1]
				item["arguments"] = asString(item["arguments"]) + asString(d["partial_json"])
				return []SSEEvent{t.respEvent("response.function_call_arguments.delta", map[string]any{
					"type": "response.function_call_arguments.delta", "item_id": item["id"],
					"output_index": t.respToolIndexes[len(t.respToolIndexes)-1], "delta": asString(d["partial_json"]),
				})}
			}
			if text := asString(d["text"]); text != "" {
				t.respText.WriteString(text)
				if !t.respPartAdded {
					t.respPartAdded = true
					var opener []SSEEvent
					if !t.respItemAdded {
						t.respItemAdded = true
						t.respMessageIndex = t.respNextOutputIndex
						t.respNextOutputIndex++
						opener = append(opener, t.respEvent("response.output_item.added", map[string]any{
							"type": "response.output_item.added", "output_index": t.respMessageIndex,
							"item": t.responsesMessageItem("in_progress"),
						}))
					}
					opener = append(opener, t.respEvent("response.content_part.added", map[string]any{
						"type": "response.content_part.added", "item_id": t.itemID, "output_index": t.respMessageIndex, "content_index": 0,
						"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
					}))
					return append(opener, t.respEvent("response.output_text.delta", map[string]any{
						"type": "response.output_text.delta", "item_id": t.itemID, "output_index": t.respMessageIndex, "content_index": 0, "delta": text,
					}))
				}
				return []SSEEvent{t.respEvent("response.output_text.delta", map[string]any{
					"type": "response.output_text.delta", "item_id": t.itemID, "output_index": t.respMessageIndex, "content_index": 0, "delta": text,
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

// responsesResponseID rebrands Anthropic message ids (msg_*) as Responses
// ids so clients see their own vocabulary.
func responsesResponseID(id string) string {
	if strings.HasPrefix(id, "msg_") {
		return "resp_" + strings.TrimPrefix(id, "msg_")
	}
	return id
}

// responsesMessageItem builds the message output item for item.added/done and
// the completed response's output array.
func (t *SSETranslator) responsesMessageItem(status string) map[string]any {
	item := map[string]any{"id": t.itemID, "type": "message", "role": "assistant", "status": status}
	if status != "in_progress" {
		item["content"] = []any{map[string]any{"type": "output_text", "text": t.respText.String(), "annotations": []any{}}}
	} else {
		item["content"] = []any{}
	}
	return item
}

func (t *SSETranslator) responsesReasoningItem(status string) map[string]any {
	summary := []any{}
	if status != "in_progress" {
		summary = append(summary, map[string]any{"type": "summary_text", "text": t.respReasoning.String()})
	}
	return map[string]any{"id": t.respReasoningID, "type": "reasoning", "status": status, "summary": summary}
}

func (t *SSETranslator) responsesReasoningDelta(text string) []SSEEvent {
	if text == "" {
		return nil
	}
	var out []SSEEvent
	if !t.respReasoningAdded {
		t.respReasoningAdded = true
		t.respReasoningID = "rs_" + t.itemID
		t.respReasoningIndex = t.respNextOutputIndex
		t.respNextOutputIndex++
		out = append(out,
			t.respEvent("response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": t.respReasoningIndex, "item": t.responsesReasoningItem("in_progress")}),
			t.respEvent("response.reasoning_summary_part.added", map[string]any{"type": "response.reasoning_summary_part.added", "item_id": t.respReasoningID, "output_index": t.respReasoningIndex, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}}))
	}
	t.respReasoning.WriteString(text)
	return append(out, t.respEvent("response.reasoning_summary_text.delta", map[string]any{"type": "response.reasoning_summary_text.delta", "item_id": t.respReasoningID, "output_index": t.respReasoningIndex, "summary_index": 0, "delta": text}))
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
	var events []SSEEvent
	output := make([]any, t.respNextOutputIndex)
	status := asString(response["status"])
	if t.respReasoningAdded {
		item := t.responsesReasoningItem(status)
		output[t.respReasoningIndex] = item
		events = append(events,
			t.respEvent("response.reasoning_summary_text.done", map[string]any{"type": "response.reasoning_summary_text.done", "item_id": t.respReasoningID, "output_index": t.respReasoningIndex, "summary_index": 0, "text": t.respReasoning.String()}),
			t.respEvent("response.reasoning_summary_part.done", map[string]any{"type": "response.reasoning_summary_part.done", "item_id": t.respReasoningID, "output_index": t.respReasoningIndex, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": t.respReasoning.String()}}),
			t.respEvent("response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": t.respReasoningIndex, "item": item}))
	}
	if t.respItemAdded {
		item := t.responsesMessageItem(status)
		output[t.respMessageIndex] = item
		if t.respPartAdded {
			events = append(events,
				t.respEvent("response.output_text.done", map[string]any{
					"type": "response.output_text.done", "item_id": t.itemID, "output_index": t.respMessageIndex, "content_index": 0,
					"text": t.respText.String(),
				}),
				t.respEvent("response.content_part.done", map[string]any{
					"type": "response.content_part.done", "item_id": t.itemID, "output_index": t.respMessageIndex, "content_index": 0,
					"part": map[string]any{"type": "output_text", "text": t.respText.String(), "annotations": []any{}},
				}))
		}
		events = append(events, t.respEvent("response.output_item.done", map[string]any{
			"type": "response.output_item.done", "output_index": t.respMessageIndex, "item": item,
		}))
	}
	for index, toolItem := range t.respToolItems {
		toolItem["status"] = status
		outputIndex := t.respToolIndexes[index]
		events = append(events,
			t.respEvent("response.function_call_arguments.done", map[string]any{
				"type": "response.function_call_arguments.done", "item_id": toolItem["id"],
				"output_index": outputIndex, "name": toolItem["name"], "arguments": toolItem["arguments"],
			}),
			t.respEvent("response.output_item.done", map[string]any{
				"type": "response.output_item.done", "output_index": outputIndex, "item": toolItem,
			}))
		output[outputIndex] = toolItem
	}
	response["output"] = output
	events = append(events, t.respEvent(finalEvent, map[string]any{"type": finalEvent, "response": response}))
	return events
}

// responsesToAnthropic converts a Responses stream into Anthropic events by
// reshaping each Responses event into a synthetic Chat chunk and reusing the
// well-tested Chat -> Anthropic state machine.
func (t *SSETranslator) responsesToAnthropic(event SSEEvent, payload map[string]any) []SSEEvent {
	if t.doneSent {
		return nil
	}
	chunk := map[string]any{"choices": []any{}}
	switch eventType(event, payload) {
	case "response.created":
		if response, ok := payload["response"].(map[string]any); ok {
			chunk["id"] = response["id"]
			chunk["model"] = response["model"]
		}
		chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant"}}}
	case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
		chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{"reasoning_content": payload["delta"]}}}
	case "response.output_text.delta":
		chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{"content": payload["delta"]}}}
	case "response.output_item.added":
		item, _ := payload["item"].(map[string]any)
		if item == nil || asString(item["type"]) != "function_call" {
			return nil
		}
		callID := asString(item["call_id"])
		if callID == "" {
			callID = asString(item["id"])
		}
		position := len(t.toolCallByItem)
		t.toolCallByItem[asString(item["id"])] = position
		chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{"tool_calls": []any{map[string]any{
			"index": position, "id": callID, "type": "function",
			"function": map[string]any{"name": item["name"], "arguments": ""},
		}}}}}
	case "response.function_call_arguments.delta":
		position, known := t.toolCallByItem[asString(payload["item_id"])]
		if !known {
			return nil
		}
		t.toolArgsSeen[asString(payload["item_id"])] = true
		chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{"tool_calls": []any{map[string]any{
			"index": position, "function": map[string]any{"arguments": payload["delta"]},
		}}}}}
	case "response.function_call_arguments.done":
		// Upstreams that only provide the final arguments on the done event
		// must not lose them.
		itemID := asString(payload["item_id"])
		position, known := t.toolCallByItem[itemID]
		if !known || t.toolArgsSeen[itemID] {
			return nil
		}
		chunk["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{"tool_calls": []any{map[string]any{
			"index": position, "function": map[string]any{"arguments": payload["arguments"]},
		}}}}}
	case "response.incomplete", "response.completed":
		if response, ok := payload["response"].(map[string]any); ok {
			if usage, ok := response["usage"].(map[string]any); ok {
				chunk["usage"] = map[string]any{"prompt_tokens": numberOrZero(usage["input_tokens"]), "completion_tokens": numberOrZero(usage["output_tokens"])}
			}
		}
		finishReason := "stop"
		if eventType(event, payload) == "response.incomplete" {
			finishReason = "length"
		} else if len(t.toolCallByItem) > 0 {
			finishReason = "tool_calls" // maps to Anthropic's tool_use
		}
		out := t.openAIEventToAnthropic(SSEEvent{}, map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finishReason}}})
		if t.doneSent {
			// Tool overflow already emitted a terminal error.
			return out
		}
		if chunk["usage"] != nil {
			out = append(out, t.openAIEventToAnthropic(SSEEvent{}, chunk)...)
		}
		out = append(out, sseEvent("message_stop", map[string]any{"type": "message_stop"}))
		t.doneSent = true
		return out
	case "response.failed", "error":
		message := "upstream request failed"
		if errPayload, ok := payload["error"].(map[string]any); ok {
			if text := asString(errPayload["message"]); text != "" {
				message = text
			}
		} else if response, ok := payload["response"].(map[string]any); ok {
			if errPayload, ok := response["error"].(map[string]any); ok {
				if text := asString(errPayload["message"]); text != "" {
					message = text
				}
			}
		}
		// Mark the stream terminated so Finish() cannot append a normal
		// message_stop after the error.
		t.doneSent = true
		t.finishSent = true
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
		if reasoning := asString(d["reasoning_content"]); reasoning != "" {
			out = append(out, t.ensureStarted(payload)...)
			out = append(out, t.closeTextBlock()...)
			out = append(out, t.openThinkingBlock()...)
			out = append(out, sseEvent("content_block_delta", map[string]any{
				"type": "content_block_delta", "index": t.thinkingIndex,
				"delta": map[string]any{"type": "thinking_delta", "thinking": reasoning},
			}))
		}
		if content := asString(d["content"]); content != "" {
			out = append(out, t.ensureStarted(payload)...)
			out = append(out, t.closeThinkingBlock()...)
			out = append(out, t.openTextBlock()...)
			out = append(out, sseEvent("content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": t.currentTextIndex(),
				"delta": map[string]any{"type": "text_delta", "text": content},
			}))
		}
		if calls, ok := d["tool_calls"].([]any); ok {
			t.accumulateToolCalls(calls)
		}
	}
	if finish != nil && !t.finishSent {
		out = append(out, t.ensureStarted(payload)...)
		out = append(out, t.closeTextBlock()...)
		out = append(out, t.closeThinkingBlock()...)
		out = append(out, t.flushBufferedTools()...)
		if t.toolOverflow {
			// Suppress both the normal finish event and any later [DONE]/Finish
			// terminator after the terminal translation error.
			t.doneSent = true
			t.finishSent = true
			return out
		}
		out = append(out, sseEvent("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": mapFinishReason(asString(finish)), "stop_sequence": nil},
		}))
		t.finishSent = true
	}
	return out
}

// accumulateToolCalls buffers tool-call deltas; the blocks are emitted by
// flushBufferedTools when the stream completes.
func (t *SSETranslator) accumulateToolCalls(calls []any) {
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
		buffer := t.toolBuffers[index]
		if buffer == nil {
			buffer = &toolCallBuffer{id: asString(call["id"])}
			t.toolBuffers[index] = buffer
		}
		if id := asString(call["id"]); id != "" {
			buffer.id = id
		}
		if fn != nil {
			if name := asString(fn["name"]); name != "" {
				buffer.name = name
			}
			chunk := asString(fn["arguments"])
			if buffer.args.Len()+len(chunk) > maxBufferedToolArgs || t.bufferedToolArgs+len(chunk) > maxBufferedToolArgsTotal {
				// Bound both one call and the complete stream; a parallel
				// response must not multiply the memory budget indefinitely.
				t.toolOverflow = true
				return
			}
			buffer.args.WriteString(chunk)
			t.bufferedToolArgs += len(chunk)
		}
	}
}

// flushBufferedTools replays every buffered tool call as a complete
// content_block start/delta/stop triple, in tool_calls index order.
func (t *SSETranslator) flushBufferedTools() []SSEEvent {
	if t.toolOverflow {
		// An error event is better than silently truncated arguments.
		return []SSEEvent{sseEvent("error", map[string]any{"type": "error", "error": map[string]any{"type": "api_error", "message": "buffered tool arguments exceeded the translation limit"}})}
	}
	indexes := make([]int, 0, len(t.toolBuffers))
	for index := range t.toolBuffers {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	var out []SSEEvent
	for _, index := range indexes {
		buffer := t.toolBuffers[index]
		t.nextBlock++
		block := t.nextBlock
		out = append(out,
			sseEvent("content_block_start", map[string]any{
				"type": "content_block_start", "index": block,
				"content_block": map[string]any{"type": "tool_use", "id": buffer.id, "name": buffer.name, "input": map[string]any{}},
			}),
			sseEvent("content_block_delta", map[string]any{
				"type": "content_block_delta", "index": block,
				"delta": map[string]any{"type": "input_json_delta", "partial_json": buffer.args.String()},
			}),
			sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": block}))
	}
	t.toolBuffers = map[int]*toolCallBuffer{}
	t.bufferedToolArgs = 0
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

func (t *SSETranslator) openThinkingBlock() []SSEEvent {
	if t.thinkingOpen {
		return nil
	}
	t.thinkingOpen = true
	t.nextBlock++
	t.thinkingIndex = t.nextBlock
	return []SSEEvent{sseEvent("content_block_start", map[string]any{
		"type": "content_block_start", "index": t.thinkingIndex,
		"content_block": map[string]any{"type": "thinking", "thinking": ""},
	})}
}

func (t *SSETranslator) closeThinkingBlock() []SSEEvent {
	if !t.thinkingOpen {
		return nil
	}
	t.thinkingOpen = false
	return []SSEEvent{sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": t.thinkingIndex})}
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
		return "tool_use" // Anthropic's stop_reason vocabulary
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
		blockIndex := 0
		if n, ok := numberValueOK(payload["index"]); ok {
			blockIndex = int(n)
		}
		toolPosition := t.toolCallCount
		t.toolCallIndexByBlock[blockIndex] = toolPosition
		t.toolCallCount++
		delta["tool_calls"] = []any{map[string]any{
			"index": toolPosition, "id": block["id"], "type": "function",
			"function": map[string]any{"name": block["name"], "arguments": ""},
		}}
	case "content_block_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			switch asString(d["type"]) {
			case "text_delta":
				if text := asString(d["text"]); text != "" {
					delta["content"] = text
				}
			case "input_json_delta":
				blockIndex := 0
				if n, ok := numberValueOK(payload["index"]); ok {
					blockIndex = int(n)
				}
				toolPosition := t.toolCallIndexByBlock[blockIndex]
				delta["tool_calls"] = []any{map[string]any{
					"index": toolPosition, "function": map[string]any{"arguments": asString(d["partial_json"])},
				}}
			case "thinking_delta":
				// Extended thinking has no official Chat Completions field;
				// reasoning_content is the de-facto standard (DeepSeek,
				// Cherry Studio, OpenWebUI, LiteLLM all render it).
				if text := asString(d["thinking"]); text != "" {
					delta["reasoning_content"] = text
				}
			}
		}
		if len(delta) == 0 && choice["finish_reason"] == nil {
			// Unhandled block types (signature deltas, redacted thinking)
			// must not become empty frames.
			return SSEEvent{}, nil
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

// StreamError preserves an upstream error encountered while buffering a stream.
// Payload is a JSON error envelope suitable for conversion to the client contract.
type StreamError struct {
	Payload []byte
	Message string
}

func (e *StreamError) Error() string { return e.Message }

func streamFailure(event SSEEvent, payload map[string]any) *StreamError {
	detail, failed := payload["error"].(map[string]any)
	if response, ok := payload["response"].(map[string]any); ok {
		if value, ok := response["error"].(map[string]any); ok {
			detail, failed = value, true
		}
		failed = failed || asString(response["status"]) == "failed"
	}
	typ := eventType(event, payload)
	if !failed && typ != "error" && typ != "response.failed" {
		return nil
	}
	if detail == nil {
		detail = map[string]any{"type": "upstream_error"}
	}
	message := asString(detail["message"])
	if message == "" {
		message = "upstream stream failed"
		detail["message"] = message
	}
	body, _ := json.Marshal(map[string]any{"error": detail})
	return &StreamError{Payload: body, Message: message}
}

// AggregateSSE reconstructs a non-streaming JSON response from translated
// events. Text, tool calls and finish/usage metadata are retained, while
// unknown event payloads are ignored rather than invented.
func AggregateSSE(events []SSEEvent, contract string) ([]byte, string, error) {
	contract = strings.ToLower(contract)
	var text, reasoning strings.Builder
	var id, model, finish string
	var usage map[string]any
	var responseOutput []any
	responseStatus := "completed"
	tools := map[int]*aggregatedTool{}
	anthropicBlocks := map[int]*aggregatedTool{}
	responseTools := map[string]*aggregatedTool{}
	responseToolOrder := []string{}

	for _, event := range events {
		if event.Data == "" || event.Data == "[DONE]" {
			continue
		}
		var payload map[string]any
		if json.Unmarshal([]byte(event.Data), &payload) != nil {
			continue
		}
		if failure := streamFailure(event, payload); failure != nil {
			return nil, "", failure
		}
		if message, ok := payload["message"].(map[string]any); ok {
			if id == "" {
				id = asString(message["id"])
			}
			if model == "" {
				model = asString(message["model"])
			}
			if value, ok := message["usage"].(map[string]any); ok && usage == nil {
				usage = value
			}
		}
		if response, ok := payload["response"].(map[string]any); ok {
			if id == "" {
				id = asString(response["id"])
			}
			if model == "" {
				model = asString(response["model"])
			}
			if value, ok := response["usage"].(map[string]any); ok {
				usage = value
			}
			if status := asString(response["status"]); status != "" {
				responseStatus = status
				if status != "in_progress" {
					if output, ok := response["output"].([]any); ok {
						responseOutput = output
					}
				}
			}
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
		aggregateOpenAIChunk(payload, &text, &reasoning, tools, &finish)
		aggregateAnthropicEvent(payload, &text, &reasoning, anthropicBlocks, &finish)
		aggregateResponsesEvent(event, payload, &text, &reasoning, responseTools, &responseToolOrder, &finish, &responseStatus)
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
		if reasoning.Len() > 0 {
			content = append(content, map[string]any{"type": "thinking", "thinking": reasoning.String()})
		}
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
		output := []any{}
		if reasoning.Len() > 0 {
			output = append(output, map[string]any{"id": nextResponsesItemID(), "type": "reasoning", "status": responseStatus, "summary": []any{map[string]any{"type": "summary_text", "text": reasoning.String()}}})
		}
		if text.Len() > 0 || reasoning.Len() == 0 && len(responseToolOrder) == 0 {
			content := []any{}
			if text.Len() > 0 || len(content) == 0 {
				content = append(content, map[string]any{"type": "output_text", "text": text.String(), "annotations": []any{}})
			}
			output = append(output, map[string]any{"id": nextResponsesItemID(), "type": "message", "status": responseStatus, "role": "assistant", "content": content})
		}
		for _, itemID := range responseToolOrder {
			tool := responseTools[itemID]
			output = append(output, map[string]any{"id": itemID, "type": "function_call", "status": responseStatus, "call_id": tool.id, "name": tool.name, "arguments": tool.args.String()})
		}
		// Terminal output is the authoritative complete set of items, retaining
		// original item IDs, ordering and content types across buffering.
		if responseOutput != nil {
			output = responseOutput
		}
		body := map[string]any{"id": id, "object": "response", "status": responseStatus, "model": model, "output": output, "usage": usage}
		if responseStatus == "incomplete" {
			body["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
		}
		if text.Len() > 0 {
			body["output_text"] = text.String()
		}
		data, err := json.Marshal(body)
		return data, "application/json", err
	}
	message := map[string]any{"role": "assistant", "content": text.String()}
	if reasoning.Len() > 0 {
		message["reasoning_content"] = reasoning.String()
	}
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

func aggregateOpenAIChunk(payload map[string]any, text, reasoning *strings.Builder, tools map[int]*aggregatedTool, finish *string) {
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
	reasoning.WriteString(asString(delta["reasoning_content"]))
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

func aggregateAnthropicEvent(payload map[string]any, text, reasoning *strings.Builder, blocks map[int]*aggregatedTool, finish *string) {
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
			if asString(d["type"]) == "thinking_delta" {
				reasoning.WriteString(asString(d["thinking"]))
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

func aggregateResponsesEvent(event SSEEvent, payload map[string]any, text, reasoning *strings.Builder, tools map[string]*aggregatedTool, order *[]string, finish *string, status *string) {
	switch eventType(event, payload) {
	case "response.output_text.delta":
		text.WriteString(asString(payload["delta"]))
	case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
		reasoning.WriteString(asString(payload["delta"]))
	case "response.output_item.added":
		item, _ := payload["item"].(map[string]any)
		if item == nil || asString(item["type"]) != "function_call" {
			return
		}
		itemID := asString(item["id"])
		if itemID == "" {
			itemID = fmt.Sprintf("fc_gateway_%d", len(*order))
		}
		callID := asString(item["call_id"])
		if callID == "" {
			callID = itemID
		}
		tool := &aggregatedTool{id: callID, name: asString(item["name"])}
		tool.args.WriteString(asString(item["arguments"]))
		tools[itemID] = tool
		*order = append(*order, itemID)
	case "response.function_call_arguments.delta":
		if tool := tools[asString(payload["item_id"])]; tool != nil {
			tool.args.WriteString(asString(payload["delta"]))
		}
	case "response.function_call_arguments.done":
		if tool := tools[asString(payload["item_id"])]; tool != nil && tool.args.Len() == 0 {
			tool.args.WriteString(asString(payload["arguments"]))
		}
	case "response.incomplete":
		*status, *finish = "incomplete", "incomplete"
	case "response.failed":
		*status, *finish = "failed", "failed"
	}
}

// EncodeBufferedSSE re-encodes a completed JSON response as a valid stream for
// the requested client contract. It is intentionally contract-aware: only
// Chat Completions uses the [DONE] sentinel.
func EncodeBufferedSSE(body []byte, contract string) ([]byte, error) {
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, fmt.Errorf("encode buffered SSE: %w", err)
	}
	contract = strings.ToLower(strings.TrimSpace(contract))
	if strings.Contains(contract, "anthropic") {
		return EncodeSSE(bufferedAnthropicEvents(value)), nil
	}
	if strings.Contains(contract, "responses") {
		return EncodeSSE(bufferedResponsesEvents(value)), nil
	}
	return EncodeSSE(bufferedOpenAIEvents(value)), nil
}

func bufferedOpenAIEvents(value map[string]any) []SSEEvent {
	chunk := map[string]any{"id": value["id"], "object": "chat.completion.chunk", "created": value["created"], "model": value["model"]}
	choice := map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": nil}
	if choices := asSlice(value["choices"]); len(choices) > 0 {
		if source, ok := choices[0].(map[string]any); ok {
			choice["finish_reason"] = source["finish_reason"]
			if message, ok := source["message"].(map[string]any); ok {
				delta := choice["delta"].(map[string]any)
				delta["role"] = "assistant"
				for _, key := range []string{"content", "reasoning_content", "tool_calls"} {
					if message[key] != nil {
						delta[key] = message[key]
					}
				}
			}
		}
	}
	chunk["choices"] = []any{choice}
	if value["usage"] != nil {
		chunk["usage"] = value["usage"]
	}
	encoded, _ := json.Marshal(chunk)
	return []SSEEvent{{Data: string(encoded)}, {Data: "[DONE]"}}
}

func bufferedAnthropicEvents(value map[string]any) []SSEEvent {
	message := map[string]any{"id": value["id"], "type": "message", "role": "assistant", "model": value["model"], "content": []any{}, "usage": value["usage"]}
	events := []SSEEvent{sseEvent("message_start", map[string]any{"type": "message_start", "message": message})}
	for index, raw := range asSlice(value["content"]) {
		block, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		typ := asString(block["type"])
		start := map[string]any{"type": typ}
		var delta map[string]any
		switch typ {
		case "text":
			start["text"] = ""
			delta = map[string]any{"type": "text_delta", "text": asString(block["text"])}
		case "thinking":
			start["thinking"] = ""
			delta = map[string]any{"type": "thinking_delta", "thinking": asString(block["thinking"])}
		case "tool_use":
			start["id"], start["name"], start["input"] = block["id"], block["name"], map[string]any{}
			args, _ := json.Marshal(block["input"])
			delta = map[string]any{"type": "input_json_delta", "partial_json": string(args)}
		default:
			continue
		}
		events = append(events,
			sseEvent("content_block_start", map[string]any{"type": "content_block_start", "index": index, "content_block": start}),
			sseEvent("content_block_delta", map[string]any{"type": "content_block_delta", "index": index, "delta": delta}),
			sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": index}))
	}
	events = append(events,
		sseEvent("message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": value["stop_reason"], "stop_sequence": nil}, "usage": value["usage"]}),
		sseEvent("message_stop", map[string]any{"type": "message_stop"}))
	return events
}

func bufferedResponsesEvents(value map[string]any) []SSEEvent {
	sequence := 0
	makeEvent := func(name string, payload map[string]any) SSEEvent {
		sequence++
		payload["sequence_number"] = sequence
		return sseEvent(name, payload)
	}
	response := map[string]any{"id": value["id"], "object": "response", "status": "in_progress", "model": value["model"]}
	events := []SSEEvent{makeEvent("response.created", map[string]any{"type": "response.created", "response": response}), makeEvent("response.in_progress", map[string]any{"type": "response.in_progress", "response": response})}
	for outputIndex, raw := range asSlice(value["output"]) {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		announced := make(map[string]any, len(item))
		for key, value := range item {
			announced[key] = value
		}
		announced["status"] = "in_progress"
		switch asString(item["type"]) {
		case "message":
			announced["content"] = []any{}
		case "reasoning":
			announced["summary"] = []any{}
		case "function_call":
			announced["arguments"] = ""
		}
		events = append(events, makeEvent("response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": outputIndex, "item": announced}))
		switch asString(item["type"]) {
		case "message":
			for contentIndex, rawContent := range asSlice(item["content"]) {
				part, ok := rawContent.(map[string]any)
				if !ok {
					continue
				}
				events = append(events, makeEvent("response.content_part.added", map[string]any{"type": "response.content_part.added", "item_id": item["id"], "output_index": outputIndex, "content_index": contentIndex, "part": part}))
				if asString(part["type"]) == "output_text" {
					events = append(events, makeEvent("response.output_text.delta", map[string]any{"type": "response.output_text.delta", "item_id": item["id"], "output_index": outputIndex, "content_index": contentIndex, "delta": part["text"]}), makeEvent("response.output_text.done", map[string]any{"type": "response.output_text.done", "item_id": item["id"], "output_index": outputIndex, "content_index": contentIndex, "text": part["text"]}))
				}
				events = append(events, makeEvent("response.content_part.done", map[string]any{"type": "response.content_part.done", "item_id": item["id"], "output_index": outputIndex, "content_index": contentIndex, "part": part}))
			}
		case "reasoning":
			for summaryIndex, rawSummary := range asSlice(item["summary"]) {
				part, ok := rawSummary.(map[string]any)
				if !ok || asString(part["type"]) != "summary_text" {
					continue
				}
				events = append(events,
					makeEvent("response.reasoning_summary_part.added", map[string]any{"type": "response.reasoning_summary_part.added", "item_id": item["id"], "output_index": outputIndex, "summary_index": summaryIndex, "part": map[string]any{"type": "summary_text", "text": ""}}),
					makeEvent("response.reasoning_summary_text.delta", map[string]any{"type": "response.reasoning_summary_text.delta", "item_id": item["id"], "output_index": outputIndex, "summary_index": summaryIndex, "delta": part["text"]}),
					makeEvent("response.reasoning_summary_text.done", map[string]any{"type": "response.reasoning_summary_text.done", "item_id": item["id"], "output_index": outputIndex, "summary_index": summaryIndex, "text": part["text"]}),
					makeEvent("response.reasoning_summary_part.done", map[string]any{"type": "response.reasoning_summary_part.done", "item_id": item["id"], "output_index": outputIndex, "summary_index": summaryIndex, "part": part}))
			}
		case "function_call":
			events = append(events, makeEvent("response.function_call_arguments.delta", map[string]any{"type": "response.function_call_arguments.delta", "item_id": item["id"], "output_index": outputIndex, "delta": item["arguments"]}), makeEvent("response.function_call_arguments.done", map[string]any{"type": "response.function_call_arguments.done", "item_id": item["id"], "output_index": outputIndex, "name": item["name"], "arguments": item["arguments"]}))
		}
		events = append(events, makeEvent("response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": outputIndex, "item": item}))
	}
	terminal := "response.completed"
	if asString(value["status"]) == "incomplete" {
		terminal = "response.incomplete"
	} else if asString(value["status"]) == "failed" {
		terminal = "response.failed"
	}
	events = append(events, makeEvent(terminal, map[string]any{"type": terminal, "response": value}))
	return events
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

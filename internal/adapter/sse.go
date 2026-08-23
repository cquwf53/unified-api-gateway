package adapter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
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

	// anthropic -> openai state: message_start carries the prompt token count
	// while usage totals only arrive with message_delta.
	promptTokens    float64
	promptTokensSet bool
}

// NewSSETranslator returns a translator between two contract names such as
// "anthropic-messages" and "openai-chat". Identical or unknown contracts
// return a pass-through translator.
func NewSSETranslator(from, to string) *SSETranslator {
	return &SSETranslator{from: strings.ToLower(from), to: strings.ToLower(to), blockByTool: map[int]int{}, nextBlock: -1}
}

// Translate converts one upstream event into zero or more client events.
func (t *SSETranslator) Translate(event SSEEvent) ([]SSEEvent, error) {
	if t.from == t.to || t.from == "" || t.to == "" {
		return []SSEEvent{event}, nil
	}
	if strings.EqualFold(event.Data, "[DONE]") {
		return t.translateDone(), nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
		// A non-JSON event can still be safely forwarded as an opaque frame.
		return []SSEEvent{event}, nil
	}
	if strings.Contains(t.from, "anthropic") && strings.Contains(t.to, "openai") {
		out, err := t.anthropicEventToOpenAI(event, payload)
		if err != nil || out.Data != "" {
			return []SSEEvent{out}, nil
		}
		return nil, nil
	}
	if strings.Contains(t.from, "openai") && strings.Contains(t.to, "anthropic") {
		return t.openAIEventToAnthropic(event, payload), nil
	}
	return []SSEEvent{event}, nil
}

func (t *SSETranslator) translateDone() []SSEEvent {
	if !strings.Contains(t.from, "openai") || !strings.Contains(t.to, "anthropic") {
		return []SSEEvent{{Data: "[DONE]"}}
	}
	var out []SSEEvent
	out = append(out, t.closeTextBlock()...)
	out = append(out, sseEvent("message_stop", map[string]any{"type": "message_stop"}))
	return out
}

func (t *SSETranslator) openAIEventToAnthropic(event SSEEvent, payload map[string]any) []SSEEvent {
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
	t.nextBlock = 0
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
	case "stop":
		return "end_turn"
	case "length":
		return "max_tokens"
	case "content_filter":
		return "refusal"
	case "", "tool_calls":
		return "tool_calls"
	default:
		return reason
	}
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
// A zero Data result means the event should be dropped (for example pings).
func (t *SSETranslator) anthropicEventToOpenAI(event SSEEvent, payload map[string]any) (SSEEvent, error) {
	typ := event.Event
	if typ == "" {
		typ = asString(payload["type"])
	}
	if typ == "ping" {
		return SSEEvent{}, nil
	}
	chunk := map[string]any{
		"id":      payload["id"],
		"object":  "chat.completion.chunk",
		"created": payload["created"],
		"model":   payload["model"],
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
			chunk["id"] = message["id"]
			chunk["model"] = message["model"]
			if usage, ok := message["usage"].(map[string]any); ok {
				if n, ok := numberValueOK(usage["input_tokens"]); ok {
					t.promptTokens = n
					t.promptTokensSet = true
				}
			}
		}
		delta["role"] = "assistant"
	case "content_block_start":
		if block, ok := payload["content_block"].(map[string]any); ok && asString(block["type"]) == "tool_use" {
			index := 0
			if n, ok := numberValueOK(payload["index"]); ok {
				index = int(n)
			}
			delta["tool_calls"] = []any{map[string]any{
				"index": index, "id": block["id"], "type": "function",
				"function": map[string]any{"name": block["name"], "arguments": ""},
			}}
		}
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
		}
		if usage, ok := payload["usage"].(map[string]any); ok {
			if _, present := usage["input_tokens"]; !present && t.promptTokensSet {
				usage["input_tokens"] = t.promptTokens
			}
			chunk["usage"] = anthropicUsageToOpenAI(usage)
		}
	case "message_stop":
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

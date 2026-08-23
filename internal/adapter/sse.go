package adapter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// SSEEvent is a parsed Server-Sent Event frame. Data may contain newlines;
// ParseSSE joins multiple data: lines according to the SSE specification.
type SSEEvent struct {
	ID    string
	Event string
	Data  string
	Retry string
}

func ParseSSE(r io.Reader) ([]SSEEvent, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	var current SSEEvent
	var data []string
	var events []SSEEvent
	flush := func() {
		if current.ID == "" && current.Event == "" && len(data) == 0 && current.Retry == "" {
			return
		}
		current.Data = strings.Join(data, "\n")
		events = append(events, current)
		current = SSEEvent{}
		data = nil
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if ok {
			value = strings.TrimPrefix(value, " ")
		}
		switch key {
		case "id":
			current.ID = value
		case "event":
			current.Event = value
		case "data":
			data = append(data, value)
		case "retry":
			current.Retry = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	flush()
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

// TranslateSSE translates the common text/usage/tool events used by OpenAI
// Chat and Anthropic Messages. Unknown events are preserved with their data so
// vendor extensions do not disappear silently.
func TranslateSSE(events []SSEEvent, from, to string) ([]SSEEvent, error) {
	from = strings.ToLower(from)
	to = strings.ToLower(to)
	if from == to || from == "" || to == "" {
		return events, nil
	}
	result := make([]SSEEvent, 0, len(events))
	for _, event := range events {
		if strings.EqualFold(event.Data, "[DONE]") {
			result = append(result, SSEEvent{Data: "[DONE]"})
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.Data), &payload); err != nil {
			// A non-JSON event can still be safely forwarded as an opaque frame.
			result = append(result, event)
			continue
		}
		switch {
		case strings.Contains(from, "anthropic") && strings.Contains(to, "openai"):
			result = append(result, anthropicEventToOpenAI(event, payload))
		case strings.Contains(from, "openai") && strings.Contains(to, "anthropic"):
			result = append(result, openAIEventToAnthropic(event, payload))
		default:
			result = append(result, event)
		}
	}
	return result, nil
}

// AggregateSSE reconstructs a non-streaming JSON response from translated
// events. It is intentionally conservative: text and finish/usage metadata are
// retained, while unknown event payloads are ignored rather than invented.
func AggregateSSE(events []SSEEvent, contract string) ([]byte, string, error) {
	contract = strings.ToLower(contract)
	var text strings.Builder
	var id, model, finish string
	var usage map[string]any
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
			usage = value
		}
		if choices, ok := payload["choices"].([]any); ok && len(choices) > 0 {
			if choice, ok := choices[0].(map[string]any); ok {
				if value := asString(choice["finish_reason"]); value != "" {
					finish = value
				}
				if delta, ok := choice["delta"].(map[string]any); ok {
					text.WriteString(asString(delta["content"]))
				}
			}
		}
		typ := asString(payload["type"])
		if typ == "content_block_delta" {
			if delta, ok := payload["delta"].(map[string]any); ok {
				text.WriteString(asString(delta["text"]))
			}
		}
		if typ == "message_delta" {
			if delta, ok := payload["delta"].(map[string]any); ok {
				finish = asString(delta["stop_reason"])
			}
		}
	}
	if id == "" {
		id = "gateway-stream"
	}
	if model == "" {
		model = "gateway"
	}
	if strings.Contains(contract, "anthropic") {
		body := map[string]any{"id": id, "type": "message", "role": "assistant", "model": model, "content": []any{map[string]any{"type": "text", "text": text.String()}}, "stop_reason": finish, "usage": usage}
		data, err := json.Marshal(body)
		return data, "application/json", err
	}
	if strings.Contains(contract, "responses") {
		body := map[string]any{"id": id, "object": "response", "status": "completed", "model": model, "output": []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": text.String()}}}}, "usage": usage}
		data, err := json.Marshal(body)
		return data, "application/json", err
	}
	body := map[string]any{"id": id, "object": "chat.completion", "model": model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text.String()}, "finish_reason": finish}}, "usage": usage}
	data, err := json.Marshal(body)
	return data, "application/json", err
}

func anthropicEventToOpenAI(event SSEEvent, payload map[string]any) SSEEvent {
	typ := event.Event
	if typ == "" {
		typ = asString(payload["type"])
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
		}
		delta["role"] = "assistant"
	case "content_block_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			if text := asString(d["text"]); text != "" {
				delta["content"] = text
			}
			if d["type"] == "input_json_delta" {
				delta["tool_calls"] = []any{map[string]any{"index": 0, "function": map[string]any{"arguments": asString(d["partial_json"])}}}
			}
		}
	case "message_delta":
		if d, ok := payload["delta"].(map[string]any); ok {
			choice["finish_reason"] = d["stop_reason"]
		}
	case "message_stop":
		choice["finish_reason"] = "stop"
	case "message_delta_usage", "usage":
		chunk["usage"] = payload["usage"]
	}
	b, _ := json.Marshal(chunk)
	return SSEEvent{Data: string(b)}
}

func openAIEventToAnthropic(event SSEEvent, payload map[string]any) SSEEvent {
	chunk := map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": ""}}
	if choices, ok := payload["choices"].([]any); ok && len(choices) > 0 {
		if choice, ok := choices[0].(map[string]any); ok {
			if delta, ok := choice["delta"].(map[string]any); ok {
				if role := asString(delta["role"]); role != "" {
					chunk = map[string]any{"type": "message_start", "message": map[string]any{"role": role}}
				} else if content := asString(delta["content"]); content != "" {
					chunk["delta"].(map[string]any)["text"] = content
				}
				if choice["finish_reason"] != nil {
					chunk = map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": choice["finish_reason"]}}
				}
			}
		}
	}
	b, _ := json.Marshal(chunk)
	return SSEEvent{Event: asString(chunk["type"]), Data: string(b)}
}

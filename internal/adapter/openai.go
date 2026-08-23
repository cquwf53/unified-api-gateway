package adapter

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"unified-api-gateway/internal/config"
)

// OpenAIChatToAnthropic translates the common Chat Completions JSON contract
// into the Anthropic Messages contract. It deliberately leaves unknown fields
// alone where doing so is safe, which makes the adapter useful for vendor
// extensions without hiding malformed core fields.
type OpenAIChatToAnthropic struct{}

func (OpenAIChatToAnthropic) Name() string { return "openai-chat-to-anthropic" }

func (OpenAIChatToAnthropic) AdaptRequest(in Envelope, _ config.Profile) (Envelope, error) {
	body, err := decodeJSON(in.Body)
	if err != nil {
		return Envelope{}, err
	}
	out := map[string]any{}
	if model, ok := body["model"]; ok {
		out["model"] = model
	}
	if max, ok := body["max_tokens"]; ok {
		out["max_tokens"] = max
	} else if max, ok := body["max_completion_tokens"]; ok {
		out["max_tokens"] = max
	} else {
		out["max_tokens"] = 1024
	}
	for _, key := range []string{"temperature", "top_p", "stop_sequences", "stream", "tool_choice", "metadata"} {
		if value, ok := body[key]; ok {
			out[key] = value
		}
	}
	if stop, ok := body["stop"]; ok {
		out["stop_sequences"] = normalizeStop(stop)
	}
	if tools := convertOpenAITools(body["tools"]); len(tools) > 0 {
		out["tools"] = tools
	}

	var messages []any
	if raw, ok := body["messages"].([]any); ok {
		for _, item := range raw {
			message, ok := item.(map[string]any)
			if !ok {
				return Envelope{}, fmt.Errorf("messages contains a non-object item")
			}
			role := strings.ToLower(asString(message["role"]))
			if role == "system" || role == "developer" {
				text := contentText(message["content"])
				if text != "" {
					appendSystem(out, text)
				}
				continue
			}
			if role == "tool" {
				messages = append(messages, map[string]any{
					"role": "user",
					"content": []any{map[string]any{
						"type":        "tool_result",
						"tool_use_id": message["tool_call_id"],
						"content":     message["content"],
					}},
				})
				continue
			}
			if role != "user" && role != "assistant" {
				role = "user"
			}
			content := convertOpenAIContent(message["content"], role)
			if role == "user" {
				if text, ok := content.(string); ok && text != "" {
					// Anthropic's canonical representation for user turns is
					// an array of content blocks.
					content = []any{map[string]any{"type": "text", "text": text}}
				}
			}
			if calls, ok := message["tool_calls"].([]any); ok && len(calls) > 0 {
				contentItems, isList := content.([]any)
				if !isList {
					contentItems = []any{}
					if text := asString(content); text != "" {
						contentItems = append(contentItems, map[string]any{"type": "text", "text": text})
					}
				}
				for _, rawCall := range calls {
					call, ok := rawCall.(map[string]any)
					if !ok {
						continue
					}
					fn, _ := call["function"].(map[string]any)
					input := map[string]any{}
					if args := asString(fn["arguments"]); args != "" {
						if json.Unmarshal([]byte(args), &input) != nil {
							input = map[string]any{"raw": args}
						}
					}
					contentItems = append(contentItems, map[string]any{
						"type":  "tool_use",
						"id":    call["id"],
						"name":  fn["name"],
						"input": input,
					})
				}
				content = contentItems
			}
			messages = append(messages, map[string]any{"role": role, "content": content})
		}
	}
	out["messages"] = messages
	b, contentType, err := encodeJSON(out)
	if err != nil {
		return Envelope{}, err
	}
	result := in.Clone()
	result.Body = b
	result.Header = ensureJSONHeader(result.Header)
	result.ContentType = contentType
	result.Stream = asBool(body["stream"])
	result.Protocol = "anthropic-messages"
	return result, nil
}

func (OpenAIChatToAnthropic) AdaptResponse(in Envelope, _ config.Profile) (Envelope, error) {
	if len(in.Body) == 0 || !strings.Contains(strings.ToLower(in.ContentType), "json") {
		return in.Clone(), nil
	}
	body, err := decodeJSON(in.Body)
	if err != nil {
		return Envelope{}, err
	}
	out := map[string]any{
		"id":      openAIResponseID(asString(body["id"])),
		"object":  "chat.completion",
		"created": body["created"],
		"model":   body["model"],
	}
	if out["created"] == nil {
		out["created"] = time.Now().Unix()
	}
	if out["model"] == nil {
		out["model"] = "gateway"
	}
	choice := map[string]any{"index": 0, "finish_reason": openAIFinishReason(asString(body["stop_reason"]))}
	message := map[string]any{"role": "assistant", "content": ""}
	if content, ok := body["content"].([]any); ok {
		var text strings.Builder
		var toolCalls []any
		for _, raw := range content {
			part, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			switch part["type"] {
			case "text":
				text.WriteString(asString(part["text"]))
			case "tool_use":
				args, _ := json.Marshal(part["input"])
				toolCalls = append(toolCalls, map[string]any{
					"id":   part["id"],
					"type": "function",
					"function": map[string]any{
						"name":      part["name"],
						"arguments": string(args),
					},
				})
			}
		}
		message["content"] = text.String()
		if len(toolCalls) > 0 {
			message["tool_calls"] = toolCalls
			choice["finish_reason"] = "tool_calls"
		}
	}
	choice["message"] = message
	out["choices"] = []any{choice}
	if usage, ok := body["usage"].(map[string]any); ok {
		out["usage"] = map[string]any{
			"prompt_tokens":     usage["input_tokens"],
			"completion_tokens": usage["output_tokens"],
			"total_tokens":      sumNumbers(usage["input_tokens"], usage["output_tokens"]),
		}
	}
	b, contentType, err := encodeJSON(out)
	if err != nil {
		return Envelope{}, err
	}
	result := in.Clone()
	result.Body = b
	result.Header = ensureJSONHeader(result.Header)
	result.ContentType = contentType
	result.Protocol = "openai-chat"
	return result, nil
}

type AnthropicToOpenAIChat struct{}

func (AnthropicToOpenAIChat) Name() string { return "anthropic-to-openai-chat" }

func (AnthropicToOpenAIChat) AdaptRequest(in Envelope, _ config.Profile) (Envelope, error) {
	// The reverse request direction is useful for clients that already speak
	// Messages while the upstream exposes Chat Completions. Keep the conversion
	// conservative and preserve any unknown top-level fields.
	body, err := decodeJSON(in.Body)
	if err != nil {
		return Envelope{}, err
	}
	out := map[string]any{"model": body["model"]}
	if max, ok := body["max_tokens"]; ok {
		out["max_tokens"] = max
	}
	if stream, ok := body["stream"]; ok {
		out["stream"] = stream
	}
	for _, key := range []string{"temperature", "top_p", "tool_choice"} {
		if value, ok := body[key]; ok {
			out[key] = value
		}
	}
	if tools := convertAnthropicTools(body["tools"]); len(tools) > 0 {
		out["tools"] = tools
	}
	if system := contentText(body["system"]); system != "" {
		out["messages"] = []any{map[string]any{"role": "system", "content": system}}
	}
	messages := asSlice(body["messages"])
	for _, raw := range messages {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		out["messages"] = append(asSlice(out["messages"]), map[string]any{
			"role":    m["role"],
			"content": contentText(m["content"]),
		})
	}
	b, contentType, err := encodeJSON(out)
	if err != nil {
		return Envelope{}, err
	}
	result := in.Clone()
	result.Body = b
	result.Header = ensureJSONHeader(result.Header)
	result.ContentType = contentType
	result.Protocol = "openai-chat"
	return result, nil
}

func (AnthropicToOpenAIChat) AdaptResponse(in Envelope, _ config.Profile) (Envelope, error) {
	// The upstream answered in OpenAI Chat format; convert it into the
	// Anthropic Messages contract the client speaks.
	if len(in.Body) == 0 || !strings.Contains(strings.ToLower(in.ContentType), "json") {
		return in.Clone(), nil
	}
	body, err := decodeJSON(in.Body)
	if err != nil {
		return Envelope{}, err
	}
	model := body["model"]
	if model == nil {
		model = "gateway"
	}
	out := map[string]any{
		"id":    anthropicMessageID(asString(body["id"])),
		"type":  "message",
		"role":  "assistant",
		"model": model,
	}
	content := []any{}
	stopReason := "end_turn"
	if choices := asSlice(body["choices"]); len(choices) > 0 {
		if choice, ok := choices[0].(map[string]any); ok {
			stopReason = anthropicStopReason(asString(choice["finish_reason"]))
			if message, ok := choice["message"].(map[string]any); ok {
				if text := asString(message["content"]); text != "" {
					content = append(content, map[string]any{"type": "text", "text": text})
				}
				for _, raw := range asSlice(message["tool_calls"]) {
					call, ok := raw.(map[string]any)
					if !ok {
						continue
					}
					fn, _ := call["function"].(map[string]any)
					input := map[string]any{}
					if args := asString(fn["arguments"]); args != "" {
						if json.Unmarshal([]byte(args), &input) != nil {
							input = map[string]any{"raw": args}
						}
					}
					content = append(content, map[string]any{"type": "tool_use", "id": call["id"], "name": fn["name"], "input": input})
				}
			}
		}
	}
	out["content"] = content
	out["stop_reason"] = stopReason
	if usage, ok := body["usage"].(map[string]any); ok {
		// Missing counters become zero, not JSON null: Messages clients
		// expect numeric fields.
		out["usage"] = map[string]any{"input_tokens": numberOrZero(usage["prompt_tokens"]), "output_tokens": numberOrZero(usage["completion_tokens"])}
	}
	b, contentType, err := encodeJSON(out)
	if err != nil {
		return Envelope{}, err
	}
	result := in.Clone()
	result.Body = b
	result.Header = ensureJSONHeader(result.Header)
	result.ContentType = contentType
	result.Protocol = "anthropic-messages"
	return result, nil
}

// anthropicMessageID rebrands OpenAI completion ids (chatcmpl-* or
// chatcmpl_*) as Anthropic message ids so clients see identifiers from their
// own vocabulary.
func anthropicMessageID(id string) string {
	if id == "" {
		return "msg-gateway"
	}
	for _, prefix := range []string{"chatcmpl_", "chatcmpl-"} {
		if strings.HasPrefix(id, prefix) {
			return "msg_" + strings.TrimPrefix(id, prefix)
		}
	}
	return id
}

// anthropicStopReason maps OpenAI finish reasons onto the Anthropic
// stop_reason vocabulary.
func anthropicStopReason(reason string) string {
	switch reason {
	case "length":
		return "max_tokens"
	case "tool_calls", "function_call", "tool_use":
		return "tool_use"
	case "content_filter":
		return "refusal"
	default: // "stop", "" and unknown values
		return "end_turn"
	}
}

type OpenAIResponsesToAnthropic struct{}

func (OpenAIResponsesToAnthropic) Name() string { return "openai-responses-to-anthropic" }

func (OpenAIResponsesToAnthropic) AdaptRequest(in Envelope, p config.Profile) (Envelope, error) {
	body, err := decodeJSON(in.Body)
	if err != nil {
		return Envelope{}, err
	}
	// Responses input can be a string or an array of message-like items. Build
	// a Chat-shaped envelope first, then reuse the well-tested adapter.
	chat := map[string]any{
		"model":    body["model"],
		"messages": responsesInputToMessages(body["input"], body["instructions"]),
		"stream":   body["stream"],
	}
	if max, ok := body["max_output_tokens"]; ok {
		chat["max_tokens"] = max
	}
	for _, key := range []string{"temperature", "top_p", "tools", "tool_choice"} {
		if value, ok := body[key]; ok {
			chat[key] = value
		}
	}
	b, _, err := encodeJSON(chat)
	if err != nil {
		return Envelope{}, err
	}
	clone := in.Clone()
	clone.Body = b
	clone.ContentType = "application/json"
	clone.Header = ensureJSONHeader(clone.Header)
	return OpenAIChatToAnthropic{}.AdaptRequest(clone, p)
}

func (OpenAIResponsesToAnthropic) AdaptResponse(in Envelope, p config.Profile) (Envelope, error) {
	// The upstream returned Anthropic Messages; run it through the
	// Anthropic -> OpenAI Chat conversion and reshape that into a Responses
	// object.
	chat, err := OpenAIChatToAnthropic{}.AdaptResponse(in, p)
	if err != nil {
		return Envelope{}, err
	}
	var body map[string]any
	if err := json.Unmarshal(chat.Body, &body); err != nil {
		return Envelope{}, err
	}
	choices := asSlice(body["choices"])
	text := ""
	if len(choices) > 0 {
		if choice, ok := choices[0].(map[string]any); ok {
			if msg, ok := choice["message"].(map[string]any); ok {
				text = asString(msg["content"])
			}
		}
	}
	result := chat.Clone()
	responseModel := body["model"]
	if responseModel == nil {
		responseModel = "gateway"
	}
	result.Body, _ = json.Marshal(map[string]any{
		"id":     body["id"],
		"object": "response",
		"status": "completed",
		"model":  responseModel,
		"output": []any{map[string]any{
			"type": "message", "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": text}},
		}},
		"usage": body["usage"],
	})
	result.Protocol = "openai-responses"
	return result, nil
}

func appendSystem(out map[string]any, text string) {
	if existing := contentText(out["system"]); existing != "" {
		out["system"] = existing + "\n\n" + text
		return
	}
	out["system"] = text
}

// openAIResponseID rebrands Anthropic message ids (msg_*) as chat completion
// ids so OpenAI clients see identifiers from their own vocabulary.
func openAIResponseID(id string) string {
	if id == "" {
		return "chatcmpl-gateway"
	}
	if strings.HasPrefix(id, "msg_") {
		return "chatcmpl_" + strings.TrimPrefix(id, "msg_")
	}
	return id
}

// openAIFinishReason maps Anthropic stop reasons onto the OpenAI
// finish_reason vocabulary; unknown values pass through unchanged.
func openAIFinishReason(reason string) string {
	switch reason {
	case "", "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	case "tool_use", "tool_calls":
		return "tool_calls"
	case "refusal":
		return "content_filter"
	default:
		return reason
	}
}

func convertOpenAIContent(value any, role string) any {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	items := asSlice(value)
	if len(items) == 0 {
		return ""
	}
	result := make([]any, 0, len(items))
	for _, raw := range items {
		part, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		typ := asString(part["type"])
		switch typ {
		case "text", "input_text", "output_text":
			result = append(result, map[string]any{"type": "text", "text": part["text"]})
		case "image_url":
			image, _ := part["image_url"].(map[string]any)
			result = append(result, map[string]any{"type": "image", "source": map[string]any{"type": "url", "url": image["url"]}})
		default:
			if text := asString(part["text"]); text != "" {
				result = append(result, map[string]any{"type": "text", "text": text})
			}
		}
	}
	if role == "assistant" && len(result) == 1 {
		if item, ok := result[0].(map[string]any); ok && item["type"] == "text" {
			return item["text"]
		}
	}
	return result
}

func convertOpenAITools(value any) []any {
	var result []any
	for _, raw := range asSlice(value) {
		tool, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if tool["type"] != nil && asString(tool["type"]) != "function" {
			continue
		}
		fn, _ := tool["function"].(map[string]any)
		name := asString(fn["name"])
		if name == "" {
			continue
		}
		item := map[string]any{"name": name, "description": fn["description"], "input_schema": fn["parameters"]}
		if item["input_schema"] == nil {
			item["input_schema"] = map[string]any{"type": "object"}
		}
		result = append(result, item)
	}
	return result
}

func normalizeStop(value any) any {
	if list := asSlice(value); len(list) > 0 {
		return list
	}
	if text := asString(value); text != "" {
		return []any{text}
	}
	return value
}

func convertAnthropicTools(value any) []any {
	var result []any
	for _, raw := range asSlice(value) {
		tool, ok := raw.(map[string]any)
		if !ok || asString(tool["name"]) == "" {
			continue
		}
		result = append(result, map[string]any{"type": "function", "function": map[string]any{
			"name":        tool["name"],
			"description": tool["description"],
			"parameters":  tool["input_schema"],
		}})
	}
	return result
}

func contentText(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	var parts []string
	for _, raw := range asSlice(value) {
		if text, ok := raw.(string); ok {
			parts = append(parts, text)
			continue
		}
		if part, ok := raw.(map[string]any); ok {
			if text := asString(part["text"]); text != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "\n")
}

func responsesInputToMessages(input, instructions any) []any {
	var result []any
	if text := contentText(instructions); text != "" {
		result = append(result, map[string]any{"role": "system", "content": text})
	}
	if text, ok := input.(string); ok {
		return append(result, map[string]any{"role": "user", "content": text})
	}
	for _, raw := range asSlice(input) {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		role := asString(item["role"])
		if role == "" {
			role = "user"
		}
		result = append(result, map[string]any{"role": role, "content": item["content"]})
	}
	return result
}

func asSlice(value any) []any {
	if value == nil {
		return nil
	}
	if items, ok := value.([]any); ok {
		return items
	}
	return nil
}

func asString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func asBool(value any) bool {
	b, ok := value.(bool)
	return ok && b
}

func sumNumbers(a, b any) float64 {
	return numberValue(a) + numberValue(b)
}

func numberValue(value any) float64 {
	switch n := value.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}

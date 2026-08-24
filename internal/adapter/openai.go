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
	for _, key := range []string{"temperature", "top_p", "stop_sequences", "stream", "metadata"} {
		if value, ok := body[key]; ok {
			out[key] = value
		}
	}
	if choice, ok := body["tool_choice"]; ok {
		if converted := toolChoiceToAnthropic(choice); converted != nil {
			out["tool_choice"] = converted
		}
	}
	// OpenAI expresses the no-parallel-tools constraint as a sibling flag;
	// Anthropic carries it inside tool_choice.
	if parallel, ok := body["parallel_tool_calls"].(bool); ok && !parallel {
		if choice, ok := out["tool_choice"].(map[string]any); ok {
			choice["disable_parallel_tool_use"] = true
		} else {
			out["tool_choice"] = map[string]any{"type": "auto", "disable_parallel_tool_use": true}
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
		var reasoning strings.Builder
		var toolCalls []any
		for _, raw := range content {
			part, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			switch part["type"] {
			case "text":
				text.WriteString(asString(part["text"]))
			case "thinking":
				reasoning.WriteString(asString(part["thinking"]))
			case "redacted_thinking":
				reasoning.WriteString(asString(part["data"]))
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
		if reasoning.Len() > 0 {
			message["reasoning_content"] = reasoning.String()
		}
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
	for _, key := range []string{"temperature", "top_p"} {
		if value, ok := body[key]; ok {
			out[key] = value
		}
	}
	if choice, ok := body["tool_choice"]; ok {
		converted := toolChoiceToOpenAI(choice)
		if anthropicChoice, ok := choice.(map[string]any); ok {
			if disable, ok := anthropicChoice["disable_parallel_tool_use"].(bool); ok {
				out["parallel_tool_calls"] = !disable
			}
		}
		if converted != nil {
			out["tool_choice"] = converted
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
		converted, role, extras := anthropicMessageToOpenAI(m)
		if role != "" {
			if message, ok := converted.(map[string]any); ok {
				// A complete OpenAI message (assistant with tool_calls).
				out["messages"] = append(asSlice(out["messages"]), message)
			} else {
				out["messages"] = append(asSlice(out["messages"]), map[string]any{
					"role":    role,
					"content": converted,
				})
			}
		}
		out["messages"] = append(asSlice(out["messages"]), extras...)
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
				if reasoning := asString(message["reasoning_content"]); reasoning != "" {
					content = append(content, map[string]any{"type": "thinking", "thinking": reasoning})
				}
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
			result = append(result, map[string]any{"type": "image", "source": anthropicImageSource(asString(image["url"]))})
		case "input_image":
			// The Responses API's flat image part.
			result = append(result, map[string]any{"type": "image", "source": anthropicImageSource(asString(part["image_url"]))})
		case "tool_use", "tool_result":
			// Already Anthropic-shaped blocks (for example synthesised by
			// the Responses input adapter); pass through untouched.
			result = append(result, part)
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
		if fn == nil {
			// Responses tools are flat: {"type":"function","name",...}.
			if asString(tool["type"]) == "function" {
				fn = tool
			}
		}
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

// anthropicImageSource converts an OpenAI image_url into an Anthropic image
// source. data: URIs (the dominant path: every SDK uploading a local file
// produces one) must become base64 sources - Anthropic rejects a data: URI
// in url form with a 400.
func anthropicImageSource(url string) map[string]any {
	if !strings.HasPrefix(url, "data:") {
		return map[string]any{"type": "url", "url": url}
	}
	// data:<media_type>[;base64],<data>
	rest := strings.TrimPrefix(url, "data:")
	header, data, found := strings.Cut(rest, ",")
	if !found {
		return map[string]any{"type": "url", "url": url}
	}
	mediaType := header
	base64Encoded := strings.Contains(header, ";base64")
	if base64Encoded {
		mediaType = strings.TrimSuffix(header, ";base64")
	}
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	if !base64Encoded {
		// Non-base64 data URIs cannot be represented; forward as a url
		// source and let the upstream reject it loudly.
		return map[string]any{"type": "url", "url": url}
	}
	return map[string]any{"type": "base64", "media_type": mediaType, "data": data}
}

// anthropicMessageToOpenAI converts one Anthropic message to the OpenAI
// shape, mapping tool_use blocks to tool_calls and tool_result blocks to
// tool-role messages. The returned role may be empty for content that has
// no OpenAI representation yet.
func anthropicMessageToOpenAI(m map[string]any) (any, string, []any) {
	role := asString(m["role"])
	switch role {
	case "user":
		// tool_result blocks become their own tool-role messages appended
		// after the (remaining) user content.
		var results []any
		var rest []any
		for _, raw := range asSlice(m["content"]) {
			part, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if asString(part["type"]) == "tool_result" {
				results = append(results, map[string]any{
					"role":         "tool",
					"tool_call_id": part["tool_use_id"],
					"content":      contentText(part["content"]),
				})
				continue
			}
			rest = append(rest, raw)
		}
		if len(results) > 0 && len(rest) == 0 {
			// Pure tool-result turn: only the tool messages are emitted.
			return nil, "", results
		}
		return anthropicContentToOpenAI(rest), role, results
	case "assistant":
		// tool_use blocks become tool_calls on the assistant message.
		content := anthropicContentToOpenAI(m["content"])
		var toolCalls []any
		for _, raw := range asSlice(m["content"]) {
			part, ok := raw.(map[string]any)
			if !ok || asString(part["type"]) != "tool_use" {
				continue
			}
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
		if len(toolCalls) == 0 {
			return content, role, nil
		}
		message := map[string]any{"role": "assistant", "content": content}
		message["tool_calls"] = toolCalls
		return message, role, nil
	default:
		return anthropicContentToOpenAI(m["content"]), role, nil
	}
}

// anthropicContentToOpenAI converts an Anthropic message body into OpenAI
// content: text blocks become strings or text parts, image sources become
// image_url parts (base64 sources round-trip as data: URIs).
func anthropicContentToOpenAI(value any) any {
	items := asSlice(value)
	if len(items) == 0 {
		return contentText(value)
	}
	var parts []any
	textOnly := true
	for _, raw := range items {
		part, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		switch asString(part["type"]) {
		case "text":
			parts = append(parts, map[string]any{"type": "text", "text": part["text"]})
		case "image":
			source, _ := part["source"].(map[string]any)
			url := asString(source["url"])
			if asString(source["type"]) == "base64" {
				url = "data:" + asString(source["media_type"]) + ";base64," + asString(source["data"])
			}
			parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": url}})
			textOnly = false
		}
	}
	if textOnly && len(parts) == 1 {
		return parts[0].(map[string]any)["text"]
	}
	return parts
}

// toolChoiceToAnthropic maps the OpenAI/Responses tool_choice vocabulary
// ("auto"/"required"/"none" strings or a function object) onto Anthropic's
// always-object form; Anthropic rejects bare strings with a 400.
func toolChoiceToAnthropic(value any) any {
	switch choice := value.(type) {
	case string:
		switch choice {
		case "auto":
			return map[string]any{"type": "auto"}
		case "required":
			return map[string]any{"type": "any"}
		case "none":
			return map[string]any{"type": "none"}
		default:
			return value
		}
	case map[string]any:
		switch asString(choice["type"]) {
		case "function":
			name := asString(choice["name"]) // Responses uses a flat name
			if name == "" {
				if fn, ok := choice["function"].(map[string]any); ok { // Chat nests it
					name = asString(fn["name"])
				}
			}
			if name != "" {
				return map[string]any{"type": "tool", "name": name}
			}
		case "auto", "any", "none", "tool":
			return choice // already Anthropic-shaped
		}
		return value
	default:
		return value
	}
}

// toolChoiceToOpenAI maps Anthropic's object form back onto the OpenAI
// string / function-object vocabulary.
func toolChoiceToOpenAI(value any) any {
	choice, ok := value.(map[string]any)
	if !ok {
		return value // strings are already OpenAI-shaped
	}
	switch asString(choice["type"]) {
	case "auto":
		return "auto"
	case "any":
		return "required"
	case "none":
		return "none"
	case "tool":
		if name := asString(choice["name"]); name != "" {
			return map[string]any{"type": "function", "function": map[string]any{"name": name}}
		}
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
		switch asString(item["type"]) {
		case "function_call":
			// A previous assistant tool call, replayed as history.
			result = append(result, map[string]any{
				"role": "assistant",
				"content": []any{map[string]any{
					"type": "tool_use", "id": item["call_id"], "name": item["name"],
					"input": decodeJSONObject(asString(item["arguments"])),
				}},
			})
		case "function_call_output":
			result = append(result, map[string]any{
				"role": "user",
				"content": []any{map[string]any{
					"type": "tool_result", "tool_use_id": item["call_id"],
					"content": item["output"],
				}},
			})
		default:
			role := asString(item["role"])
			if role == "" {
				role = "user"
			}
			result = append(result, map[string]any{"role": role, "content": item["content"]})
		}
	}
	return result
}

// decodeJSONObject parses a JSON object string, falling back to a raw
// wrapper when the payload is not valid JSON.
func decodeJSONObject(text string) any {
	if text == "" {
		return map[string]any{}
	}
	var parsed any
	if json.Unmarshal([]byte(text), &parsed) == nil {
		return parsed
	}
	return map[string]any{"raw": text}
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

package adapter

import (
	"sort"
	"strings"
)

type aggregatedAnthropicBlock struct {
	data map[string]any
	text strings.Builder
	args strings.Builder
}

func aggregateAnthropicEvent(payload map[string]any, text, reasoning *strings.Builder, blocks map[int]*aggregatedAnthropicBlock, finish *string) {
	index := 0
	if value, ok := numberValueOK(payload["index"]); ok {
		index = int(value)
	}
	switch asString(payload["type"]) {
	case "content_block_start":
		data, ok := payload["content_block"].(map[string]any)
		if !ok {
			return
		}
		block := &aggregatedAnthropicBlock{data: data}
		switch asString(data["type"]) {
		case "text":
			block.text.WriteString(asString(data["text"]))
			text.WriteString(asString(data["text"]))
		case "thinking":
			block.text.WriteString(asString(data["thinking"]))
			reasoning.WriteString(asString(data["thinking"]))
		}
		blocks[index] = block
	case "content_block_delta":
		delta, ok := payload["delta"].(map[string]any)
		if !ok {
			return
		}
		kind := asString(delta["type"])
		var typ string
		switch kind {
		case "text_delta":
			typ = "text"
		case "thinking_delta":
			typ = "thinking"
		case "input_json_delta":
			typ = "tool_use"
		default:
			return
		}
		block := blocks[index]
		if block == nil {
			block = &aggregatedAnthropicBlock{data: map[string]any{"type": typ}}
			blocks[index] = block
		}
		switch kind {
		case "text_delta":
			value := asString(delta["text"])
			block.text.WriteString(value)
			text.WriteString(value)
		case "thinking_delta":
			value := asString(delta["thinking"])
			block.text.WriteString(value)
			reasoning.WriteString(value)
		case "input_json_delta":
			block.args.WriteString(asString(delta["partial_json"]))
		}
	case "message_delta":
		if delta, ok := payload["delta"].(map[string]any); ok {
			if reason := asString(delta["stop_reason"]); reason != "" {
				*finish = reason
			}
		}
	}
}

func aggregatedAnthropicContent(blocks map[int]*aggregatedAnthropicBlock) []any {
	indexes := make([]int, 0, len(blocks))
	for index := range blocks {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	result := make([]any, 0, len(indexes))
	for _, index := range indexes {
		block := blocks[index]
		switch asString(block.data["type"]) {
		case "text":
			block.data["text"] = block.text.String()
		case "thinking":
			block.data["thinking"] = block.text.String()
		case "tool_use":
			if block.args.Len() > 0 {
				block.data["input"] = decodeJSONObject(block.args.String())
			} else if block.data["input"] == nil {
				block.data["input"] = map[string]any{}
			}
		}
		result = append(result, block.data)
	}
	return result
}

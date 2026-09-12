package adapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
)

// decodeJSONValue preserves number lexemes at every boundary, including JSON
// strings nested inside tool arguments. Reject trailing values as Unmarshal did.
func decodeJSONValue(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

// Token counters are integers. Add them without routing decoded integers through
// float64; this also preserves counters above the exact IEEE-754 integer range.
func exactNumberSum(a, b any) any {
	left, lok := new(big.Int).SetString(fmt.Sprint(numberOrZero(a)), 10)
	right, rok := new(big.Int).SetString(fmt.Sprint(numberOrZero(b)), 10)
	if lok && rok {
		return json.Number(new(big.Int).Add(left, right).String())
	}
	return numberValue(a) + numberValue(b)
}

func subtractTokenCounts(total, offset any) any {
	left, lok := new(big.Int).SetString(fmt.Sprint(numberOrZero(total)), 10)
	right, rok := new(big.Int).SetString(fmt.Sprint(numberOrZero(offset)), 10)
	if lok && rok {
		result := new(big.Int).Sub(left, right)
		if result.Sign() < 0 {
			return 0
		}
		return json.Number(result.String())
	}
	value := numberValue(total) - numberValue(offset)
	if value < 0 {
		return 0
	}
	return value
}

func mergeUsage(previous, delta map[string]any) map[string]any {
	result := make(map[string]any, len(previous)+len(delta))
	for key, value := range previous {
		result[key] = value
	}
	for key, value := range delta {
		if value != nil {
			result[key] = value
		}
	}
	return result
}

func anthropicUsageToResponses(usage map[string]any) map[string]any {
	return openAIUsageToResponses(anthropicUsageToOpenAI(usage))
}

func openAIUsageToResponses(usage map[string]any) map[string]any {
	input, output := numberOrZero(usage["prompt_tokens"]), numberOrZero(usage["completion_tokens"])
	result := map[string]any{"input_tokens": input, "output_tokens": output, "total_tokens": sumNumbers(input, output)}
	if total, ok := usage["total_tokens"]; ok && total != nil {
		result["total_tokens"] = total
	}
	if details, ok := usage["prompt_tokens_details"].(map[string]any); ok {
		result["input_tokens_details"] = details
	}
	return result
}

func responsesUsageToOpenAI(usage map[string]any) map[string]any {
	input, output := numberOrZero(usage["input_tokens"]), numberOrZero(usage["output_tokens"])
	result := map[string]any{"prompt_tokens": input, "completion_tokens": output, "total_tokens": sumNumbers(input, output)}
	if details, ok := usage["input_tokens_details"].(map[string]any); ok {
		result["prompt_tokens_details"] = details
	}
	return result
}

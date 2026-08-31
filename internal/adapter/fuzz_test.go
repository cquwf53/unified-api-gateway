package adapter

import (
	"strings"
	"testing"
)

func FuzzSSEParser(f *testing.F) {
	for _, seed := range []string{
		"data: [DONE]\n\n",
		"event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		"data: first\ndata: second\n\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		events, err := ParseSSE(strings.NewReader(raw))
		if err != nil {
			return
		}
		encoded := EncodeSSE(events)
		_, _ = ParseSSE(strings.NewReader(string(encoded)))
	})
}

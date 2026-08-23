// Package adapter contains protocol-level request/response adapters.
//
// Adapters intentionally operate on an HTTP-shaped Envelope rather than on
// net/http.Request directly. This keeps the data plane deterministic: the
// request can be previewed, transformed, recorded, and replayed without
// opening a connection to an upstream.
package adapter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"unified-api-gateway/internal/config"
)

// Envelope is the protocol-neutral HTTP representation used between pipeline
// stages. Body is bounded by the server before an adapter sees it.
type Envelope struct {
	Method      string      `json:"method"`
	URL         *url.URL    `json:"url,omitempty"`
	Header      http.Header `json:"headers"`
	Body        []byte      `json:"body,omitempty"`
	StatusCode  int         `json:"status_code,omitempty"`
	ContentType string      `json:"content_type,omitempty"`
	Stream      bool        `json:"stream,omitempty"`
	Protocol    string      `json:"protocol,omitempty"`
}

func (e Envelope) Clone() Envelope {
	out := e
	out.Header = e.Header.Clone()
	out.Body = append([]byte(nil), e.Body...)
	if e.URL != nil {
		u := *e.URL
		out.URL = &u
	}
	return out
}

// Adapter is a bidirectional protocol adapter. Implementations must not
// mutate the input envelope; this makes preview and shadow modes safe.
type Adapter interface {
	Name() string
	AdaptRequest(Envelope, config.Profile) (Envelope, error)
	AdaptResponse(Envelope, config.Profile) (Envelope, error)
}

// Registry contains built-in adapters and can later be extended with signed
// plugins. Unknown adapters fail closed during configuration validation.
type Registry struct {
	items map[string]Adapter
}

func NewRegistry() *Registry {
	r := &Registry{items: map[string]Adapter{}}
	for _, a := range []Adapter{
		Transparent{},
		OpenAIChatToAnthropic{},
		AnthropicToOpenAIChat{},
		OpenAIResponsesToAnthropic{},
	} {
		r.items[a.Name()] = a
	}
	return r
}

func (r *Registry) Get(name string) (Adapter, bool) {
	if strings.TrimSpace(name) == "" {
		name = "transparent"
	}
	a, ok := r.items[strings.ToLower(strings.TrimSpace(name))]
	return a, ok
}

func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.items))
	for name := range r.items {
		names = append(names, name)
	}
	return names
}

type Transparent struct{}

func (Transparent) Name() string { return "transparent" }
func (Transparent) AdaptRequest(in Envelope, _ config.Profile) (Envelope, error) {
	return in.Clone(), nil
}
func (Transparent) AdaptResponse(in Envelope, _ config.Profile) (Envelope, error) {
	return in.Clone(), nil
}

func decodeJSON(body []byte) (map[string]any, error) {
	var value map[string]any
	if len(body) == 0 {
		return map[string]any{}, nil
	}
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, fmt.Errorf("decode JSON body: %w", err)
	}
	return value, nil
}

func encodeJSON(value map[string]any) (EnvelopeBody []byte, contentType string, err error) {
	b, err := json.Marshal(value)
	return b, "application/json", err
}

func headerOrDefault(h http.Header, key, fallback string) string {
	if value := strings.TrimSpace(h.Get(key)); value != "" {
		return value
	}
	return fallback
}

func ensureJSONHeader(h http.Header) http.Header {
	out := h.Clone()
	if out.Get("Content-Type") == "" {
		out.Set("Content-Type", "application/json")
	}
	return out
}

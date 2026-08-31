package transform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestContentTypeHelpers(t *testing.T) {
	cases := []struct {
		name, contentType string
		want              bool
	}{
		{"empty", "", false},
		{"json", "application/json", true},
		{"json parameters", "Application/JSON; charset=utf-8", true},
		{"vendor json", "application/vnd.example.resource+json; charset=utf-8", true},
		{"text json suffix", "text/example+json", false},
		{"plain text", "text/plain", false},
		{"malformed", "application/json; charset=", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := IsJSONContentType(testCase.contentType); got != testCase.want {
				t.Fatalf("IsJSONContentType(%q)=%v, want %v", testCase.contentType, got, testCase.want)
			}
		})
	}
	fromHeader := Envelope{Headers: http.Header{"Content-Type": {"application/vnd.example+json; charset=utf-8"}}}
	if got := EffectiveContentType(fromHeader); got != "application/vnd.example+json; charset=utf-8" {
		t.Fatalf("header fallback=%q", got)
	}
	fromField := Envelope{ContentType: " application/json; charset=utf-8 ", Headers: http.Header{"Content-Type": {"text/plain"}}}
	if got := EffectiveContentType(fromField); got != "application/json; charset=utf-8" {
		t.Fatalf("explicit content type precedence=%q", got)
	}
}

func TestBodyTransformRejectsExplicitNonJSONContentType(t *testing.T) {
	_, err := Apply(context.Background(), Envelope{
		ContentType: "text/plain; charset=utf-8",
		Body:        []byte(`{"ok":true}`),
	}, TransformSpec{Body: BodySpec{Set: map[string]any{"x": 1}}})
	if err == nil {
		t.Fatal("expected non-JSON content type error")
	}
	var te *TransformError
	if !errors.As(err, &te) || te.Code != "unsupported_content_type" {
		t.Fatalf("expected unsupported_content_type, got %T %v", err, err)
	}
	if !strings.Contains(err.Error(), "text/plain") || !strings.Contains(err.Error(), "JSON") {
		t.Fatalf("error should explain content type requirement: %v", err)
	}
}

func TestBodyTransformUsesHeaderContentTypeAndEmptyRemainsCompatible(t *testing.T) {
	_, err := Apply(context.Background(), Envelope{
		Headers: http.Header{"Content-Type": {"text/plain"}},
		Body:    []byte(`{"ok":true}`),
	}, TransformSpec{Body: BodySpec{Set: map[string]any{"x": 1}}})
	if err == nil {
		t.Fatal("expected header content type error")
	}
	var te *TransformError
	if !errors.As(err, &te) || te.Code != "unsupported_content_type" {
		t.Fatalf("expected unsupported_content_type, got %T %v", err, err)
	}
	out, err := Apply(context.Background(), Envelope{Body: []byte(`{"ok":true}`)}, TransformSpec{Body: BodySpec{Set: map[string]any{"x": 1}}})
	if err != nil {
		t.Fatalf("empty Content-Type should preserve compatibility: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(out.Body, &body); err != nil || body["x"] != float64(1) {
		t.Fatalf("body transform with empty Content-Type failed: %s, %v", out.Body, err)
	}
}

func TestApplyRequestRulesDoesNotMutateInput(t *testing.T) {
	in := Envelope{
		Method:  "POST",
		Path:    "/v1/users",
		Query:   url.Values{"old": {"q"}},
		Headers: http.Header{"X-Old": {"header"}},
		Body:    []byte(`{"user":{"id":7},"name":"Ada"}`),
	}
	spec := TransformSpec{
		Method: "PUT",
		Path:   "/v2/users",
		Query: QuerySpec{
			Rename: map[string]string{"old": "new"},
			Set:    map[string]string{"fixed": "yes"},
		},
		Headers: HeaderSpec{
			Rename: map[string]string{"x-old": "X-New"},
			Set:    map[string]string{"X-Added": "1"},
		},
		Body: BodySpec{
			Rename:  map[string]string{"user.id": "user.user_id"},
			Default: map[string]any{"user.role": "member"},
			Set:     map[string]any{"active": true},
			Delete:  []string{"name"},
		},
	}
	out, err := Apply(context.Background(), in, spec)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if in.Method != "POST" || in.Path != "/v1/users" || in.Query.Get("new") != "" || in.Headers.Get("X-New") != "" {
		t.Fatalf("input envelope was mutated: %#v", in)
	}
	if out.Method != "PUT" || out.Path != "/v2/users" || out.Query.Get("new") != "q" || out.Query.Get("fixed") != "yes" {
		t.Fatalf("transport rules not applied: %#v", out)
	}
	if out.Headers.Get("X-New") != "header" || out.Headers.Get("X-Old") != "" || out.Headers.Get("X-Added") != "1" {
		t.Fatalf("header rules not applied: %#v", out.Headers)
	}
	var body map[string]any
	if err := json.Unmarshal(out.Body, &body); err != nil {
		t.Fatalf("output body is not JSON: %v", err)
	}
	user := body["user"].(map[string]any)
	if user["user_id"] != float64(7) || user["role"] != "member" || body["active"] != true {
		t.Fatalf("body rules not applied: %#v", body)
	}
	if _, ok := body["name"]; ok {
		t.Fatalf("delete rule did not remove name: %#v", body)
	}
}

func TestApplyBodyArrayAndNestedCreation(t *testing.T) {
	in := Envelope{Body: []byte(`{"items":[{"id":1},{"id":2}]}`)}
	spec := TransformSpec{Body: BodySpec{
		Set:     map[string]any{"meta.source": "gateway", "items[1].tag": "second"},
		Default: map[string]any{"items[0].tag": "first", "missing": 3},
		Delete:  []string{"items[0].id"},
	}}
	out, err := Apply(context.Background(), in, spec)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Body, &got); err != nil {
		t.Fatal(err)
	}
	items := got["items"].([]any)
	first := items[0].(map[string]any)
	if _, exists := first["id"]; exists || first["tag"] != "first" {
		t.Fatalf("nested array rules failed: %#v", got)
	}
	second := items[1].(map[string]any)
	if second["tag"] != "second" || got["missing"] != float64(3) || got["meta"].(map[string]any)["source"] != "gateway" {
		t.Fatalf("nested creation failed: %#v", got)
	}
}

func TestApplyBodyMinimum(t *testing.T) {
	spec := TransformSpec{Body: BodySpec{Minimum: map[string]float64{"max_tokens": 128}}}
	for name, input := range map[string]string{
		"below":   `{"max_tokens":1}`,
		"missing": `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			out, err := Apply(context.Background(), Envelope{Body: []byte(input)}, spec)
			if err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			if err := json.Unmarshal(out.Body, &body); err != nil {
				t.Fatal(err)
			}
			if body["max_tokens"] != float64(128) {
				t.Fatalf("minimum not applied: %#v", body)
			}
		})
	}
	out, err := Apply(context.Background(), Envelope{Body: []byte(`{"max_tokens":512}`)}, spec)
	if err != nil {
		t.Fatal(err)
	}
	var preserved map[string]any
	if err := json.Unmarshal(out.Body, &preserved); err != nil {
		t.Fatal(err)
	}
	if preserved["max_tokens"] != float64(512) {
		t.Fatalf("larger value was changed: %#v", preserved)
	}
}

func TestApplyInvalidBodyReturnsTypedError(t *testing.T) {
	_, err := Apply(context.Background(), Envelope{Body: []byte(`{"ok":true} trailing`)}, TransformSpec{Body: BodySpec{Set: map[string]any{"x": 1}}})
	if err == nil {
		t.Fatal("expected malformed JSON error")
	}
	var te *TransformError
	if !errors.As(err, &te) || te.Code != "invalid_json" {
		t.Fatalf("expected TransformError invalid_json, got %T %v", err, err)
	}
}

func TestTransformSpecYAMLSequence(t *testing.T) {
	var spec TransformSpec
	if err := yaml.Unmarshal([]byte("- path: /one\n- path: /two\n"), &spec); err != nil {
		t.Fatal(err)
	}
	if len(spec.Steps) != 2 || spec.Steps[1].Path != "/two" {
		t.Fatalf("sequence not decoded: %#v", spec)
	}
	c, err := Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Apply(context.Background(), Envelope{Path: "/old"})
	if err != nil || out.Path != "/two" {
		t.Fatalf("sequence application failed: %v %#v", err, out)
	}
}

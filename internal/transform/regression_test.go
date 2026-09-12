package transform

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"
)

func TestCompiledBodyDefaultsRemainImmutable(t *testing.T) {
	spec := TransformSpec{Body: BodySpec{
		Default: map[string]any{"meta": map[string]any{"x": 1}, "items": []any{map[string]any{"x": 1}}},
		Set:     map[string]any{"meta.y": 2, "items[0].y": 2},
	}}
	compiled := mustCompile(t, spec)
	before := compiled.Spec()
	in := Envelope{Body: []byte(`{}`), ContentType: "application/json"}
	out, err := compiled.Apply(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Body) != `{"items":[{"x":1,"y":2}],"meta":{"x":1,"y":2}}` {
		t.Fatalf("body = %s", out.Body)
	}
	if !reflect.DeepEqual(before, compiled.Spec()) {
		t.Fatal("applying nested edits mutated the compiled defaults")
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 20 {
				got, err := compiled.Apply(context.Background(), in)
				if err != nil || string(got.Body) != string(out.Body) {
					t.Errorf("concurrent Apply = %s, %v", got.Body, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if !reflect.DeepEqual(before, compiled.Spec()) {
		t.Fatal("concurrent requests mutated the compiled defaults")
	}
}

func TestCompiledBodySetObjectRemainsImmutable(t *testing.T) {
	compiled := mustCompile(t, TransformSpec{Body: BodySpec{
		Set:     map[string]any{"limits": map[string]any{"tokens": 1}},
		Minimum: map[string]float64{"limits.tokens": 10},
	}})
	before := compiled.Spec()
	out, err := compiled.Apply(context.Background(), Envelope{Body: []byte(`{}`)})
	if err != nil || string(out.Body) != `{"limits":{"tokens":10}}` {
		t.Fatalf("Apply = %s, %v", out.Body, err)
	}
	if !reflect.DeepEqual(before, compiled.Spec()) {
		t.Fatal("minimum mutated the compiled set object")
	}
}

func TestCompiledStatusIsIndependentOfCaller(t *testing.T) {
	status := http.StatusCreated
	compiled := mustCompile(t, TransformSpec{Status: &status})
	status = http.StatusForbidden
	copyOf := compiled.Spec()
	*copyOf.Status = http.StatusNotFound
	out, err := compiled.Apply(context.Background(), Envelope{})
	if err != nil || out.StatusCode != http.StatusCreated {
		t.Fatalf("compiled status changed through caller: %d, %v", out.StatusCode, err)
	}
}

func TestTransformMapsUseStableKeyOrder(t *testing.T) {
	compiled := mustCompile(t, TransformSpec{
		Body: BodySpec{
			Rename: map[string]string{"a": "b", "b": "c"},
			Set:    map[string]any{"meta": map[string]any{"x": 1}, "meta.y": 2},
		},
		Headers: HeaderSpec{Rename: map[string]string{"X-A": "X-B", "X-B": "X-C"}},
		Query:   QuerySpec{Rename: map[string]string{"a": "b", "b": "c"}},
	})
	for range 100 {
		out, err := compiled.Apply(context.Background(), Envelope{
			Body:    []byte(`{"a":1,"b":2}`),
			Headers: http.Header{"X-A": {"1"}, "X-B": {"2"}},
			Query:   url.Values{"a": {"1"}, "b": {"2"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if string(out.Body) != `{"c":1,"meta":{"x":1,"y":2}}` || out.Headers.Get("X-C") != "1" || out.Query.Get("c") != "1" {
			t.Fatalf("dependent rules ran out of order: body=%s headers=%v query=%v", out.Body, out.Headers, out.Query)
		}
	}
}

func TestHeaderTransformsCanonicalizeAndRemoveAllCaseVariants(t *testing.T) {
	in := Envelope{Headers: http.Header{
		"x-old": {"lower"}, "X-Old": {"canonical"},
		"x-delete": {"secret"}, "x-set": {"stale"}, "x-add": {"first"},
	}}
	out := mustApply(t, TransformSpec{Headers: HeaderSpec{
		Rename: map[string]string{"x-old": "x-new"},
		Delete: []string{"X-Delete"},
		Set:    map[string]string{"X-Set": "fresh"},
		Add:    map[string]string{"X-Add": "second"},
	}}, in)
	want := http.Header{"X-New": {"canonical", "lower"}, "X-Set": {"fresh"}, "X-Add": {"first", "second"}}
	if !reflect.DeepEqual(out.Headers, want) {
		t.Fatalf("headers = %#v, want %#v", out.Headers, want)
	}
	if in.Headers["x-delete"][0] != "secret" || in.Headers["x-set"][0] != "stale" {
		t.Fatal("input headers were mutated")
	}
}

func TestCompiledTypedJSONValuesAreIndependentOfCaller(t *testing.T) {
	type settings struct {
		Limit *int `json:"limit"`
	}
	limit := 10
	roles := []string{"user"}
	metadata := map[string]string{"scope": "read"}
	options := &settings{Limit: &limit}
	compiled := mustCompile(t, TransformSpec{Body: BodySpec{Set: map[string]any{
		"roles": roles, "metadata": metadata, "options": options,
	}}})
	roles[0] = "admin"
	metadata["scope"] = "write"
	limit = 100
	exposed := compiled.Spec()
	exposed.Body.Set["roles"].([]string)[0] = "owner"
	*exposed.Body.Set["options"].(*settings).Limit = 1000
	out, err := compiled.Apply(context.Background(), Envelope{Body: []byte(`{}`)})
	if err != nil || string(out.Body) != `{"metadata":{"scope":"read"},"options":{"limit":10},"roles":["user"]}` {
		t.Fatalf("caller mutation changed compiled JSON values: %s, %v", out.Body, err)
	}
}

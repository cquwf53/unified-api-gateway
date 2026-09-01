package transform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"
)

func mustApply(t *testing.T, spec TransformSpec, in Envelope) Envelope {
	t.Helper()
	compiled, err := Compile(spec)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := compiled.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return out
}

func TestPathPrefixReplacesFirstSegmentOnly(t *testing.T) {
	out := mustApply(t, TransformSpec{PathPrefix: "/v2"}, Envelope{Path: "/v1/users/42"})
	if out.Path != "/v2/users/42" {
		t.Fatalf("path=%q", out.Path)
	}
	single := mustApply(t, TransformSpec{PathPrefix: "/v2"}, Envelope{Path: "/v1"})
	if single.Path != "/v2" {
		t.Fatalf("single segment path=%q", single.Path)
	}
}

func TestMethodStatusAndStatusMapApply(t *testing.T) {
	status := 201
	out := mustApply(t, TransformSpec{Method: "PUT", Status: &status, StatusMap: map[int]int{201: 200}},
		Envelope{Method: "POST", StatusCode: 201})
	if out.Method != "PUT" || out.StatusCode != 200 {
		t.Fatalf("method=%q status=%d", out.Method, out.StatusCode)
	}
}

func TestQueryRenameDeleteSetAdd(t *testing.T) {
	spec := TransformSpec{Query: QuerySpec{
		Rename: map[string]string{"userid": "id"},
		Delete: []string{"DEBUG"},
		Set:    map[string]string{"src": "gateway"},
		Add:    map[string]string{"tag": "a"},
	}}
	out := mustApply(t, spec, Envelope{Query: url.Values{"UserID": {"7"}, "debug": {"1"}, "src": {"old"}}})
	if got := out.Query.Get("id"); got != "7" {
		t.Fatalf("rename id=%q", got)
	}
	if _, ok := out.Query["debug"]; ok {
		t.Fatalf("case-insensitive delete failed: %v", out.Query)
	}
	if out.Query.Get("src") != "gateway" || len(out.Query["tag"]) != 1 {
		t.Fatalf("set/add failed: %v", out.Query)
	}
}

func TestHeaderRenameIsCaseInsensitiveAndAddAppends(t *testing.T) {
	spec := TransformSpec{Headers: HeaderSpec{
		Rename: map[string]string{"x-old": "X-New"},
		Add:    map[string]string{"X-New": "second"},
	}}
	// A non-canonical map key forces headerValues through its EqualFold scan.
	out := mustApply(t, spec, Envelope{Headers: http.Header{"x-old": {"first"}}})
	values := out.Headers.Values("X-New")
	if len(values) != 2 || values[0] != "first" || values[1] != "second" {
		t.Fatalf("X-New=%v", values)
	}
	if len(out.Headers.Values("x-old")) != 0 {
		t.Fatalf("old header kept: %v", out.Headers)
	}
}

func TestBodySetCreatesIndexedArrays(t *testing.T) {
	out := mustApply(t, TransformSpec{Body: BodySpec{Set: map[string]any{"items[1]": 5}}}, Envelope{Body: []byte(`{}`), ContentType: "application/json"})
	var value map[string]any
	if err := json.Unmarshal(out.Body, &value); err != nil {
		t.Fatal(err)
	}
	items, ok := value["items"].([]any)
	if !ok || len(items) != 2 || items[0] != nil || items[1] != float64(5) {
		t.Fatalf("items=%v", value["items"])
	}
}

func TestBodyDeletePreservesArrayIndexes(t *testing.T) {
	out := mustApply(t, TransformSpec{Body: BodySpec{Delete: []string{"a[1]"}}}, Envelope{Body: []byte(`{"a":[1,2,3]}`), ContentType: "application/json"})
	if string(out.Body) != `{"a":[1,3]}` {
		t.Fatalf("body=%s", out.Body)
	}
}

func TestBodyDefaultFillsMissingAndNull(t *testing.T) {
	out := mustApply(t, TransformSpec{Body: BodySpec{Default: map[string]any{"a": 1, "b": 2, "c": 3}}},
		Envelope{Body: []byte(`{"a":9,"b":null}`), ContentType: "application/json"})
	var value map[string]any
	if err := json.Unmarshal(out.Body, &value); err != nil {
		t.Fatal(err)
	}
	if value["a"] != float64(9) || value["b"] != float64(2) || value["c"] != float64(3) {
		t.Fatalf("defaults=%v", value)
	}
}

func TestBodyMinimumRequiresNumericValue(t *testing.T) {
	compiled, err := Compile(TransformSpec{Body: BodySpec{Minimum: map[string]float64{"max_tokens": 128}}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Apply(context.Background(), Envelope{Body: []byte(`{"max_tokens":"many"}`), ContentType: "application/json"})
	if err == nil {
		t.Fatalf("expected minimum failure, got %s", out.Body)
	}
	var terr *TransformError
	if !errors.As(err, &terr) || terr.Code != "minimum_failed" {
		t.Fatalf("err=%v", err)
	}
}

func TestBodyMinimumCreatesMissingPath(t *testing.T) {
	out := mustApply(t, TransformSpec{Body: BodySpec{Minimum: map[string]float64{"opts.max_tokens": 0.5}}},
		Envelope{Body: []byte(`{}`), ContentType: "application/json"})
	var value map[string]any
	if err := json.Unmarshal(out.Body, &value); err != nil {
		t.Fatal(err)
	}
	opts, _ := value["opts"].(map[string]any)
	if opts == nil || opts["max_tokens"] != 0.5 {
		t.Fatalf("opts=%v", value["opts"])
	}
}

func TestValidateRejectsBadPathsAndRoutes(t *testing.T) {
	if err := (TransformSpec{Body: BodySpec{Set: map[string]any{"a..b": 1}}}).Validate(); err == nil {
		t.Fatal("empty path segment must fail")
	}
	if err := (TransformSpec{Path: "v1/users"}).Validate(); err == nil {
		t.Fatal("relative path must fail")
	}
	if err := (TransformSpec{PathPrefix: "v1"}).Validate(); err == nil {
		t.Fatal("relative path_prefix must fail")
	}
}

func TestUnmarshalJSONAcceptsStepList(t *testing.T) {
	var spec TransformSpec
	if err := json.Unmarshal([]byte(`[{"path":"/a"},{"path":"/b"}]`), &spec); err != nil {
		t.Fatal(err)
	}
	if len(spec.Steps) != 2 || spec.Steps[1].Path != "/b" {
		t.Fatalf("steps=%+v", spec.Steps)
	}
}

func TestIsZeroCloneSpecAndSpecAccessor(t *testing.T) {
	if !(TransformSpec{}).IsZero() {
		t.Fatal("empty spec must be zero")
	}
	if (TransformSpec{Path: "/x"}).IsZero() {
		t.Fatal("path spec must not be zero")
	}
	original := TransformSpec{Body: BodySpec{Set: map[string]any{"a": map[string]any{"deep": 1}}}}
	clone := CloneSpec(original)
	clone.Body.Set["a"].(map[string]any)["deep"] = 2
	if original.Body.Set["a"].(map[string]any)["deep"] != 1 {
		t.Fatal("CloneSpec must deep-copy nested values")
	}
	compiled, err := Compile(original)
	if err != nil {
		t.Fatal(err)
	}
	fromSpec := compiled.Spec()
	fromSpec.Body.Set["a"].(map[string]any)["deep"] = 3
	if compiled.Spec().Body.Set["a"].(map[string]any)["deep"] != 1 {
		t.Fatal("Spec must return a defensive copy")
	}
}

func TestCompileAliasesAndApplyInPlace(t *testing.T) {
	spec := TransformSpec{Path: "/x"}
	if _, err := CompileTransform(spec); err != nil {
		t.Fatalf("CompileTransform: %v", err)
	}
	if _, err := New(spec); err != nil {
		t.Fatalf("New: %v", err)
	}
	env := Envelope{Path: "/y"}
	if err := mustCompile(t, spec).ApplyInPlace(context.Background(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Path != "/x" {
		t.Fatalf("ApplyInPlace path=%q", env.Path)
	}
	if err := ApplyInPlace(context.Background(), &env, TransformSpec{Path: "/z"}); err != nil {
		t.Fatal(err)
	}
	if env.Path != "/z" {
		t.Fatalf("package ApplyInPlace path=%q", env.Path)
	}
}

func TestApplyContextCanceled(t *testing.T) {
	compiled, err := Compile(TransformSpec{Path: "/x"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := compiled.Apply(ctx, Envelope{}); err == nil {
		t.Fatal("canceled context must fail the apply")
	}
}

func TestTransformErrorUnwrap(t *testing.T) {
	sentinel := errors.New("root cause")
	err := &TransformError{Stage: "body", Code: "invalid_json", Err: sentinel}
	if !errors.Is(err, sentinel) {
		t.Fatal("Unwrap must expose the root cause")
	}
}

func TestStreamingSafeFollowsBodyRules(t *testing.T) {
	headerOnly, err := Compile(TransformSpec{Headers: HeaderSpec{Set: map[string]string{"A": "1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !headerOnly.StreamingSafe() {
		t.Fatal("header-only transform must be streaming safe")
	}
	withBody, err := Compile(TransformSpec{Body: BodySpec{Set: map[string]any{"a": 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if withBody.StreamingSafe() {
		t.Fatal("body transform must not be streaming safe")
	}
	withSteps, err := Compile(TransformSpec{Steps: []TransformSpec{{Path: "/x"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !withSteps.StreamingSafe() {
		t.Fatal("steps without body rules must be streaming safe")
	}
}

func mustCompile(t *testing.T, spec TransformSpec) *CompiledTransform {
	t.Helper()
	compiled, err := Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

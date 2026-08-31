// Package transform contains the small, deterministic transformation engine
// used by the gateway.  The package deliberately operates on an Envelope
// rather than on net/http types so that the same rules can be used for
// requests, ordinary responses and recorded fixtures.
package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/strictyaml"
)

// Envelope is the transport-neutral representation passed through a
// transformation pipeline.  Body is kept as bytes until a body rule requires
// JSON decoding.  Headers and Query are copied by Apply before they are
// changed, so callers can safely retain the original envelope.
type Envelope struct {
	Method     string
	Scheme     string
	Host       string
	Path       string
	Query      url.Values
	Headers    http.Header
	Body       []byte
	StatusCode int
	// ContentType is optional; when empty the Content-Type header is used.
	ContentType string
	// Metadata is not transformed by the built-in rules, but is available to
	// adapters and audit hooks.
	Metadata map[string]any
}

// RawEnvelope is an alias retained for adapter implementations that use the
// terminology from the gateway design document.
type RawEnvelope = Envelope

// EffectiveContentType returns the envelope's effective media type. An
// explicitly populated ContentType takes precedence; otherwise the
// Content-Type header is used for compatibility with HTTP-shaped callers.
func EffectiveContentType(env Envelope) string {
	if contentType := strings.TrimSpace(env.ContentType); contentType != "" {
		return contentType
	}
	if env.Headers == nil {
		return ""
	}
	return strings.TrimSpace(env.Headers.Get("Content-Type"))
}

// IsJSONContentType reports whether contentType is JSON or an application
// media type with a +json structured suffix. Media type parameters are parsed
// and ignored, so values such as application/json; charset=utf-8 are accepted.
func IsJSONContentType(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil {
		return false
	}
	mediaType = strings.ToLower(mediaType)
	return mediaType == "application/json" ||
		(strings.HasPrefix(mediaType, "application/") && strings.HasSuffix(mediaType, "+json"))
}

// HeaderSpec describes header changes.  Set replaces an existing value,
// Add appends a value, Rename is case-insensitive, and Delete removes a key.
type HeaderSpec struct {
	Set    map[string]string `yaml:"set,omitempty" json:"set,omitempty"`
	Add    map[string]string `yaml:"add,omitempty" json:"add,omitempty"`
	Rename map[string]string `yaml:"rename,omitempty" json:"rename,omitempty"`
	Delete []string          `yaml:"delete,omitempty" json:"delete,omitempty"`
}

// QuerySpec describes query-string changes.  Values are strings because a
// query string has no JSON type distinction.  A scalar YAML value is accepted
// by the custom unmarshaller below and converted to its textual form.
type QuerySpec struct {
	Set    map[string]string `yaml:"set,omitempty" json:"set,omitempty"`
	Add    map[string]string `yaml:"add,omitempty" json:"add,omitempty"`
	Rename map[string]string `yaml:"rename,omitempty" json:"rename,omitempty"`
	Delete []string          `yaml:"delete,omitempty" json:"delete,omitempty"`
}

// BodySpec contains operations on a JSON object/array.  Paths use a compact
// dot notation (for example "user.id", "messages[0].role", or
// "$.user.id").  Set and Default values can be any JSON-compatible value.
type BodySpec struct {
	Set     map[string]any     `yaml:"set,omitempty" json:"set,omitempty"`
	Rename  map[string]string  `yaml:"rename,omitempty" json:"rename,omitempty"`
	Delete  []string           `yaml:"delete,omitempty" json:"delete,omitempty"`
	Default map[string]any     `yaml:"default,omitempty" json:"default,omitempty"`
	Minimum map[string]float64 `yaml:"minimum,omitempty" json:"minimum,omitempty"`
}

// TransformSpec is intentionally declarative and serialisable.  Steps allows
// a profile to express a sequence while retaining a single spec for the
// common case.  A YAML sequence supplied where a spec is expected is decoded
// into Steps by UnmarshalYAML.
type TransformSpec struct {
	Name       string          `yaml:"name,omitempty" json:"name,omitempty"`
	Method     string          `yaml:"method,omitempty" json:"method,omitempty"`
	Path       string          `yaml:"path,omitempty" json:"path,omitempty"`
	PathPrefix string          `yaml:"path_prefix,omitempty" json:"path_prefix,omitempty"`
	Query      QuerySpec       `yaml:"query,omitempty" json:"query,omitempty"`
	Headers    HeaderSpec      `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body       BodySpec        `yaml:"body,omitempty" json:"body,omitempty"`
	Status     *int            `yaml:"status,omitempty" json:"status,omitempty"`
	StatusMap  map[int]int     `yaml:"status_map,omitempty" json:"status_map,omitempty"`
	StreamSafe bool            `yaml:"streaming_safe,omitempty" json:"streaming_safe,omitempty"`
	Steps      []TransformSpec `yaml:"steps,omitempty" json:"steps,omitempty"`
}

// IsZero reports whether a specification contains no operation.  It is
// useful when profiles expose an optional request/response spec as a value
// rather than a pointer.
func (s TransformSpec) IsZero() bool {
	return s.Name == "" && s.Method == "" && s.Path == "" && s.PathPrefix == "" &&
		len(s.Query.Set) == 0 && len(s.Query.Add) == 0 && len(s.Query.Rename) == 0 && len(s.Query.Delete) == 0 &&
		len(s.Headers.Set) == 0 && len(s.Headers.Add) == 0 && len(s.Headers.Rename) == 0 && len(s.Headers.Delete) == 0 &&
		len(s.Body.Set) == 0 && len(s.Body.Rename) == 0 && len(s.Body.Delete) == 0 && len(s.Body.Default) == 0 && len(s.Body.Minimum) == 0 &&
		s.Status == nil && len(s.StatusMap) == 0 && !s.StreamSafe && len(s.Steps) == 0
}

// CloneSpec returns a deep copy suitable for retaining in a config snapshot.
func CloneSpec(in TransformSpec) TransformSpec { return cloneSpec(in) }

// UnmarshalYAML accepts either a mapping or a list of mappings.  Accepting a
// list is useful for profile request/response fields where users naturally
// write a pipeline directly. Unknown mapping keys anywhere in the spec are
// rejected with their original line numbers so a typo like "renmae" fails at
// load time.
func (s *TransformSpec) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.SequenceNode {
		var steps []TransformSpec
		if err := node.Decode(&steps); err != nil {
			return err
		}
		*s = TransformSpec{Steps: steps}
		return nil
	}
	type plainSpec TransformSpec
	var p plainSpec
	if err := node.Decode(&p); err != nil {
		return err
	}
	if err := strictyaml.UnknownKeys(node, p); err != nil {
		return err
	}
	*s = TransformSpec(p)
	return nil
}

// UnmarshalJSON mirrors the YAML convenience for API clients posting a list
// of transformation steps.
func (s *TransformSpec) UnmarshalJSON(data []byte) error {
	var steps []TransformSpec
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &steps); err != nil {
			return err
		}
		*s = TransformSpec{Steps: steps}
		return nil
	}
	type plain TransformSpec
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	*s = TransformSpec(p)
	return nil
}

// Validate checks a spec before it is activated.  It catches malformed paths
// and conflicting/empty operations early, while leaving values unrestricted
// so custom upstream contracts remain possible.
func (s TransformSpec) Validate() error {
	for i := range s.Steps {
		if err := s.Steps[i].Validate(); err != nil {
			return fmt.Errorf("step %d: %w", i, err)
		}
	}
	for p := range s.Body.Set {
		if _, err := parsePath(p); err != nil {
			return fmt.Errorf("body.set %q: %w", p, err)
		}
	}
	for p := range s.Body.Default {
		if _, err := parsePath(p); err != nil {
			return fmt.Errorf("body.default %q: %w", p, err)
		}
	}
	for p := range s.Body.Minimum {
		if _, err := parsePath(p); err != nil {
			return fmt.Errorf("body.minimum %q: %w", p, err)
		}
	}
	for p, destination := range s.Body.Rename {
		if _, err := parsePath(p); err != nil {
			return fmt.Errorf("body.rename source %q: %w", p, err)
		}
		if _, err := parsePath(destination); err != nil {
			return fmt.Errorf("body.rename destination %q: %w", destination, err)
		}
	}
	for _, p := range s.Body.Delete {
		if _, err := parsePath(p); err != nil {
			return fmt.Errorf("body.delete %q: %w", p, err)
		}
	}
	if s.Path != "" {
		if !strings.HasPrefix(s.Path, "/") {
			return fmt.Errorf("path must start with '/'")
		}
	}
	if s.PathPrefix != "" && !strings.HasPrefix(s.PathPrefix, "/") {
		return fmt.Errorf("path_prefix must start with '/'")
	}
	return nil
}

// TransformError is returned for a rule or stage failure.  Callers can expose
// Code/Path in a protocol-specific error without leaking the original body.
type TransformError struct {
	Stage string
	Code  string
	Path  string
	Err   error
}

func (e *TransformError) Error() string {
	if e == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if e.Stage != "" {
		parts = append(parts, e.Stage)
	}
	if e.Code != "" {
		parts = append(parts, e.Code)
	}
	if e.Path != "" {
		parts = append(parts, e.Path)
	}
	if e.Err != nil {
		if len(parts) == 0 {
			return e.Err.Error()
		}
		return strings.Join(parts, ": ") + ": " + e.Err.Error()
	}
	return strings.Join(parts, ": ")
}

func (e *TransformError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// CompiledTransform is safe to reuse concurrently.  The spec is copied at
// compile time and Apply only works on a clone of the caller's envelope.
type CompiledTransform struct {
	spec TransformSpec
}

// Compile validates and compiles a specification.
func Compile(spec TransformSpec) (*CompiledTransform, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &CompiledTransform{spec: cloneSpec(spec)}, nil
}

// CompileTransform is a descriptive alias used by adapters.
func CompileTransform(spec TransformSpec) (*CompiledTransform, error) { return Compile(spec) }

// New is an alias for Compile.
func New(spec TransformSpec) (*CompiledTransform, error) { return Compile(spec) }

// Spec returns a defensive copy of the compiled specification.
func (c *CompiledTransform) Spec() TransformSpec {
	if c == nil {
		return TransformSpec{}
	}
	return cloneSpec(c.spec)
}

// StreamingSafe reports whether this transform can be applied to individual
// stream frames.  Body operations are not streaming-safe unless explicitly
// marked and contain no body mutation.
func (c *CompiledTransform) StreamingSafe() bool {
	if c == nil {
		return true
	}
	if len(c.spec.Steps) > 0 {
		for i := range c.spec.Steps {
			step, err := Compile(c.spec.Steps[i])
			if err != nil || !step.StreamingSafe() {
				return false
			}
		}
	}
	if c.spec.StreamSafe {
		return true
	}
	return len(c.spec.Body.Set) == 0 && len(c.spec.Body.Rename) == 0 &&
		len(c.spec.Body.Delete) == 0 && len(c.spec.Body.Default) == 0 && len(c.spec.Body.Minimum) == 0
}

// Apply applies the compiled transformation to a cloned envelope and returns
// the result.  The input is never mutated.
func (c *CompiledTransform) Apply(ctx context.Context, in Envelope) (Envelope, error) {
	if c == nil {
		return cloneEnvelope(in), nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	out := cloneEnvelope(in)
	if err := ctx.Err(); err != nil {
		return out, &TransformError{Stage: "transform", Code: "canceled", Err: err}
	}
	if err := applySpec(ctx, c.spec, &out); err != nil {
		return out, err
	}
	return out, nil
}

// ApplyInPlace is useful in the proxy hot path.  It applies a compiled
// transform to an envelope pointer and replaces its fields only on success.
func (c *CompiledTransform) ApplyInPlace(ctx context.Context, in *Envelope) error {
	if in == nil {
		return errors.New("transform: nil envelope")
	}
	out, err := c.Apply(ctx, *in)
	if err != nil {
		return err
	}
	*in = out
	return nil
}

// Apply is a convenience function for one-off transformations.
func Apply(ctx context.Context, in Envelope, spec TransformSpec) (Envelope, error) {
	c, err := Compile(spec)
	if err != nil {
		return cloneEnvelope(in), err
	}
	return c.Apply(ctx, in)
}

// ApplyInPlace is the one-off pointer convenience function.
func ApplyInPlace(ctx context.Context, in *Envelope, spec TransformSpec) error {
	c, err := Compile(spec)
	if err != nil {
		return err
	}
	return c.ApplyInPlace(ctx, in)
}

func applySpec(ctx context.Context, spec TransformSpec, env *Envelope) error {
	for i := range spec.Steps {
		if err := ctx.Err(); err != nil {
			return &TransformError{Stage: "transform", Code: "canceled", Err: err}
		}
		if err := applySpec(ctx, spec.Steps[i], env); err != nil {
			return fmt.Errorf("step %d: %w", i, err)
		}
	}
	if spec.Method != "" {
		env.Method = spec.Method
	}
	if spec.Path != "" {
		env.Path = spec.Path
	}
	if spec.PathPrefix != "" {
		// PathPrefix is a replacement prefix: preserve the suffix from the
		// current path.  It is intentionally deterministic and does not perform
		// regex expansion.
		old := env.Path
		suffix := old
		if i := strings.IndexByte(old, '/'); i >= 0 {
			// A prefix rule replaces only the first path segment when no explicit
			// Path is supplied.  For a leading slash, find the second slash.
			if j := strings.IndexByte(old[1:], '/'); j >= 0 {
				suffix = old[j+1:]
			} else {
				suffix = ""
			}
		}
		env.Path = strings.TrimRight(spec.PathPrefix, "/") + "/" + strings.TrimLeft(suffix, "/")
		if suffix == "" {
			env.Path = strings.TrimRight(spec.PathPrefix, "/")
		}
	}
	applyHeaders(env.Headers, spec.Headers)
	applyQuery(env.Query, spec.Query)
	if spec.Status != nil {
		env.StatusCode = *spec.Status
	}
	if env.StatusCode != 0 && spec.StatusMap != nil {
		if mapped, ok := spec.StatusMap[env.StatusCode]; ok {
			env.StatusCode = mapped
		}
	}
	if hasBodyOps(spec.Body) {
		if err := applyBody(&env.Body, EffectiveContentType(*env), spec.Body); err != nil {
			return err
		}
	}
	return nil
}

func hasBodyOps(b BodySpec) bool {
	return len(b.Set) > 0 || len(b.Rename) > 0 || len(b.Delete) > 0 || len(b.Default) > 0 || len(b.Minimum) > 0
}

func applyHeaders(headers http.Header, spec HeaderSpec) {
	if headers == nil {
		return
	}
	for from, to := range spec.Rename {
		vals := headerValues(headers, from)
		if len(vals) == 0 || strings.EqualFold(from, to) {
			continue
		}
		headers.Del(from)
		headers[to] = append([]string(nil), vals...)
	}
	for _, key := range spec.Delete {
		headers.Del(key)
	}
	for key, value := range spec.Set {
		headers.Set(key, value)
	}
	for key, value := range spec.Add {
		headers.Add(key, value)
	}
}

func headerValues(h http.Header, key string) []string {
	// Fast paths first: net/http canonicalises header keys on insertion, so
	// the exact and canonical lookups hit without scanning.
	if values, ok := h[key]; ok {
		return values
	}
	if values, ok := h[textproto.CanonicalMIMEHeaderKey(key)]; ok {
		return values
	}
	for k, v := range h {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return nil
}

func applyQuery(q url.Values, spec QuerySpec) {
	if q == nil {
		return
	}
	for from, to := range spec.Rename {
		if strings.EqualFold(from, to) {
			continue
		}
		vals, ok := q[from]
		if !ok {
			// url.Values keys are case-sensitive; tolerate case variants.
			for k, v := range q {
				if strings.EqualFold(k, from) {
					vals, ok = v, true
					delete(q, k)
					break
				}
			}
		} else {
			delete(q, from)
		}
		if ok {
			q[to] = append([]string(nil), vals...)
		}
	}
	for _, key := range spec.Delete {
		delete(q, key)
		for k := range q {
			if strings.EqualFold(k, key) {
				delete(q, k)
			}
		}
	}
	for key, value := range spec.Set {
		q.Set(key, value)
	}
	for key, value := range spec.Add {
		q.Add(key, value)
	}
}

func applyBody(raw *[]byte, contentType string, spec BodySpec) error {
	if raw == nil {
		return &TransformError{Stage: "body", Code: "nil_body"}
	}
	if strings.TrimSpace(contentType) != "" && !IsJSONContentType(contentType) {
		return &TransformError{Stage: "body", Code: "unsupported_content_type", Err: fmt.Errorf("body transform requires JSON content type, got %q", contentType)}
	}
	if len(bytes.TrimSpace(*raw)) == 0 {
		return &TransformError{Stage: "body", Code: "invalid_json", Err: errors.New("empty body")}
	}
	var value any
	dec := json.NewDecoder(bytes.NewReader(*raw))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return &TransformError{Stage: "body", Code: "invalid_json", Err: err}
	}
	// Refuse trailing non-whitespace JSON to avoid silently changing malformed
	// payloads.
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return &TransformError{Stage: "body", Code: "invalid_json", Err: errors.New("multiple JSON values")}
	} else if !errors.Is(err, io.EOF) {
		return &TransformError{Stage: "body", Code: "invalid_json", Err: err}
	}
	for path, v := range spec.Rename {
		if err := renameAt(&value, path, v); err != nil {
			return &TransformError{Stage: "body", Code: "rename_failed", Path: path, Err: err}
		}
	}
	for _, path := range spec.Delete {
		if err := deleteAt(&value, path); err != nil {
			return &TransformError{Stage: "body", Code: "delete_failed", Path: path, Err: err}
		}
	}
	for path, v := range spec.Default {
		if err := defaultAt(&value, path, v); err != nil {
			return &TransformError{Stage: "body", Code: "default_failed", Path: path, Err: err}
		}
	}
	for path, v := range spec.Set {
		if err := setAt(&value, path, v); err != nil {
			return &TransformError{Stage: "body", Code: "set_failed", Path: path, Err: err}
		}
	}
	for path, minimum := range spec.Minimum {
		if err := minimumAt(&value, path, minimum); err != nil {
			return &TransformError{Stage: "body", Code: "minimum_failed", Path: path, Err: err}
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return &TransformError{Stage: "body", Code: "encode_failed", Err: err}
	}
	*raw = encoded
	return nil
}

type pathToken struct {
	key   string
	index *int
}

var pathTokenRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_\-]*)(?:\[([0-9]+)\])?$`)

func parsePath(path string) ([]pathToken, error) {
	path = strings.TrimSpace(path)
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return nil, errors.New("path is empty")
	}
	parts := strings.Split(path, ".")
	result := make([]pathToken, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return nil, errors.New("empty path segment")
		}
		m := pathTokenRE.FindStringSubmatch(part)
		if m == nil {
			return nil, fmt.Errorf("invalid segment %q", part)
		}
		// Keep the object key and array index as separate traversal tokens.
		// This makes "items[0].id" work for both reads and writes while
		// retaining a compact syntax for users.
		result = append(result, pathToken{key: m[1]})
		if m[2] != "" {
			i, _ := strconv.Atoi(m[2])
			result = append(result, pathToken{index: &i})
		}
	}
	return result, nil
}

func renameAt(root *any, from, to string) error {
	fromTokens, err := parsePath(from)
	if err != nil {
		return err
	}
	toTokens, err := parsePath(to)
	if err != nil {
		return err
	}
	value, ok := getAt(*root, fromTokens)
	if !ok {
		return nil
	}
	if err := deleteAtTokens(root, fromTokens); err != nil {
		return err
	}
	return setAtTokens(root, toTokens, value)
}

func setAt(root *any, path string, value any) error {
	tokens, err := parsePath(path)
	if err != nil {
		return err
	}
	return setAtTokens(root, tokens, value)
}

func defaultAt(root *any, path string, value any) error {
	tokens, err := parsePath(path)
	if err != nil {
		return err
	}
	if existing, ok := getAt(*root, tokens); ok && existing != nil {
		return nil
	}
	return setAtTokens(root, tokens, value)
}

func minimumAt(root *any, path string, minimum float64) error {
	tokens, err := parsePath(path)
	if err != nil {
		return err
	}
	existing, ok := getAt(*root, tokens)
	if !ok || existing == nil {
		return setAtTokens(root, tokens, normalizedMinimum(minimum))
	}
	value, err := numericValue(existing)
	if err != nil {
		return err
	}
	if value >= minimum {
		return nil
	}
	return setAtTokens(root, tokens, normalizedMinimum(minimum))
}

func numericValue(value any) (float64, error) {
	switch n := value.(type) {
	case json.Number:
		parsed, err := n.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid JSON number %q", n)
		}
		return parsed, nil
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case int32:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("minimum requires a numeric value, received %T", value)
	}
}

func normalizedMinimum(value float64) any {
	if value == float64(int64(value)) {
		return int64(value)
	}
	return value
}

func deleteAt(root *any, path string) error {
	tokens, err := parsePath(path)
	if err != nil {
		return err
	}
	return deleteAtTokens(root, tokens)
}

func getAt(root any, tokens []pathToken) (any, bool) {
	cur := root
	for _, token := range tokens {
		if token.index != nil {
			arr, ok := cur.([]any)
			if !ok || *token.index < 0 || *token.index >= len(arr) {
				return nil, false
			}
			cur = arr[*token.index]
			continue
		}
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = obj[token.key]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func setAtTokens(root *any, tokens []pathToken, value any) error {
	if len(tokens) == 0 {
		*root = value
		return nil
	}
	return setRec(root, tokens, value)
}

func setRec(slot *any, tokens []pathToken, value any) error {
	t := tokens[0]
	if t.index != nil {
		arr, ok := (*slot).([]any)
		if !ok {
			// A nil interface can become an array when an indexed path is set.
			if *slot == nil {
				arr = []any{}
			} else {
				return errors.New("path traverses a non-array value")
			}
		}
		idx := *t.index
		for len(arr) <= idx {
			arr = append(arr, nil)
		}
		if len(tokens) == 1 {
			arr[idx] = value
			*slot = arr
			return nil
		}
		if err := setRec(&arr[idx], tokens[1:], value); err != nil {
			return err
		}
		*slot = arr
		return nil
	}
	obj, ok := (*slot).(map[string]any)
	if !ok {
		if *slot == nil {
			obj = map[string]any{}
		} else {
			return errors.New("path traverses a non-object value")
		}
	}
	if len(tokens) == 1 {
		obj[t.key] = value
		*slot = obj
		return nil
	}
	child, exists := obj[t.key]
	if !exists || child == nil {
		if tokens[1].index != nil {
			child = []any{}
		} else {
			child = map[string]any{}
		}
	}
	if err := setRec(&child, tokens[1:], value); err != nil {
		return err
	}
	obj[t.key] = child
	*slot = obj
	return nil
}

func deleteAtTokens(root *any, tokens []pathToken) error {
	if len(tokens) == 0 {
		return errors.New("cannot delete root")
	}
	return deleteRec(root, tokens)
}

func deleteRec(slot *any, tokens []pathToken) error {
	t := tokens[0]
	if t.index != nil {
		arr, ok := (*slot).([]any)
		if !ok || *t.index < 0 || *t.index >= len(arr) {
			return nil
		}
		if len(tokens) == 1 {
			// Preserve indexes of the remaining array by removing the element.
			idx := *t.index
			copy(arr[idx:], arr[idx+1:])
			arr = arr[:len(arr)-1]
			*slot = arr
			return nil
		}
		return deleteRec(&arr[*t.index], tokens[1:])
	}
	obj, ok := (*slot).(map[string]any)
	if !ok {
		return nil
	}
	if len(tokens) == 1 {
		delete(obj, t.key)
		return nil
	}
	child, ok := obj[t.key]
	if !ok {
		return nil
	}
	if err := deleteRec(&child, tokens[1:]); err != nil {
		return err
	}
	obj[t.key] = child
	return nil
}

func cloneEnvelope(in Envelope) Envelope {
	out := in
	out.Body = append([]byte(nil), in.Body...)
	out.Query = make(url.Values, len(in.Query))
	for k, v := range in.Query {
		out.Query[k] = append([]string(nil), v...)
	}
	out.Headers = make(http.Header, len(in.Headers))
	for k, v := range in.Headers {
		out.Headers[k] = append([]string(nil), v...)
	}
	out.Metadata = cloneMap(in.Metadata)
	return out
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneMap(x)
	case []any:
		a := make([]any, len(x))
		for i := range x {
			a[i] = cloneValue(x[i])
		}
		return a
	default:
		return v
	}
}

func cloneSpec(in TransformSpec) TransformSpec {
	out := in
	out.Query.Set = cloneStringMap(in.Query.Set)
	out.Query.Add = cloneStringMap(in.Query.Add)
	out.Query.Rename = cloneStringMap(in.Query.Rename)
	out.Query.Delete = append([]string(nil), in.Query.Delete...)
	out.Headers.Set = cloneStringMap(in.Headers.Set)
	out.Headers.Add = cloneStringMap(in.Headers.Add)
	out.Headers.Rename = cloneStringMap(in.Headers.Rename)
	out.Headers.Delete = append([]string(nil), in.Headers.Delete...)
	out.Body.Set = cloneMap(in.Body.Set)
	out.Body.Default = cloneMap(in.Body.Default)
	out.Body.Rename = cloneStringMap(in.Body.Rename)
	out.Body.Delete = append([]string(nil), in.Body.Delete...)
	out.Body.Minimum = cloneFloatMap(in.Body.Minimum)
	if in.StatusMap != nil {
		out.StatusMap = make(map[int]int, len(in.StatusMap))
		for k, v := range in.StatusMap {
			out.StatusMap[k] = v
		}
	}
	out.Steps = make([]TransformSpec, len(in.Steps))
	for i := range in.Steps {
		out.Steps[i] = cloneSpec(in.Steps[i])
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneFloatMap(in map[string]float64) map[string]float64 {
	if in == nil {
		return nil
	}
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

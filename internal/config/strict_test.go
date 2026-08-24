package config

import (
	"strings"
	"testing"
)

func TestPolicyTyposRejected(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		typo string
	}{
		{"concurrency", "version: 1\npolicies:\n  defaults:\n    max_concurrenc: 2\n", "max_concurrenc"},
		{"circuit breaker", "version: 1\npolicies:\n  defaults:\n    circuit_breakr:\n      failure_threshold: 3\n", "circuit_breakr"},
		{"audit switch", "version: 1\npolicies:\n  audit:\n    enabl: true\n", "enabl"},
		{"retry attempts", "version: 1\npolicies:\n  defaults:\n    retry:\n      max_attempt: 2\n", "max_attempt"},
		{"request timeout", "version: 1\npolicies:\n  defaults:\n    request_timeout_m: 1000\n", "request_timeout_m"},
	}
	for _, testCase := range cases {
		_, err := Parse([]byte(testCase.yaml))
		if err == nil {
			t.Errorf("%s: policy typo %q accepted silently", testCase.name, testCase.typo)
			continue
		}
		if !strings.Contains(err.Error(), "policies") {
			t.Errorf("%s: error should point at the policies block, got: %v", testCase.name, err)
		}
	}
}

func TestValidPoliciesStillAccepted(t *testing.T) {
	c, err := Parse([]byte(`
version: 1
policies:
  defaults:
    request_timeout_ms: 60000
    max_concurrency: 128
    retry:
      max_attempts: 2
      idempotent_only: true
    circuit_breaker:
      failure_threshold: 5
      reset_timeout_ms: 30000
  audit:
    enabled: true
    redact: [authorization, x-api-key]
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Policies) == 0 {
		t.Fatal("policies block lost")
	}
}

func TestParseCommentOnlyConfigFallsBackToDefaults(t *testing.T) {
	c, err := Parse([]byte("# only comments\n# nothing else\n"))
	if err != nil {
		t.Fatalf("comment-only config must fall back to defaults, got: %v", err)
	}
	if c.Listeners.HTTP != ":8080" || c.Listeners.Admin != ":9090" {
		t.Fatalf("defaults not applied: %#v", c.Listeners)
	}
}

func TestUnknownKeyErrorReportsAccurateLine(t *testing.T) {
	yaml := "version: 1\nprofiles:\n  p:\n    adapter: transparent\n    stream_sfe: true\n"
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("typo accepted")
	}
	if !strings.Contains(err.Error(), "line 5") {
		t.Fatalf("error should report line 5 (original file line), got: %v", err)
	}
}

func TestNamedTransformReferenceValidated(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
profiles:
  p:
    adapter: transparent
    request:
      name: does-not-exist
`))
	if err == nil {
		t.Fatal("missing named transform must fail validation")
	}
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Fatalf("error should name the transform: %v", err)
	}
}

func TestDuplicateRouteMatchersRejected(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
profiles:
  p:
    adapter: transparent
routes:
  - id: one
    match:
      path: /v1/x
    profile: p
  - id: two
    match:
      path: /v1/x
    profile: p
`))
	if err == nil {
		t.Fatal("identical matchers must be rejected")
	}
	if !strings.Contains(err.Error(), "identical matchers") {
		t.Fatalf("error: %v", err)
	}
}

func TestAuthRefValidated(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
upstreams:
  u:
    base_url: http://127.0.0.1:1
    auth_ref: missing-cred
profiles:
  p:
    adapter: transparent
    upstream: u
`))
	if err == nil {
		t.Fatal("missing auth_ref must fail validation")
	}
	if !strings.Contains(err.Error(), "missing-cred") {
		t.Fatalf("error: %v", err)
	}
}

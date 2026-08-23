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

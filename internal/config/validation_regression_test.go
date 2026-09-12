package config

import (
	"encoding/json"
	"strings"
	"testing"

	"unified-api-gateway/internal/transform"
)

func TestParseRejectsAdditionalYAMLDocuments(t *testing.T) {
	for _, raw := range []string{"version: 1\n---\nversion: 2\n", "version: 1\n---\n", "version: 1\n---\ncredentials:\n  hidden:\n    value: literal\n"} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatal("additional YAML document was silently accepted")
		}
	}
}

func TestLegacyListenerAliasesWithDefaultsProfile(t *testing.T) {
	cfg, err := Parse([]byte("version: 1\ndefaults_profile: p\nhttp: '127.0.0.1:8180'\nadmin: '127.0.0.1:8181'\ngrpc: '127.0.0.1:8182'\nprofiles:\n  p:\n    traffic_mode: mock\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listeners != (Listeners{HTTP: "127.0.0.1:8180", Admin: "127.0.0.1:8181", GRPC: "127.0.0.1:8182"}) {
		t.Fatalf("legacy listener aliases lost: %+v", cfg.Listeners)
	}
}

func TestProfileNestedTransformReferenceMustExist(t *testing.T) {
	for _, field := range []string{"request", "response", "request_transforms", "response_transforms"} {
		raw := "version: 1\nprofiles:\n  p:\n    traffic_mode: mock\n    " + field + ":\n      - steps:\n          - name: missing\n"
		if _, err := Parse([]byte(raw)); err == nil || !strings.Contains(err.Error(), "missing") {
			t.Fatalf("%s nested missing reference error=%v", field, err)
		}
	}
}

func TestPersistableRejectsLiteralRefAlongsideEnv(t *testing.T) {
	cfg := NewDefault()
	cfg.Credentials["upstream"] = Credential{Ref: "literal-secret", Env: "UPSTREAM_KEY"}
	if err := cfg.ValidatePersistable(); err == nil {
		t.Fatal("unused env field must not allow the literal ref into version history")
	}
}

func TestPersistableRejectsSensitiveTransformHeaders(t *testing.T) {
	spec := transform.TransformSpec{Steps: []transform.TransformSpec{{Headers: transform.HeaderSpec{Set: map[string]string{"Authorization": "Bearer literal-secret"}, Add: map[string]string{"X-API-Key": "literal-secret"}}}}}
	for _, field := range []string{"request", "response", "request_transforms", "response_transforms", "transforms"} {
		cfg := NewDefault()
		profile := Profile{TrafficMode: "mock"}
		switch field {
		case "request":
			profile.Request = spec
		case "response":
			profile.Response = spec
		case "request_transforms":
			profile.RequestTransforms = []transform.TransformSpec{spec}
		case "response_transforms":
			profile.ResponseTransforms = []transform.TransformSpec{spec}
		case "transforms":
			cfg.Transforms["auth"] = spec
		}
		cfg.Profiles["p"] = profile
		if err := cfg.ValidatePersistable(); err == nil || !strings.Contains(err.Error(), "Authorization") || !strings.Contains(err.Error(), "X-API-Key") {
			t.Fatalf("%s: sensitive transform values were accepted: %v", field, err)
		}
	}
}

func TestShadowedRoutesDoesNotMistakePartialCoverageForFullCoverage(t *testing.T) {
	for _, high := range []RouteMatch{{PathPrefix: "/api"}, {Method: "GET"}, {Path: "/api", PathPrefix: "/only"}} {
		cfg := &Config{Routes: []Route{{ID: "restricted", Priority: 2, Match: high}, {ID: "fallback", Priority: 1}}}
		if warnings := cfg.ShadowedRoutes(); len(warnings) != 0 {
			t.Fatalf("partial route %+v was reported to cover fallback: %v", high, warnings)
		}
	}
}

func TestShadowedRoutesHonorsCombinedAndEmptyQueryConstraints(t *testing.T) {
	for _, test := range []struct{ high, low RouteMatch }{
		{RouteMatch{Path: "/api", PathPrefix: "/other"}, RouteMatch{Path: "/api"}},
		{RouteMatch{Query: map[string]string{"flag": ""}}, RouteMatch{}},
	} {
		cfg := &Config{Routes: []Route{{ID: "restricted", Priority: 2, Match: test.high}, {ID: "fallback", Priority: 1, Match: test.low}}}
		if warnings := cfg.ShadowedRoutes(); len(warnings) != 0 {
			t.Fatalf("route constraints were ignored: %v", warnings)
		}
	}
}

func TestRedactedConfigProtectsNestedHeadersWithoutMutatingRuntime(t *testing.T) {
	cfg := NewDefault()
	cfg.Policies = map[string]any{"audit": map[string]any{"redact": []string{"X-Private"}}}
	cfg.Credentials["env"] = Credential{Type: "env", Ref: "UPSTREAM_KEY"}
	cfg.Credentials["literal"] = Credential{Ref: "dummy-ref-secret", Env: "UNUSED_ENV"}
	spec := transform.TransformSpec{Steps: []transform.TransformSpec{{Headers: transform.HeaderSpec{Set: map[string]string{"X-Private": "dummy-custom-secret"}, Add: map[string]string{"X-API-Key": "dummy-api-secret"}}}}}
	cfg.Profiles["p"] = Profile{TrafficMode: "mock", Request: spec, ResponseTransforms: []transform.TransformSpec{spec}}
	cfg.Transforms["nested"] = spec
	redacted := cfg.Redacted()
	encoded, err := json.Marshal(redacted)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"dummy-ref-secret", "dummy-custom-secret", "dummy-api-secret"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("redacted display contains secret")
		}
	}
	if redacted.Credentials["env"].Ref != "UPSTREAM_KEY" {
		t.Fatal("environment reference name should remain visible")
	}
	if cfg.Credentials["literal"].Ref != "dummy-ref-secret" || cfg.Profiles["p"].Request.Steps[0].Headers.Set["X-Private"] != "dummy-custom-secret" || cfg.Transforms["nested"].Steps[0].Headers.Add["X-API-Key"] != "dummy-api-secret" {
		t.Fatal("display redaction changed the runtime configuration")
	}
}

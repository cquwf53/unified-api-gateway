package config

import (
	"strings"
	"testing"

	"unified-api-gateway/internal/transform"
)

func TestValidateRejectsNamedTransformCycles(t *testing.T) {
	cfg := NewDefault()
	cfg.Transforms["a"] = transform.TransformSpec{Name: "b"}
	cfg.Transforms["b"] = transform.TransformSpec{Name: "a"}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "transform reference cycle") {
		t.Fatalf("error=%v", err)
	}
}

func TestValidateAllowsNestedNamedTransforms(t *testing.T) {
	cfg := NewDefault()
	cfg.Transforms["base"] = transform.TransformSpec{Headers: transform.HeaderSpec{Set: map[string]string{"X-Test": "ok"}}}
	cfg.Transforms["nested"] = transform.TransformSpec{Name: "base"}
	cfg.Profiles["p"] = Profile{TrafficMode: "dry-run", Request: transform.TransformSpec{Name: "nested"}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/transform"
)

func credentialUsesEnvironment(credential Credential) bool {
	switch strings.ToLower(strings.TrimSpace(credential.Type)) {
	case "env", "api_key":
		return true
	default:
		return false
	}
}

// Redacted returns an independent configuration for display. Credential
// literals and sensitive headers are masked, including nested transform chains.
func (c *Config) Redacted() *Config {
	if c == nil {
		return nil
	}
	clone := c.Clone()
	extra := map[string]bool{}
	if encoded, err := yaml.Marshal(c.Policies); err == nil {
		var policies PoliciesSpec
		if yaml.Unmarshal(encoded, &policies) == nil {
			for _, header := range policies.Audit.Redact {
				extra[strings.ToLower(strings.TrimSpace(header))] = true
			}
		}
	}
	redactHeaders := func(_ string, headers map[string]string) {
		for header := range headers {
			if persistSensitiveHeader(header) || extra[strings.ToLower(strings.TrimSpace(header))] {
				headers[header] = "[REDACTED]"
			}
		}
	}
	for id, credential := range clone.Credentials {
		if credential.Value != "" {
			credential.Value = "[REDACTED]"
		}
		if credential.Ref != "" && !isSecretReference(strings.TrimSpace(credential.Ref)) && !credentialUsesEnvironment(credential) {
			credential.Ref = "[REDACTED]"
		}
		clone.Credentials[id] = credential
	}
	for id, client := range clone.Clients {
		if client.Key != "" {
			client.Key = "[REDACTED]"
		}
		if client.KeyRef != "" && !isSecretReference(strings.TrimSpace(client.KeyRef)) {
			client.KeyRef = "[REDACTED]"
		}
		clone.Clients[id] = client
	}
	for _, upstream := range clone.Upstreams {
		redactHeaders("", upstream.Headers)
	}
	visitTransformHeaders(clone, redactHeaders)
	return clone
}

func visitTransformHeaders(c *Config, visit func(string, map[string]string)) {
	var walk func(string, transform.TransformSpec)
	walk = func(path string, spec transform.TransformSpec) {
		visit(path+".headers.set", spec.Headers.Set)
		visit(path+".headers.add", spec.Headers.Add)
		for i, step := range spec.Steps {
			walk(fmt.Sprintf("%s.steps[%d]", path, i), step)
		}
	}
	for id, profile := range c.Profiles {
		path := fmt.Sprintf("profile %q", id)
		walk(path+" request", profile.Request)
		walk(path+" response", profile.Response)
		for i, spec := range profile.RequestTransforms {
			walk(fmt.Sprintf("%s request_transforms[%d]", path, i), spec)
		}
		for i, spec := range profile.ResponseTransforms {
			walk(fmt.Sprintf("%s response_transforms[%d]", path, i), spec)
		}
	}
	for name, spec := range c.Transforms {
		walk(fmt.Sprintf("transform %q", name), spec)
	}
}

package config

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/transform"
)

func TestParseAppliesDefaultsAndProfileTransforms(t *testing.T) {
	data := []byte(`
version: 1
upstreams:
  api:
    base_url: https://example.test/api
profiles:
  user:
    adapter: transparent
    upstream: api
    request:
      path: /v2/users
      body:
        rename:
          user_id: id
routes:
  - id: users
    match:
      path: /v1/users
      methods: [POST]
    profile: user
`)
	c, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if c.Version != 1 || c.Listeners.HTTP != ":8080" || c.Listeners.Admin != ":9090" {
		t.Fatalf("defaults not applied: %#v", c.Listeners)
	}
	p := c.Profiles["user"]
	if p.Name != "user" || p.Adapter != "transparent" || len(p.Request.Body.Rename) != 1 {
		t.Fatalf("profile not decoded/normalised: %#v", p)
	}
	if p.Request.Body.Rename["user_id"] != "id" {
		t.Fatalf("body transform not decoded: %#v", p.Request)
	}
}

func TestParseRejectsBrokenReferences(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
profiles:
  p:
    upstream: missing
routes:
  - profile: missing
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestProfileContractsValidateAgainstAdapterDirections(t *testing.T) {
	valid := []struct {
		name, adapter, client, upstream string
	}{
		{"transparent empty", "transparent", "", ""},
		{"transparent generic", "transparent", "generic-json", "generic-json"},
		{"transparent chat", "transparent", "openai-chat", "openai-chat"},
		{"transparent responses", "transparent", "openai-responses", "openai-responses"},
		{"transparent anthropic", "transparent", "anthropic-messages", "anthropic-messages"},
		{"chat to anthropic", "openai-chat-to-anthropic", "openai-chat", "anthropic-messages"},
		{"anthropic to chat", "anthropic-to-openai-chat", "anthropic-messages", "openai-chat"},
		{"responses to anthropic", "openai-responses-to-anthropic", "openai-responses", "anthropic-messages"},
	}
	for _, testCase := range valid {
		t.Run(testCase.name, func(t *testing.T) {
			data := []byte("version: 1\nprofiles:\n  p:\n    adapter: " + testCase.adapter + "\n    traffic_mode: dry-run\n")
			if testCase.client != "" {
				data = append(data, []byte("    client_contract: "+testCase.client+"\n    upstream_contract: "+testCase.upstream+"\n")...)
			}
			if _, err := Parse(data); err != nil {
				t.Fatalf("valid contract mapping rejected: %v", err)
			}
		})
	}

	invalid := []struct {
		name, yaml, want string
	}{
		{
			name: "unknown client contract",
			yaml: "version: 1\nprofiles:\n  p:\n    adapter: transparent\n    client_contract: made-up\n    upstream_contract: made-up\n    traffic_mode: dry-run\n",
			want: "unknown client_contract",
		},
		{
			name: "unknown upstream contract",
			yaml: "version: 1\nprofiles:\n  p:\n    adapter: transparent\n    client_contract: generic-json\n    upstream_contract: made-up\n    traffic_mode: dry-run\n",
			want: "unknown upstream_contract",
		},
		{
			name: "transparent mismatch",
			yaml: "version: 1\nprofiles:\n  p:\n    adapter: transparent\n    client_contract: openai-chat\n    upstream_contract: anthropic-messages\n    traffic_mode: dry-run\n",
			want: "transparent adapter requires matching contracts",
		},
		{
			name: "semantic direction mismatch",
			yaml: "version: 1\nprofiles:\n  p:\n    adapter: openai-chat-to-anthropic\n    client_contract: anthropic-messages\n    upstream_contract: openai-chat\n    traffic_mode: dry-run\n",
			want: "requires client_contract \"openai-chat\"",
		},
		{
			name: "semantic contracts required",
			yaml: "version: 1\nprofiles:\n  p:\n    adapter: openai-responses-to-anthropic\n    traffic_mode: dry-run\n",
			want: "requires client_contract \"openai-responses\"",
		},
	}
	for _, testCase := range invalid {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := Parse([]byte(testCase.yaml))
			if err == nil {
				t.Fatal("invalid contract mapping was accepted")
			}
			if !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error %q does not contain %q", err, testCase.want)
			}
		})
	}
}

func TestSnapshotCloneIsDeep(t *testing.T) {
	c := NewDefault()
	c.Upstreams["api"] = Upstream{BaseURL: "https://example.test"}
	c.Profiles["p"] = Profile{Upstream: "api", Request: transform.TransformSpec{Body: transform.BodySpec{Set: map[string]any{"x": 1}}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	s, err := NewSnapshot(c, "test")
	if err != nil {
		t.Fatal(err)
	}
	clone := s.Clone()
	clone.Config.Upstreams["api"] = Upstream{BaseURL: "https://changed.test"}
	clone.Config.Profiles["p"] = Profile{Upstream: "api"}
	if s.Config.Upstreams["api"].BaseURL != "https://example.test" {
		t.Fatal("snapshot upstream was aliased")
	}
	if len(s.Config.Profiles["p"].Request.Body.Set) != 1 {
		t.Fatal("snapshot transform was aliased or lost")
	}
	if s.Hash == "" || s.ID == "" || s.Version != 1 {
		t.Fatalf("snapshot metadata missing: %#v", s)
	}
}

func TestLoadFile(t *testing.T) {
	path := t.TempDir() + "/gateway.yaml"
	if err := os.WriteFile(path, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadFile(path)
	if err != nil || c.Version != 1 {
		t.Fatalf("LoadFile() = %#v, %v", c, err)
	}
}

func TestSnapshotIDStableAcrossYAMLRoundTrip(t *testing.T) {
	c := &Config{Version: 1, Profiles: map[string]Profile{"p": {Adapter: "transparent", TrafficMode: "dry-run"}}}
	first, err := NewSnapshot(c, "first")
	if err != nil {
		t.Fatal(err)
	}
	b, err := yaml.Marshal(first.Config)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSnapshot(reloaded, "second")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.Hash != second.Hash {
		t.Fatalf("snapshot changed across round trip: first=%s/%s second=%s/%s", first.ID, first.Hash, second.ID, second.Hash)
	}
}

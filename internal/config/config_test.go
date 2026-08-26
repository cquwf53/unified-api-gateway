package config

import (
	"os"
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

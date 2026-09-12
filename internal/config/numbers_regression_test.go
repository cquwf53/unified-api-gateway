package config

import (
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"testing"
	"unified-api-gateway/internal/transform"
)

func TestSnapshotPreservesLargeIntegerRules(t *testing.T) {
	var previousID string
	for _, integer := range []string{"9007199254740992", "9007199254740993", "18446744073709551615"} {
		raw := "version: 1\nprofiles:\n  p:\n    traffic_mode: mock\n    request:\n      body:\n        set:\n          external_id: " + integer + "\n"
		cfg, err := Parse([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := NewSnapshot(cfg, "integer")
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := transform.Compile(snapshot.Config.Profiles["p"].Request)
		if err != nil {
			t.Fatal(err)
		}
		out, err := compiled.Apply(context.Background(), transform.Envelope{Body: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if string(out.Body) != fmt.Sprintf(`{"external_id":%s}`, integer) {
			t.Errorf("snapshot changed %s to %s", integer, out.Body)
		}
		if snapshot.ID == previousID {
			t.Errorf("distinct integer rules share snapshot ID %s", snapshot.ID)
		}
		previousID = snapshot.ID
		encoded, err := yaml.Marshal(snapshot.Config)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := Parse(encoded)
		if err != nil {
			t.Fatal(err)
		}
		roundTrip, err := NewSnapshot(restored, "restored")
		if err != nil {
			t.Fatal(err)
		}
		if roundTrip.Hash != snapshot.Hash {
			t.Fatal("integer rule changed during YAML round trip")
		}
	}
}

func TestSnapshotNormalizesProgrammaticPolicyNumbers(t *testing.T) {
	cfg := NewDefault()
	cfg.Policies = map[string]any{"defaults": map[string]any{"max_concurrency": json.Number("2")}}
	snapshot, err := NewSnapshot(cfg, "programmatic")
	if err != nil {
		t.Fatal(err)
	}
	// Runtime policy readers marshal the raw policies block separately.
	raw, err := yaml.Marshal(snapshot.Config.Policies)
	if err != nil {
		t.Fatal(err)
	}
	var spec PoliciesSpec
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Defaults.MaxConcurrency != 2 {
		t.Fatal("programmatic policy limit changed")
	}
}

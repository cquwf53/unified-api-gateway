package server

import (
	"encoding/json"
	"sync"
	"testing"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

func TestBuildingPipelinesPreservesSnapshotConfiguration(t *testing.T) {
	cfg := &config.Config{
		Version: 1,
		Transforms: map[string]transform.TransformSpec{
			"leaf":  {Headers: transform.HeaderSpec{Set: map[string]string{"X-Tag": "resolved"}}},
			"chain": {Steps: []transform.TransformSpec{{Name: "leaf"}}},
		},
		Profiles: map[string]config.Profile{
			"preview": {Adapter: "transparent", TrafficMode: "dry-run", Request: transform.TransformSpec{Steps: []transform.TransformSpec{{Name: "chain"}}}},
		},
	}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(snapshot.Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildPipelines(snapshot.Config); err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(snapshot.Config)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("resolving nested transform references mutated the hashed configuration")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := buildPipelines(snapshot.Config); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	rehashed, err := config.NewSnapshot(snapshot.Config, "after-compile")
	if err != nil || rehashed.ID != snapshot.ID {
		t.Fatalf("pipeline compilation changed snapshot identity: %v", err)
	}
}

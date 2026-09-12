package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/store"
)

func TestHotActivationRejectsClientAuthWithoutKeysAndPreservesState(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	base := &config.Config{
		Version:  1,
		Profiles: map[string]config.Profile{"p": {Name: "p", Adapter: "transparent", TrafficMode: "dry-run"}},
		Routes:   []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/thing"}, Profile: "p"}},
	}
	baseSnapshot, err := config.NewSnapshot(base, "base")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gateway, err := New(Options{Snapshot: baseSnapshot, Store: db})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	if err := gateway.Activate(baseSnapshot); err != nil {
		t.Fatal(err)
	}

	candidate := "version: 1\npolicies:\n  defaults:\n    require_client_auth: true\nprofiles:\n  p:\n    adapter: transparent\n    traffic_mode: dry-run\nroutes:\n  - id: r\n    match:\n      path: /thing\n    profile: p\n"
	payload, _ := json.Marshal(map[string]any{"yaml": candidate})
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/config/activate", strings.NewReader(string(payload)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Admin-Token", "test-admin")
	recorder := httptest.NewRecorder()
	gateway.ServeAdminHTTP(recorder, request)
	if recorder.Code != http.StatusUnprocessableEntity || !strings.Contains(recorder.Body.String(), "client_auth_keys_required") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := gateway.Snapshot().ID; got != baseSnapshot.ID {
		t.Fatalf("memory changed after rejected activation: got %s want %s", got, baseSnapshot.ID)
	}
	active, err := db.Active(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if active.ID != baseSnapshot.ID {
		t.Fatalf("stored active version changed: got %s want %s", active.ID, baseSnapshot.ID)
	}
}

func TestGlobalClientKeySatisfiesFailClosedRequirement(t *testing.T) {
	t.Setenv("GATEWAY_CLIENT_API_KEY", "global-secret")
	cfg := &config.Config{Version: 1, Policies: map[string]any{
		"defaults": map[string]any{"require_client_auth": true},
	}}
	if !ClientAuthKeysConfigured(cfg) {
		t.Fatal("global client key should satisfy require_client_auth")
	}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Snapshot: snapshot}); err != nil {
		t.Fatalf("server rejected configured global client key: %v", err)
	}
}

func TestReadyParityIncludesStoreHealth(t *testing.T) {
	cfg := &config.Config{Version: 1}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshot, Store: db})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()

	for _, serve := range []func(http.ResponseWriter, *http.Request){gateway.ServeDataHTTP, gateway.ServeAdminHTTP} {
		recorder := httptest.NewRecorder()
		serve(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("ready status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, serve := range []func(http.ResponseWriter, *http.Request){gateway.ServeDataHTTP, gateway.ServeAdminHTTP} {
		recorder := httptest.NewRecorder()
		serve(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "store") {
			t.Fatalf("unready status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	}
}

func TestActivationFailureRevertsMemoryAndStoredActiveVersion(t *testing.T) {
	base := &config.Config{
		Version:   1,
		Policies:  map[string]any{"defaults": map[string]any{"circuit_breaker": map[string]any{"failure_threshold": 1}}},
		Upstreams: map[string]config.Upstream{"u": {BaseURL: "http://127.0.0.1:1"}},
		Profiles:  map[string]config.Profile{"one": {Name: "one", Adapter: "transparent", TrafficMode: "dry-run", Upstream: "u"}},
		Routes:    []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/thing"}, Profile: "one"}},
	}
	baseSnapshot, err := config.NewSnapshot(base, "base")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := t.TempDir() + "/gateway.db"
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gateway, err := New(Options{Snapshot: baseSnapshot, Store: db})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	if err := gateway.Activate(baseSnapshot); err != nil {
		t.Fatal(err)
	}

	// Delete every newly inserted version after SaveVersion succeeds. This
	// forces Store.Activate to fail after memory has been published and
	// exercises commitConfig's rollback path.
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`CREATE TRIGGER delete_new_version AFTER INSERT ON config_versions
WHEN NEW.id <> '` + baseSnapshot.ID + `'
BEGIN
  DELETE FROM config_versions WHERE id = NEW.id;
END;`); err != nil {
		t.Fatal(err)
	}
	candidate := base.Clone()
	candidate.Policies = map[string]any{"defaults": map[string]any{"circuit_breaker": map[string]any{"failure_threshold": 5}}}
	candidate.Profiles["two"] = config.Profile{Name: "two", Adapter: "transparent", TrafficMode: "dry-run"}
	candidate.Routes[0].Profile = "two"
	candidateSnapshot, err := config.NewSnapshot(candidate, "candidate")
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.Activate(candidateSnapshot); err == nil || !strings.Contains(err.Error(), "activate stored version") {
		t.Fatalf("activation error=%v", err)
	}
	if got := gateway.Snapshot().ID; got != baseSnapshot.ID {
		t.Fatalf("memory kept failed candidate %s; want %s", got, baseSnapshot.ID)
	}
	if threshold := gateway.runtimeFor("one").breaker.threshold; threshold != 1 {
		t.Fatalf("breaker config was not rolled back: threshold=%d", threshold)
	}
	active, err := db.Active(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if active.ID != baseSnapshot.ID {
		t.Fatalf("stored active version=%s want=%s", active.ID, baseSnapshot.ID)
	}
}

func TestConcurrentQuickSwitchesDoNotLoseUpdates(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "test-admin")
	cfg := &config.Config{
		Version: 1,
		Profiles: map[string]config.Profile{
			"one": {Adapter: "transparent", TrafficMode: "dry-run"},
			"two": {Adapter: "transparent", TrafficMode: "dry-run"},
		},
		Routes: []config.Route{
			{ID: "a", Match: config.RouteMatch{Path: "/a"}, Profile: "one"},
			{ID: "b", Match: config.RouteMatch{Path: "/b"}, Profile: "one"},
		},
	}
	snapshot, err := config.NewSnapshot(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gateway, err := New(Options{Snapshot: snapshot, Store: db})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, routeID := range []string{"a", "b"} {
		routeID := routeID
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/admin/v1/profile/switch", strings.NewReader(`{"route_id":"`+routeID+`","profile":"two"}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Admin-Token", "test-admin")
			gateway.ServeAdminHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				t.Errorf("route %s status=%d body=%s", routeID, recorder.Code, recorder.Body.String())
			}
		}()
	}
	close(start)
	wg.Wait()
	for _, route := range gateway.Snapshot().Config.Routes {
		if route.Profile != "two" {
			t.Fatalf("lost concurrent update for route %s: %#v", route.ID, gateway.Snapshot().Config.Routes)
		}
	}
}

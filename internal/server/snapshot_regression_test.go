package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/store"
	"unified-api-gateway/internal/transform"
)

func snapshotOwnershipFixture(t *testing.T) *config.ConfigSnapshot {
	t.Helper()
	cfg := &config.Config{Version: 1, Profiles: map[string]config.Profile{"p": {TrafficMode: "mock"}}, Routes: []config.Route{{ID: "r", Match: config.RouteMatch{Path: "/before"}, Profile: "p"}}}
	snapshot, err := config.NewSnapshot(cfg, "ownership")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestPublishedSnapshotDoesNotShareCallerOwnedState(t *testing.T) {
	for _, useAccessor := range []bool{false, true} {
		snapshot := snapshotOwnershipFixture(t)
		gateway, err := New(Options{Snapshot: snapshot})
		if err != nil {
			t.Fatal(err)
		}
		owned := snapshot
		if useAccessor {
			owned = gateway.Snapshot()
		}
		owned.Config.Routes[0].Match.Path = "/after"
		response := httptest.NewRecorder()
		gateway.ServeDataHTTP(response, httptest.NewRequest(http.MethodPost, "/after", nil))
		gateway.Close()
		if response.Code != http.StatusNotFound {
			t.Fatalf("useAccessor=%v: routing changed without Activate: %d", useAccessor, response.Code)
		}
	}
}

func TestActivationRecomputesContentAddressAndRestoresStoredConfiguration(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	initial := snapshotOwnershipFixture(t)
	gateway, err := New(Options{Snapshot: initial, Store: db})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	if err := gateway.Activate(initial); err != nil {
		t.Fatal(err)
	}
	edited := initial.Clone()
	edited.Config.Routes[0].Match.Path = "/changed"
	profile := edited.Config.Profiles["p"]
	profile.Response = transform.TransformSpec{Body: transform.BodySpec{Set: map[string]any{"external_id": json.Number("9007199254740993")}}}
	edited.Config.Profiles["p"] = profile
	if err := gateway.Activate(edited); err != nil {
		t.Fatal(err)
	}
	active, err := db.Active(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restoredConfig, err := config.Parse(active.YAML)
	if err != nil {
		t.Fatal(err)
	}
	restoredSnapshot, err := config.NewSnapshot(restoredConfig, "restart")
	if err != nil {
		t.Fatal(err)
	}
	if active.ID == initial.ID || active.ID != restoredSnapshot.ID || active.SHA256 != restoredSnapshot.Hash {
		t.Fatalf("stored content address mismatch: initial=%s active=%s restored=%s", initial.ID, active.ID, restoredSnapshot.ID)
	}
	if previous, err := db.GetVersion(context.Background(), initial.ID); err != nil || previous.SHA256 != initial.Hash {
		t.Fatalf("activation overwrote previous version: %v", err)
	}
	restarted, err := New(Options{Snapshot: restoredSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	response := httptest.NewRecorder()
	restarted.ServeDataHTTP(response, httptest.NewRequest(http.MethodPost, "/changed", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"external_id":9007199254740993`) {
		t.Fatalf("restored response=%d %s", response.Code, response.Body)
	}
	edited.Config.Routes[0].Match.Path = "/mutated-after-activation"
	if gateway.Snapshot().Config.Routes[0].Match.Path != "/changed" {
		t.Fatal("activation retained caller-owned configuration")
	}
}

func TestFailedAuditWritesCountAsDroppedEvents(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := New(Options{Snapshot: snapshotOwnershipFixture(t), Store: db})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	gateway.submitAudit(store.AuditEvent{Action: "request"})
	gateway.Close()
	if got := gateway.Metrics().AuditEventsDropped; got != 1 {
		t.Fatalf("failed audit persistence missing from loss counter: %d", got)
	}
}

func TestProgrammaticActivationAuditFailureCountsAsDropped(t *testing.T) {
	path := t.TempDir() + "/gateway.db"
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`CREATE TRIGGER reject_audit BEFORE INSERT ON audit_events BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotOwnershipFixture(t)
	gateway, err := New(Options{Snapshot: snapshot, Store: db})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	if err := gateway.Activate(snapshot); err != nil {
		t.Fatal(err)
	}
	if got := gateway.Metrics().AuditEventsDropped; got != 1 {
		t.Fatalf("activation audit failure missing from loss counter: %d", got)
	}
}

func TestProgrammaticActivationPreservesStatusMapRules(t *testing.T) {
	snapshot := snapshotOwnershipFixture(t)
	profile := snapshot.Config.Profiles["p"]
	profile.Response.StatusMap = map[int]int{200: 201}
	snapshot.Config.Profiles["p"] = profile
	gateway, err := New(Options{Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	if err := gateway.Activate(snapshot); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	gateway.ServeDataHTTP(response, httptest.NewRequest(http.MethodPost, "/before", nil))
	if response.Code != http.StatusCreated {
		t.Fatalf("status mapping changed: %d", response.Code)
	}
}

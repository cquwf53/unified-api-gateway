package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestVersionSaveActivateAndList(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if err := store.SaveVersion(ctx, Version{ID: "v1", CreatedBy: "test", Active: false, SHA256: "a", YAML: []byte("version: 1")}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveVersion(ctx, Version{ID: "v2", CreatedBy: "test", Active: false, SHA256: "b", YAML: []byte("version: 1")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Activate(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	active, err := store.Active(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if active.ID != "v2" {
		t.Fatalf("active=%s", active.ID)
	}
	items, err := store.ListVersions(ctx, 10)
	if err != nil || len(items) != 2 {
		t.Fatalf("versions=%d err=%v", len(items), err)
	}
	for _, item := range items {
		if item.Active == (item.ID == "v1") {
			t.Fatalf("active flag wrong for %s", item.ID)
		}
	}
}

func TestActivateUnknownVersionFails(t *testing.T) {
	store := openTestStore(t)
	if err := store.Activate(context.Background(), "missing"); err == nil {
		t.Fatal("expected error for unknown version")
	}
}

func TestRecordingRoundTripAndFuzzyLookup(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if err := store.SaveRecording(ctx, Recording{Key: "k1", ProfileID: "chat", Method: "POST", Path: "/v1/messages", ConfigID: "cfg-1", StatusCode: 200, ContentType: "application/json", Headers: map[string][]string{"Content-Type": {"application/json"}}, Body: []byte(`{"a":1}`), CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond) // distinct created_at for ordering
	if err := store.SaveRecording(ctx, Recording{Key: "k2", ProfileID: "chat", Method: "POST", Path: "/v1/messages", ConfigID: "cfg-1", StatusCode: 200, ContentType: "application/json", Body: []byte(`{"a":2}`), CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	exact, err := store.GetRecording(ctx, "k1", "", "", "cfg-1")
	if err != nil || string(exact.Body) != `{"a":1}` {
		t.Fatalf("exact=%v err=%v", exact, err)
	}
	latest, err := store.LatestRecordingForProfile(ctx, "chat", "POST", "/v1/messages", "", "", "cfg-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(latest.Body) != `{"a":2}` {
		t.Fatalf("latest body=%s", latest.Body)
	}
	if _, err := store.LatestRecordingForProfile(ctx, "chat", "GET", "/none", "", "", "cfg-1"); err == nil {
		t.Fatal("expected no recording for unknown method/path")
	}
	if _, err := store.GetRecording(ctx, "k1", "", "", "cfg-2"); err == nil {
		t.Fatal("recordings must not cross configuration versions")
	}
}

func TestAuditEvents(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if err := store.AddAudit(ctx, AuditEvent{Action: "request", Actor: "gateway", RequestID: "r1", Details: map[string]any{"status": 200}}); err != nil {
		t.Fatal(err)
	}
	events, err := store.ListAudit(ctx, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
	if events[0].Action != "request" || events[0].Details["status"] != float64(200) {
		t.Fatalf("event=%#v", events[0])
	}
}

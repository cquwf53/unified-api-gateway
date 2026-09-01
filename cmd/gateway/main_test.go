package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/store"
)

func TestActivateDoesNotFallbackOnNon2xxResponse(t *testing.T) {
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/redirect-target")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer admin.Close()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(configPath, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "fallback.db")
	err := activate(configPath, dbPath, admin.URL, "")
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("activation error=%v", err)
	}
	if _, statErr := os.Stat(dbPath); !os.IsNotExist(statErr) {
		t.Fatalf("non-2xx response unexpectedly created fallback database: %v", statErr)
	}
}

func TestHealthUsesReadyEndpointAndRequiresSuccess(t *testing.T) {
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			t.Errorf("path=%q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ready.Close()
	if err := health(ready.URL); err != nil {
		t.Fatal(err)
	}
	unready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unready.Close()
	if err := health(unready.URL + "/readyz"); err == nil {
		t.Fatal("503 readiness response must fail the health command")
	}
}

func TestServeRestoresStoredConfigWhenFileIsMissing(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gateway.db")
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Version: 1, Listeners: config.Listeners{HTTP: "invalid", Admin: "invalid"}}
	snapshot, err := config.NewSnapshot(cfg, "seed")
	if err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(snapshot.Config)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SaveVersion(context.Background(), store.Version{ID: snapshot.ID, Active: true, SHA256: snapshot.Hash, YAML: data}); err != nil {
		t.Fatal(err)
	}
	if err := db.Activate(context.Background(), snapshot.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	err = serve(filepath.Join(dir, "missing.yaml"), dbPath, "", "", false)
	if err == nil {
		t.Fatal("invalid listener should make serve return")
	}
	if strings.Contains(err.Error(), "no stored active configuration") || strings.Contains(err.Error(), "config file could not be loaded") {
		t.Fatalf("stored active configuration was not restored: %v", err)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	os.Stdout = writer
	output := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(reader)
		output <- string(data)
	}()
	fn()
	os.Stdout = original
	writer.Close()
	return <-output
}

func TestApplyLogLevel(t *testing.T) {
	previous := slog.SetLogLoggerLevel(slog.LevelInfo)
	defer slog.SetLogLoggerLevel(previous)
	applyLogLevel("debug")
	if level := slog.SetLogLoggerLevel(slog.LevelInfo); level != slog.LevelDebug {
		t.Fatalf("debug level=%v", level)
	}
	applyLogLevel("warning")
	if level := slog.SetLogLoggerLevel(slog.LevelInfo); level != slog.LevelWarn {
		t.Fatalf("warn level=%v", level)
	}
	applyLogLevel("")
	if level := slog.SetLogLoggerLevel(slog.LevelInfo); level != slog.LevelInfo {
		t.Fatalf("default level=%v", level)
	}
}

func TestPoliciesDecodeRequireClientAuth(t *testing.T) {
	parse := func(t *testing.T, text string) *config.ConfigSnapshot {
		t.Helper()
		cfg, err := config.Parse([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := config.NewSnapshot(cfg, "policy-test")
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	if !policiesDecodeRequireClientAuth(parse(t, "version: 1\npolicies:\n  defaults:\n    require_client_auth: true\n")) {
		t.Fatal("require_client_auth: true must be detected")
	}
	if policiesDecodeRequireClientAuth(parse(t, "version: 1\n")) {
		t.Fatal("unset require_client_auth must be false")
	}
	if policiesDecodeRequireClientAuth(nil) {
		t.Fatal("nil snapshot must be false")
	}
}

func TestPreviewCommandPrintsSnapshot(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(configPath, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if err := preview(configPath); err != nil {
			t.Errorf("preview: %v", err)
		}
	})
	if !strings.Contains(out, `"id"`) || !strings.Contains(out, `"config"`) {
		t.Fatalf("preview output=%q", out)
	}
}

func TestPrintStatusListsVersions(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gateway.db")
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SaveVersion(context.Background(), store.Version{ID: "version-1", CreatedBy: "test", YAML: []byte("version: 1\n")}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if err := printStatus(dbPath); err != nil {
			t.Errorf("status: %v", err)
		}
	})
	if !strings.Contains(out, "version-1") || !strings.Contains(out, "versions") {
		t.Fatalf("status output=%q", out)
	}
}

func TestRollbackCommandValidatesIntegrityAndActivates(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gateway.db")
	cfg := &config.Config{Version: 1, Listeners: config.Listeners{HTTP: ":8780", Admin: "127.0.0.1:8781"}}
	snapshot, err := config.NewSnapshot(cfg, "rollback-test")
	if err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(snapshot.Config)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := db.SaveVersionAndActivate(ctx, store.Version{ID: snapshot.ID, SHA256: snapshot.Hash, YAML: data, CreatedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if err := rollback(dbPath, snapshot.ID); err != nil {
			t.Errorf("rollback: %v", err)
		}
	})
	if !strings.Contains(out, "rolled back") {
		t.Fatalf("rollback output=%q", out)
	}
	db, err = store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	active, err := db.Active(ctx)
	if err != nil || active.ID != snapshot.ID {
		t.Fatalf("active=%+v err=%v", active, err)
	}
}

func TestRollbackCommandRejectsHashMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gateway.db")
	cfg := &config.Config{Version: 1, Listeners: config.Listeners{HTTP: ":8780", Admin: "127.0.0.1:8781"}}
	snapshot, err := config.NewSnapshot(cfg, "rollback-integrity")
	if err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(snapshot.Config)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SaveVersionAndActivate(context.Background(), store.Version{ID: snapshot.ID, SHA256: strings.Repeat("0", 64), YAML: data, CreatedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	err = rollback(dbPath, snapshot.ID)
	if err == nil || !strings.Contains(err.Error(), "integrity") {
		t.Fatalf("expected integrity failure, got %v", err)
	}
}

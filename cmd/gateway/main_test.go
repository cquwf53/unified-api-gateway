package main

import (
	"context"
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

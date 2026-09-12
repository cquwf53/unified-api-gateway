package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/store"
)

func TestServeFailureKeepsPreviousStoredActiveVersion(t *testing.T) {
	for _, failure := range []string{"pipeline", "listener"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "gateway.db")
			previousData := []byte("version: 1\n")
			cfg, err := config.Parse(previousData)
			if err != nil {
				t.Fatal(err)
			}
			previous, err := config.NewSnapshot(cfg, "previous")
			if err != nil {
				t.Fatal(err)
			}
			db, err := store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.SaveVersionAndActivate(context.Background(), store.Version{ID: previous.ID, SHA256: previous.Hash, YAML: previousData}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			candidate := "version: 1\nlisteners:\n  http: '127.0.0.1:0'\n  admin: '127.0.0.1:0'\nprofiles:\n  p:\n    traffic_mode: mock\n"
			if failure == "pipeline" {
				candidate += "    request:\n      - name: missing\n"
			} else {
				busy, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer busy.Close()
				candidate = strings.Replace(candidate, "admin: '127.0.0.1:0'", "admin: '"+busy.Addr().String()+"'", 1)
			}
			path := filepath.Join(dir, "candidate.yaml")
			if err := os.WriteFile(path, []byte(candidate), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := serve(path, dbPath, "", "", true); err == nil {
				t.Fatal("startup should fail")
			}
			db, err = store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			active, err := db.Active(context.Background())
			if err != nil || active.ID != previous.ID {
				t.Fatalf("failed startup changed previous active version: got %s want %s error %v", active.ID, previous.ID, err)
			}
		})
	}
}

func TestPreviewRedactsCredentialAndTransformSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	raw := "version: 1\ncredentials:\n  auth:\n    value: dummy-credential-secret\nprofiles:\n  p:\n    traffic_mode: mock\n    request:\n      headers:\n        set:\n          Authorization: dummy-transform-secret\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if err := preview(path); err != nil {
			t.Error(err)
		}
	})
	if strings.Contains(out, "dummy-credential-secret") || strings.Contains(out, "dummy-transform-secret") {
		t.Fatal("preview exposed literal credentials")
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatal("preview is missing redaction marker")
	}
}

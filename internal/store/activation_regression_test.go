package store

import (
	"context"
	"errors"
	"testing"
)

func TestSaveVersionAndActivateKeepsSingleActive(t *testing.T) {
	store, err := Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SaveVersionAndActivate(ctx, Version{ID: "one", SHA256: "one", YAML: []byte("version: 1\n")}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveVersion(ctx, Version{ID: "two", Active: true, SHA256: "two", YAML: []byte("version: 1\n")}); err != nil {
		t.Fatal(err)
	}
	active, err := store.Active(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if active.ID != "two" {
		t.Fatalf("active=%s", active.ID)
	}
	var count int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM config_versions WHERE active = 1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("active rows=%d", count)
	}
}

func TestActiveDetectsCorruptMultipleMarkers(t *testing.T) {
	store, err := Open(t.TempDir() + "/gateway.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, `DROP INDEX IF EXISTS idx_config_versions_single_active`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		if _, err := store.db.ExecContext(ctx, `INSERT INTO config_versions (id, created_at, created_by, active, sha256, yaml) VALUES (?, 'now', 'test', 1, ?, 'version: 1')`, id, id); err != nil {
			t.Fatal(err)
		}
	}
	_, err = store.Active(ctx)
	if !errors.Is(err, ErrMultipleActiveVersions) {
		t.Fatalf("error=%v", err)
	}
}

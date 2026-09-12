package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestTimestampOrderingUsesNanosecondChronology(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	first := time.Date(2026, 9, 12, 0, 0, 0, 100000000, time.UTC)
	for _, row := range []struct {
		id   string
		when time.Time
	}{{"earlier", first}, {"later", first.Add(time.Nanosecond)}} {
		created := row.when.Format(time.RFC3339Nano)
		if err := db.SaveVersion(ctx, Version{ID: row.id, YAML: []byte("version: 1"), CreatedAt: created}); err != nil {
			t.Fatal(err)
		}
		if err := db.AddAudit(ctx, AuditEvent{Action: row.id, CreatedAt: created}); err != nil {
			t.Fatal(err)
		}
		if err := db.SaveRecording(ctx, Recording{Key: row.id, ProfileID: "fixture", Method: "POST", Path: "/x", ConfigID: "cfg", StatusCode: 200, Body: []byte(`{}`), CreatedAt: created}); err != nil {
			t.Fatal(err)
		}
	}
	assertNewestTimestamp(t, db, "later")
}

func assertNewestTimestamp(t *testing.T, db *Store, want string) {
	t.Helper()
	ctx := context.Background()
	versions, err := db.ListVersions(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	audits, err := db.ListAudit(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	recordings, err := db.ListRecordings(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := db.LatestRecordingForProfile(ctx, "fixture", "POST", "/x", "", "", "cfg")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0].ID != want {
		t.Errorf("versions latest=%v", versions)
	}
	if len(audits) != 1 || audits[0].Action != want {
		t.Errorf("audit latest=%v", audits)
	}
	if len(recordings) != 1 || recordings[0].Key != want {
		t.Errorf("recordings latest=%v", recordings)
	}
	if latest.Key != want {
		t.Errorf("fuzzy replay latest=%s want=%s", latest.Key, want)
	}
}

func TestOpenNormalizesLegacyTimestampsWithoutChangingHistoryContents(t *testing.T) {
	path := t.TempDir() + "/legacy.db"
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, row := range []struct{ id, created string }{{"earlier", "2026-09-12T00:00:00.1Z"}, {"later", "2026-09-12T08:00:00.100000001+08:00"}} {
		if err := db.SaveVersion(ctx, Version{ID: row.id, YAML: []byte("version: 1\n# retain original bytes"), CreatedAt: row.created}); err != nil {
			t.Fatal(err)
		}
		if err := db.AddAudit(ctx, AuditEvent{Action: row.id, Details: map[string]any{"kept": true}, CreatedAt: row.created}); err != nil {
			t.Fatal(err)
		}
		if err := db.SaveRecording(ctx, Recording{Key: row.id, ProfileID: "fixture", Method: "POST", Path: "/x", ConfigID: "cfg", StatusCode: 200, Body: []byte(`{"kept":true}`), CreatedAt: row.created}); err != nil {
			t.Fatal(err)
		}
		// Model a pre-migration store containing the original variable-precision
		// RFC3339Nano fields, including valid non-UTC timestamps supplied by callers.
		for _, table := range []string{"config_versions", "audit_events", "recordings"} {
			key := "id"
			if table == "audit_events" {
				key = "action"
			}
			if table == "recordings" {
				key = "key"
			}
			if _, err := db.db.ExecContext(ctx, "UPDATE "+table+" SET created_at = ? WHERE "+key+" = ?", row.created, row.id); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Activate(ctx, "earlier"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, "DROP TABLE store_migrations"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `CREATE TRIGGER reject_timestamp_update BEFORE UPDATE OF created_at ON audit_events BEGIN SELECT RAISE(FAIL, 'migration failure'); END;`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if failed, err := Open(path); err == nil {
		failed.Close()
		t.Fatal("migration must report a failed timestamp update")
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var unchanged string
	if err := raw.QueryRow(`SELECT created_at FROM config_versions WHERE id = 'earlier'`).Scan(&unchanged); err != nil {
		t.Fatal(err)
	}
	if unchanged != "2026-09-12T00:00:00.1Z" {
		t.Fatal("failed migration committed a partial timestamp rewrite")
	}
	if _, err := raw.Exec(`DROP TRIGGER reject_timestamp_update`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		db, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		assertNewestTimestamp(t, db, "later")
		active, err := db.Active(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if active.ID != "earlier" || string(active.YAML) != "version: 1\n# retain original bytes" {
			t.Fatal("timestamp migration changed the active config or YAML contents")
		}
		recording, err := db.GetRecording(ctx, "later", "", "", "cfg")
		if err != nil {
			t.Fatal(err)
		}
		if string(recording.Body) != `{"kept":true}` {
			t.Fatal("timestamp migration changed recording body")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Package store persists configuration history and lightweight audit data.
// It intentionally keeps the data model small so the gateway can run as a
// single binary; a future distributed control plane can replace this package.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/glebarez/sqlite"
)

type Store struct {
	db *sql.DB
}

// ErrMultipleActiveVersions reports a corrupted history containing more than
// one active configuration version. Callers can use errors.Is to distinguish
// this from a database or missing-row error.
var ErrMultipleActiveVersions = errors.New("store: multiple active configuration versions")

// ErrLiteralSecret reports YAML that would persist a credential or sensitive
// header value literally. Secret values must remain references so rollback can
// retain the original persisted configuration without redacting it.
var ErrLiteralSecret = errors.New("store: literal secret cannot be persisted")

// LiteralSecretError identifies the YAML path that contained a literal secret.
type LiteralSecretError struct {
	Path string
}

func (e *LiteralSecretError) Error() string {
	if e == nil || e.Path == "" {
		return ErrLiteralSecret.Error()
	}
	return fmt.Sprintf("%s at %s", ErrLiteralSecret, e.Path)
}

func (e *LiteralSecretError) Unwrap() error { return ErrLiteralSecret }

type Version struct {
	ID          string `json:"id"`
	CreatedAt   string `json:"created_at"`
	CreatedBy   string `json:"created_by"`
	Active      bool   `json:"active"`
	SHA256      string `json:"sha256"`
	Description string `json:"description,omitempty"`
	YAML        []byte `json:"-"`
}

type AuditEvent struct {
	Action    string         `json:"action"`
	Actor     string         `json:"actor"`
	VersionID string         `json:"version_id,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
	CreatedAt string         `json:"created_at"`
}

func Open(path string) (*Store, error) {
	if path == "" {
		path = "data/gateway.db"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// pragma statements applied right after opening. WAL keeps readers and the
// single writer from blocking each other; busy_timeout avoids spurious
// "database is locked" errors under concurrent access.
func (s *Store) pragmas(ctx context.Context) error {
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("sqlite %s: %w", statement, err)
		}
	}
	return nil
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS config_versions (
  id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  created_by TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 0,
  sha256 TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  yaml BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_config_versions_created ON config_versions(created_at DESC);
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action TEXT NOT NULL,
  actor TEXT NOT NULL,
  version_id TEXT NOT NULL DEFAULT '',
  request_id TEXT NOT NULL DEFAULT '',
  details_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_events_created ON audit_events(created_at DESC);
CREATE TABLE IF NOT EXISTS recordings (
  key TEXT PRIMARY KEY,
  profile_id TEXT NOT NULL,
  method TEXT NOT NULL DEFAULT '',
  path TEXT NOT NULL DEFAULT '',
  client_id TEXT NOT NULL DEFAULT '',
  tenant TEXT NOT NULL DEFAULT '',
  config_id TEXT NOT NULL DEFAULT '',
  status_code INTEGER NOT NULL,
  content_type TEXT NOT NULL DEFAULT '',
  headers_json TEXT NOT NULL DEFAULT '{}',
  body BLOB NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recordings_created ON recordings(created_at DESC);
`)
	if err != nil {
		return fmt.Errorf("migrate sqlite store: %w", err)
	}
	// Additive migrations for databases created before replay matching stored
	// the request method and path. Duplicate-column errors mean the column is
	// already present and are ignored.
	for _, statement := range []string{
		`ALTER TABLE recordings ADD COLUMN method TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE recordings ADD COLUMN path TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE recordings ADD COLUMN client_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE recordings ADD COLUMN tenant TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE recordings ADD COLUMN config_id TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			if !strings.Contains(err.Error(), "duplicate column") {
				return fmt.Errorf("migrate sqlite store: %w", err)
			}
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_recordings_replay
ON recordings(config_id, profile_id, method, path, client_id, tenant, created_at DESC)`); err != nil {
		return fmt.Errorf("migrate sqlite store: %w", err)
	}
	if err := s.ensureSingleActiveIndex(ctx); err != nil {
		return fmt.Errorf("migrate sqlite store: %w", err)
	}
	return s.pragmas(ctx)
}

// ensureSingleActiveIndex adds the invariant for new and already-consistent
// stores. Older stores may contain multiple active rows; migration must remain
// usable for those stores so Active can report the corruption instead of
// silently choosing a row or making the database unopenable. Once an operator
// or a normal activation repairs the duplicate state, the index is added on
// the next migration.
func (s *Store) ensureSingleActiveIndex(ctx context.Context) error {
	var activeCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM config_versions WHERE active <> 0`).Scan(&activeCount); err != nil {
		return err
	}
	if activeCount > 1 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_config_versions_single_active
ON config_versions(active) WHERE active <> 0`)
	return err
}

func ensureSingleActiveIndexTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_config_versions_single_active
ON config_versions(active) WHERE active <> 0`)
	return err
}

// Ping verifies the database connection is still usable.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("store: not open")
	}
	return s.db.PingContext(ctx)
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) SaveVersion(ctx context.Context, version Version) error {
	if version.ID == "" {
		return fmt.Errorf("version id is required")
	}
	if version.CreatedAt == "" {
		version.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if version.CreatedBy == "" {
		version.CreatedBy = "system"
	}
	if version.Active {
		return s.SaveVersionAndActivate(ctx, version)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO config_versions
 (id, created_at, created_by, active, sha256, description, yaml)
 VALUES (?, ?, ?, 0, ?, ?, ?)
 ON CONFLICT(id) DO UPDATE SET
   created_at = excluded.created_at,
   created_by = excluded.created_by,
   sha256 = excluded.sha256,
   description = excluded.description,
   yaml = excluded.yaml`, version.ID, version.CreatedAt, version.CreatedBy,
		version.SHA256, version.Description, version.YAML)
	return err
}

// SaveVersionAndActivate persists a candidate and flips the single active
// marker in one transaction. A crash or statement failure therefore leaves the
// previously active version intact.
func (s *Store) SaveVersionAndActivate(ctx context.Context, version Version) error {
	if version.ID == "" {
		return fmt.Errorf("version id is required")
	}
	if version.CreatedAt == "" {
		version.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if version.CreatedBy == "" {
		version.CreatedBy = "system"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO config_versions
 (id, created_at, created_by, active, sha256, description, yaml)
 VALUES (?, ?, ?, 0, ?, ?, ?)`, version.ID, version.CreatedAt, version.CreatedBy,
		version.SHA256, version.Description, version.YAML); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE config_versions SET active = 0`); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE config_versions SET active = 1 WHERE id = ?`, version.ID)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return fmt.Errorf("configuration version %q not found", version.ID)
	}
	if err := ensureSingleActiveIndexTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Activate(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE config_versions SET active = 0`); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE config_versions SET active = 1 WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return fmt.Errorf("configuration version %q not found", id)
	}
	if err := ensureSingleActiveIndexTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Active(ctx context.Context) (Version, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, created_at, created_by, active, sha256, description, yaml
FROM config_versions WHERE active = 1 ORDER BY created_at DESC LIMIT 2`)
	if err != nil {
		return Version{}, err
	}
	defer rows.Close()
	var versions []Version
	for rows.Next() {
		var v Version
		var active int
		if err := rows.Scan(&v.ID, &v.CreatedAt, &v.CreatedBy, &active, &v.SHA256, &v.Description, &v.YAML); err != nil {
			return Version{}, err
		}
		v.Active = active != 0
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return Version{}, err
	}
	if len(versions) == 0 {
		return Version{}, sql.ErrNoRows
	}
	if len(versions) > 1 {
		return Version{}, ErrMultipleActiveVersions
	}
	return versions[0], nil
}

func (s *Store) GetVersion(ctx context.Context, id string) (Version, error) {
	return s.get(ctx, `SELECT id, created_at, created_by, active, sha256, description, yaml
FROM config_versions WHERE id = ?`, id)
}

func (s *Store) get(ctx context.Context, query string, args ...any) (Version, error) {
	var v Version
	var active int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&v.ID, &v.CreatedAt, &v.CreatedBy,
		&active, &v.SHA256, &v.Description, &v.YAML)
	if err != nil {
		return Version{}, err
	}
	v.Active = active != 0
	return v, nil
}

func (s *Store) ListVersions(ctx context.Context, limit int) ([]Version, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, created_at, created_by, active, sha256, description
FROM config_versions ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Version
	for rows.Next() {
		var v Version
		var active int
		if err := rows.Scan(&v.ID, &v.CreatedAt, &v.CreatedBy, &active, &v.SHA256, &v.Description); err != nil {
			return nil, err
		}
		v.Active = active != 0
		result = append(result, v)
	}
	return result, rows.Err()
}

func (s *Store) AddAudit(ctx context.Context, event AuditEvent) error {
	if event.CreatedAt == "" {
		event.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if event.Actor == "" {
		event.Actor = "system"
	}
	details := event.Details
	if details == nil {
		details = map[string]any{}
	}
	b, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO audit_events
 (action, actor, version_id, request_id, details_json, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		event.Action, event.Actor, event.VersionID, event.RequestID, string(b), event.CreatedAt)
	return err
}

func (s *Store) ListAudit(ctx context.Context, limit int) ([]AuditEvent, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT action, actor, version_id, request_id, details_json, created_at
FROM audit_events ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []AuditEvent
	for rows.Next() {
		var event AuditEvent
		var details string
		if err := rows.Scan(&event.Action, &event.Actor, &event.VersionID, &event.RequestID, &details, &event.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(details), &event.Details)
		result = append(result, event)
	}
	return result, rows.Err()
}

type Recording struct {
	Key         string              `json:"key"`
	ProfileID   string              `json:"profile_id"`
	Method      string              `json:"method,omitempty"`
	Path        string              `json:"path,omitempty"`
	ClientID    string              `json:"client_id,omitempty"`
	Tenant      string              `json:"tenant,omitempty"`
	ConfigID    string              `json:"config_id,omitempty"`
	StatusCode  int                 `json:"status_code"`
	ContentType string              `json:"content_type,omitempty"`
	Headers     map[string][]string `json:"headers,omitempty"`
	Body        []byte              `json:"-"`
	CreatedAt   string              `json:"created_at"`
}

func (s *Store) SaveRecording(ctx context.Context, recording Recording) error {
	if recording.Key == "" {
		return fmt.Errorf("recording key is required")
	}
	if recording.CreatedAt == "" {
		recording.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if recording.Headers == nil {
		recording.Headers = map[string][]string{}
	}
	headers, err := json.Marshal(recording.Headers)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT OR REPLACE INTO recordings
	 (key, profile_id, method, path, client_id, tenant, config_id, status_code, content_type, headers_json, body, created_at)
	 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, recording.Key, recording.ProfileID, recording.Method, recording.Path,
		recording.ClientID, recording.Tenant, recording.ConfigID, recording.StatusCode, recording.ContentType, string(headers), recording.Body, recording.CreatedAt)
	return err
}

func (s *Store) GetRecording(ctx context.Context, key string, clientID, tenant, configID string) (Recording, error) {
	var recording Recording
	var headers string
	err := s.db.QueryRowContext(ctx, `SELECT key, profile_id, method, path, client_id, tenant, config_id, status_code, content_type, headers_json, body, created_at
	FROM recordings WHERE key = ? AND client_id = ? AND tenant = ? AND config_id = ?`, key, clientID, tenant, configID).Scan(&recording.Key, &recording.ProfileID, &recording.Method, &recording.Path,
		&recording.ClientID, &recording.Tenant, &recording.ConfigID,
		&recording.StatusCode, &recording.ContentType, &headers, &recording.Body, &recording.CreatedAt)
	if err != nil {
		return Recording{}, err
	}
	if err := json.Unmarshal([]byte(headers), &recording.Headers); err != nil {
		return Recording{}, err
	}
	return recording, nil
}

// LatestRecordingForProfile returns the most recent recording of a given
// method and path for a profile. It backs the fuzzy replay fallback: exact
// request hashes rarely match twice for chat workloads where the body
// changes on every call.
func (s *Store) LatestRecordingForProfile(ctx context.Context, profileID, method, path, clientID, tenant, configID string) (Recording, error) {
	var recording Recording
	var headers string
	err := s.db.QueryRowContext(ctx, `SELECT key, profile_id, method, path, client_id, tenant, config_id, status_code, content_type, headers_json, body, created_at
	FROM recordings WHERE profile_id = ? AND method = ? AND path = ? AND client_id = ? AND tenant = ? AND config_id = ?
	ORDER BY created_at DESC LIMIT 1`, profileID, method, path, clientID, tenant, configID).Scan(&recording.Key, &recording.ProfileID, &recording.Method, &recording.Path,
		&recording.ClientID, &recording.Tenant, &recording.ConfigID,
		&recording.StatusCode, &recording.ContentType, &headers, &recording.Body, &recording.CreatedAt)
	if err != nil {
		return Recording{}, err
	}
	if err := json.Unmarshal([]byte(headers), &recording.Headers); err != nil {
		return Recording{}, err
	}
	return recording, nil
}

func (s *Store) ListRecordings(ctx context.Context, limit int) ([]Recording, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT key, profile_id, method, path, client_id, tenant, config_id, status_code, content_type, created_at
FROM recordings ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Recording
	for rows.Next() {
		var recording Recording
		if err := rows.Scan(&recording.Key, &recording.ProfileID, &recording.Method, &recording.Path, &recording.ClientID, &recording.Tenant, &recording.ConfigID, &recording.StatusCode, &recording.ContentType, &recording.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, recording)
	}
	return result, rows.Err()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

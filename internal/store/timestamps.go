package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Fixed precision UTC timestamps sort chronologically using the existing TEXT
// indexes, including nanoseconds that SQLite's date functions would truncate.
const storedTimestampLayout = "2006-01-02T15:04:05.000000000Z"
const timestampMigration = "fixed_precision_utc_timestamps_v1"

func normalizedTimestamp(value string) (string, error) {
	if value == "" {
		return time.Now().UTC().Format(storedTimestampLayout), nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return "", err
	}
	return parsed.UTC().Format(storedTimestampLayout), nil
}

// Normalize the old RFC3339Nano representation once, in bounded batches and a
// single transaction. Responses and YAML remain untouched. Invalid legacy
// timestamps are retained for inspection instead of preventing the store from
// opening; all new writes require a valid timestamp.
func (s *Store) normalizeStoredTimestamps(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var recorded string
	err = tx.QueryRowContext(ctx, `SELECT name FROM store_migrations WHERE name = ?`, timestampMigration).Scan(&recorded)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	type timestampRow struct {
		rowID int64
		value string
	}
	for _, table := range []string{"config_versions", "audit_events", "recordings"} {
		var after int64
		for {
			rows, err := tx.QueryContext(ctx, "SELECT rowid, created_at FROM "+table+" WHERE rowid > ? ORDER BY rowid LIMIT 512", after)
			if err != nil {
				return err
			}
			batch := make([]timestampRow, 0, 512)
			for rows.Next() {
				var row timestampRow
				if err := rows.Scan(&row.rowID, &row.value); err != nil {
					rows.Close()
					return err
				}
				batch = append(batch, row)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
			for _, row := range batch {
				if row.value == "" {
					continue
				}
				fixed, err := normalizedTimestamp(row.value)
				if err != nil || fixed == row.value {
					continue
				}
				if _, err := tx.ExecContext(ctx, "UPDATE "+table+" SET created_at = ? WHERE rowid = ?", fixed, row.rowID); err != nil {
					return err
				}
			}
			after = batch[len(batch)-1].rowID
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO store_migrations (name) VALUES (?)`, timestampMigration); err != nil {
		return err
	}
	return tx.Commit()
}

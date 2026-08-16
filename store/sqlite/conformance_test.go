package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/sqlite"
	"github.com/gnikyt/cq-dashboard/store/storetest"
)

// The SQLite driver is held to the same contract as any other backend.
func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T) store.Store {
		st, err := sqlite.Open(":memory:")
		if err != nil {
			t.Fatalf("Open(): %v", err)
		}
		if err := st.Migrate(context.Background()); err != nil {
			t.Fatalf("Migrate(): %v", err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	})
}

// A caller-supplied database is held to the contract too: New is a real seam,
// not a convenience that only half works.
func TestConformanceWithSuppliedDB(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T) store.Store {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("sql.Open(): %v", err)
		}
		db.SetMaxOpenConns(1) // ":memory:" is per-connection.
		st := sqlite.New(db)
		if err := st.Migrate(context.Background()); err != nil {
			t.Fatalf("Migrate(): %v", err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	})
}

// The DSN form in New's documentation has to actually apply the pragmas, or
// the advice is worse than none: callers would believe they were protected
// against SQLITE_BUSY when they were not.
func TestDocumentedDSNAppliesPragmas(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cq.db")
	db, err := sql.Open("sqlite",
		path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("sql.Open(): %v", err)
	}
	defer db.Close()

	var timeout int
	if err := db.QueryRow("PRAGMA busy_timeout").Scan(&timeout); err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if timeout != 5000 {
		t.Errorf("busy_timeout = %d, want 5000", timeout)
	}

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q, want %q", mode, "wal")
	}
}

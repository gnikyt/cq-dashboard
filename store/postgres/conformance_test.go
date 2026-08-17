package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/postgres"
	"github.com/gnikyt/cq-dashboard/store/storetest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// The driver is held to the same contract as every other backend. It needs a
// real server, so the suite is skipped unless one is pointed at:
//
//	CQ_DASH_PG_DSN=postgres://user:pass@localhost:5432/cqdash go test ./store/postgres/
func TestConformance(t *testing.T) {
	dsn := os.Getenv("CQ_DASH_PG_DSN")
	if dsn == "" {
		t.Skip("set CQ_DASH_PG_DSN to run the Postgres conformance suite")
	}

	storetest.RunSuite(t, func(t *testing.T) store.Store {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("sql.Open(): %v", err)
		}
		st := postgres.New(db)
		if err := st.Migrate(context.Background()); err != nil {
			t.Fatalf("Migrate(): %v", err)
		}
		// Every case expects an empty store. One database is reused across
		// them, so truncate rather than paying for a schema per case.
		for _, table := range []string{"jobs", "attempts", "epochs"} {
			if _, err := db.ExecContext(context.Background(), "TRUNCATE TABLE "+table); err != nil {
				t.Fatalf("TRUNCATE %s: %v", table, err)
			}
		}
		t.Cleanup(func() { st.Close() })
		return st
	})
}

package sqlite_test

import (
	"context"
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

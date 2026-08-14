package memory_test

import (
	"testing"

	"github.com/gnikyt/cq-dashboard/store"
	"github.com/gnikyt/cq-dashboard/store/memory"
	"github.com/gnikyt/cq-dashboard/store/storetest"
)

// The reference implementation is held to the same contract as any driver.
// If this and the SQLite suite ever disagree, the contract is underspecified.
func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T) store.Store {
		return memory.Open()
	})
}

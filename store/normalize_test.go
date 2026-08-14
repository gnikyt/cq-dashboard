package store

import "testing"

func TestNormalizeName(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"import-rows", "import-rows"},
		{"import-rows-8471", "import-rows-*"},
		{"import_rows_8471", "import_rows_*"},
		{"sync:9f8a1c2d3e4b:shard-03", "sync:*:shard-*"},
		{"charge-card", "charge-card"},
		{"job.42.retry", "job.*.retry"},
		{"", ""},
		{"order-a1b2c3d4e5f6", "order-*"},
	} {
		if got := NormalizeName(tc.in); got != tc.want {
			t.Errorf("NormalizeName(%q): got %q, want %q", tc.in, got, tc.want)
		}
	}
}

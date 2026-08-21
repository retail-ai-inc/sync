package domain

import "testing"

func TestClampSyncedToday(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   int64
		want int64
	}{
		{"positive", 75, 75},
		{"zero", 0, 0},
		{"negative", -12, 0},
		{"large negative", -1 << 40, 0},
		{"one", 1, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClampSyncedToday(tt.in); got != tt.want {
				t.Errorf("ClampSyncedToday(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestClampingHidesDeletions records that the daily figure is a difference
// between the highest and lowest row count seen, so a table that lost rows
// produces a negative delta. Clamping it to zero reports "nothing synced today"
// for a table that in fact shrank, and the deletion leaves no trace in the API.
func TestClampingHidesDeletions(t *testing.T) {
	if got := ClampSyncedToday(-500); got != 0 {
		t.Fatalf("ClampSyncedToday(-500) = %d; deletions now surface somewhere, "+
			"so assert how they are reported instead", got)
	}
}

func TestTableStatCarriesItsFields(t *testing.T) {
	s := TableStat{TableName: "orders", SyncedToday: 75, TotalRows: 175, LastSyncTime: "2026-08-21 02:00:00"}

	if s.TableName != "orders" || s.SyncedToday != 75 || s.TotalRows != 175 ||
		s.LastSyncTime != "2026-08-21 02:00:00" {
		t.Errorf("TableStat = %+v", s)
	}
}

func TestLowerIsCaseFolding(t *testing.T) {
	for in, want := range map[string]string{
		"MongoDB": "mongodb",
		"MYSQL":   "mysql",
		"redis":   "redis",
		"":        "",
	} {
		if got := lower(in); got != want {
			t.Errorf("lower(%q) = %q, want %q", in, got, want)
		}
	}
}

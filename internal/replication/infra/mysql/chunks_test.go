package mysql

import (
	"testing"
)

// TestTheKeyColumnIsFoundInTheColumnList keeps the chunk's last key from being
// read out of the wrong column, which would make the re-copy walk the table in
// the wrong order and skip rows.
func TestTheKeyColumnIsFoundInTheColumnList(t *testing.T) {
	columns := []string{"created_at", "id", "customer"}
	if got := indexOf(columns, "id"); got != 1 {
		t.Errorf("indexOf(id) = %d, want 1", got)
	}
	if got := indexOf(columns, "ID"); got != 1 {
		t.Errorf("indexOf is case sensitive; MySQL column names are not")
	}
}

// TestAnUnknownKeyColumnFallsBackToTheFirst is a deliberate choice rather than a
// silent one: a key that is not in the list is a bug, and walking from the first
// column at least terminates.
func TestAnUnknownKeyColumnFallsBackToTheFirst(t *testing.T) {
	if got := indexOf([]string{"a", "b"}, "missing"); got != 0 {
		t.Errorf("indexOf(missing) = %d, want 0", got)
	}
}

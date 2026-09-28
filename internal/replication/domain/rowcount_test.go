package domain

import "testing"

// A side that could not be counted must never look like agreement, and must
// never look like zero: a table that is missing and one that is empty are
// different problems, and this is read while deciding whether a region is safe
// to promote.

func TestAPairThatMatchesAgrees(t *testing.T) {
	if !(ObjectCount{SourceRows: 638193, TargetRows: 638193}).Agrees() {
		t.Error("equal counts did not agree")
	}
	if (ObjectCount{SourceRows: 638193, TargetRows: 638192}).Agrees() {
		t.Error("counts one apart agreed")
	}
}

func TestAnEmptyPairOnBothSidesAgrees(t *testing.T) {
	if !(ObjectCount{}).Agrees() {
		t.Error("a table that is empty on both sides did not agree")
	}
}

// TestAnUncountedSideNeverAgrees is the important one. -1 means the side could
// not be read; treating that as zero would make an unreachable target agree
// with an empty source, and a missing table report as replicated.
func TestAnUncountedSideNeverAgrees(t *testing.T) {
	for name, o := range map[string]ObjectCount{
		"source unknown":                   {SourceRows: -1, TargetRows: 0},
		"target unknown":                   {SourceRows: 0, TargetRows: -1},
		"both unknown":                     {SourceRows: -1, TargetRows: -1},
		"source unknown, target populated": {SourceRows: -1, TargetRows: 100},
	} {
		t.Run(name, func(t *testing.T) {
			if o.Agrees() {
				t.Error("a pair with an uncounted side agreed")
			}
			if got := o.Difference(); got != 0 {
				t.Errorf("Difference() = %d for an uncounted side; the distance is "+
					"not known, and Agrees is what says so", got)
			}
		})
	}
}

func TestTheDifferenceHasNoSign(t *testing.T) {
	if got := (ObjectCount{SourceRows: 100, TargetRows: 60}).Difference(); got != 40 {
		t.Errorf("Difference() = %d, want 40", got)
	}
	if got := (ObjectCount{SourceRows: 60, TargetRows: 100}).Difference(); got != 40 {
		t.Errorf("Difference() = %d, want 40 when the target is ahead", got)
	}
}

func TestTheReportCountsOnlyTheObjectsThatDisagree(t *testing.T) {
	counts := RowCounts{Objects: []ObjectCount{
		{Source: "orders", SourceRows: 100, TargetRows: 100},
		{Source: "payments", SourceRows: 100, TargetRows: 90},
		{Source: "refunds", SourceRows: -1, TargetRows: 0},
		{Source: "audit", SourceRows: 5, TargetRows: 5},
	}}

	if got := counts.Difference(); got != 2 {
		t.Errorf("Difference() = %d, want 2 -- the short table and the uncounted one", got)
	}
}

func TestAReportOfNothingHasNoDifferences(t *testing.T) {
	if got := (RowCounts{}).Difference(); got != 0 {
		t.Errorf("Difference() = %d on an empty report", got)
	}
}

package domain

// The row counts behind the "objects captured" figure, read on demand.
//
// A count of both sides of every replicated object is expensive: on the sharded
// MongoDB source here, counting 116 collections on both sides took about five
// minutes, because a sharded countDocuments has to ask every shard and cannot
// use the collection metadata -- that metadata is an estimate and is stale.
// That cost cannot sit on a monitoring interval, so this is asked for rather
// than collected: somebody opens the figure and waits for the answer.

// RowCounts is one task's comparison.
type RowCounts struct {
	Engine string
	// Discovered is true when the task named no objects and they were found on
	// the source, which is the rule replication itself follows.
	Discovered bool
	Objects    []ObjectCount
}

// Difference reports how many objects disagree.
func (r RowCounts) Difference() int {
	n := 0
	for _, object := range r.Objects {
		if !object.Agrees() {
			n++
		}
	}
	return n
}

// ObjectCount is one table, collection or database.
type ObjectCount struct {
	// Source and Target name the object on each side, which differ when a
	// mapping renames.
	Source string
	Target string
	// SourceRows and TargetRows are -1 when that side could not be counted,
	// which is not the same as zero: a table that is missing and one that is
	// empty are different problems.
	SourceRows int64
	TargetRows int64
	// Note carries why a side could not be counted.
	Note string
}

// Agrees reports whether the two sides hold the same number. A side that could
// not be counted never agrees -- an unknown is not a match.
func (o ObjectCount) Agrees() bool {
	if o.SourceRows < 0 || o.TargetRows < 0 {
		return false
	}
	return o.SourceRows == o.TargetRows
}

// Difference is how far apart the two sides are, without a sign. It is zero for
// a pair that could not be counted, which Agrees distinguishes.
func (o ObjectCount) Difference() int64 {
	if o.SourceRows < 0 || o.TargetRows < 0 {
		return 0
	}
	if d := o.SourceRows - o.TargetRows; d >= 0 {
		return d
	}
	return o.TargetRows - o.SourceRows
}

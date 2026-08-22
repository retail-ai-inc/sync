// Package verify compares a replicated table against its source.
//
// Replication reports that it applied what it read. It cannot report what it
// never read: an event dropped before the offset was written, a row written
// while a subscription was reconnecting, a manual change made on the target by
// somebody debugging. None of those show up as an error anywhere, and for a
// disaster-recovery copy of a payment ledger "probably identical" is not a
// statement anyone can act on.
//
// The comparison walks both sides in primary-key order and hashes each row, so
// it reports three distinct things: rows the target is missing, rows it holds
// that the source does not, and rows that exist on both sides with different
// contents. Repair then makes each of them right, because knowing a row is
// wrong and having to fix it by hand is most of the work.
package verify

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// DefaultChunkSize is how many rows are read from each side at a time. It is a
// compromise: larger chunks mean fewer round trips, smaller ones mean less held
// in memory while comparing a table that does not fit in it.
const DefaultChunkSize = 1000

// DefaultReportLimit caps how many differing keys are named. A comparison that
// finds a million differences has told the operator what they need to know by
// the hundredth; carrying the rest would turn a report into a data dump.
const DefaultReportLimit = 100

// Row is one row as the comparison sees it: what identifies it, and a hash of
// everything else.
type Row struct {
	// Key identifies the row. Both sides must render it the same way, because
	// the comparison walks them in this order.
	Key string
	// Digest is a hash of the row's contents.
	Digest string
}

// Side is one end of a comparison.
type Side interface {
	// Rows reports up to limit rows whose key sorts after the given one, in key
	// order. An empty after means start at the beginning.
	Rows(ctx context.Context, after string, limit int) ([]Row, error)
	// Name describes the side for a report.
	Name() string
}

// Difference is what the comparison found about one row.
type Difference struct {
	Key string
	// Kind is what is wrong with it.
	Kind Kind
}

// Kind names the three ways two sides can disagree about a row.
type Kind string

const (
	// Missing: the source has the row and the target does not. This is the one
	// that means data loss.
	Missing Kind = "missing"
	// Extra: the target has a row the source does not. Either a delete was lost
	// or somebody wrote to the replica.
	Extra Kind = "extra"
	// Differing: both sides have the row and its contents disagree.
	Differing Kind = "differing"
)

// Result is what one comparison found.
type Result struct {
	// SourceRows and TargetRows are how many rows each side held.
	SourceRows int64
	TargetRows int64
	// Missing, Extra and Differing count every difference found, even the ones
	// past the report limit.
	Missing   int64
	Extra     int64
	Differing int64
	// Sample names up to the report limit of differences, so an operator has
	// something to look at.
	Sample []Difference
	// Truncated says the sample is not the whole story.
	Truncated bool
	// Repaired and RepairFailed count what a repair pass managed. They are zero
	// when the comparison was only asked to look.
	Repaired     int64
	RepairFailed int64
}

// Identical reports whether the two sides agree about every row.
func (r Result) Identical() bool {
	return r.Missing == 0 && r.Extra == 0 && r.Differing == 0
}

// Total counts every difference.
func (r Result) Total() int64 { return r.Missing + r.Extra + r.Differing }

// Summary describes the result in one line, for a log or an alert.
func (r Result) Summary() string {
	if r.Identical() {
		return fmt.Sprintf("identical: %d rows on both sides", r.SourceRows)
	}
	return fmt.Sprintf("%d differences across %d source and %d target rows "+
		"(%d missing, %d extra, %d differing)",
		r.Total(), r.SourceRows, r.TargetRows, r.Missing, r.Extra, r.Differing)
}

// Compare walks both sides and reports what they disagree about.
//
// Both sides are read in key order and merged, so the whole of neither has to
// be held in memory: at any moment it holds one chunk from each.
func Compare(ctx context.Context, source, target Side, chunkSize int) (Result, error) {
	return CompareAndRepair(ctx, source, target, chunkSize, nil)
}

// CompareAndRepair walks both sides and hands each difference to fix as it is
// found.
//
// Repairing from the reported sample instead would only ever fix the first
// hundred: a table a thousand rows apart needed ten passes to converge, and
// nothing said how far along it was. Fixing during the walk repairs everything
// in one pass, and the counts say what happened.
func CompareAndRepair(ctx context.Context, source, target Side, chunkSize int, fix func(Difference) error) (Result, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	var result Result
	var sourceChunk, targetChunk []Row
	var sourceAfter, targetAfter string
	sourceDone, targetDone := false, false

	// fill tops up a chunk when it has been consumed.
	fill := func(side Side, chunk *[]Row, after *string, done *bool) error {
		if len(*chunk) > 0 || *done {
			return nil
		}
		rows, err := side.Rows(ctx, *after, chunkSize)
		if err != nil {
			return fmt.Errorf("read %s: %w", side.Name(), err)
		}
		if len(rows) == 0 {
			*done = true
			return nil
		}
		*chunk = rows
		*after = rows[len(rows)-1].Key
		return nil
	}

	record := result.recorder(fix)

	for {
		if err := fill(source, &sourceChunk, &sourceAfter, &sourceDone); err != nil {
			return result, err
		}
		if err := fill(target, &targetChunk, &targetAfter, &targetDone); err != nil {
			return result, err
		}
		if len(sourceChunk) == 0 && len(targetChunk) == 0 {
			return result, nil
		}

		switch {
		case len(targetChunk) == 0:
			// Everything left on the source is missing from the target.
			result.SourceRows++
			record(sourceChunk[0].Key, Missing)
			sourceChunk = sourceChunk[1:]

		case len(sourceChunk) == 0:
			result.TargetRows++
			record(targetChunk[0].Key, Extra)
			targetChunk = targetChunk[1:]

		default:
			s, t := sourceChunk[0], targetChunk[0]
			switch {
			case s.Key == t.Key:
				result.SourceRows++
				result.TargetRows++
				if s.Digest != t.Digest {
					record(s.Key, Differing)
				}
				sourceChunk, targetChunk = sourceChunk[1:], targetChunk[1:]
			case s.Key < t.Key:
				result.SourceRows++
				record(s.Key, Missing)
				sourceChunk = sourceChunk[1:]
			default:
				result.TargetRows++
				record(t.Key, Extra)
				targetChunk = targetChunk[1:]
			}
		}
	}
}

// ------------------------------------------------------------------- SQL

// SQLSide reads one SQL table.
type SQLSide struct {
	DB *sql.DB
	// Schema may be empty, in which case the table is addressed unqualified.
	Schema string
	Table  string
	// Key is the column the comparison orders by. It has to identify a row on
	// its own, which for a replicated table is what the primary key does.
	Key string
	// Columns are the columns whose contents are hashed. The key is included, so
	// a row whose key was rewritten shows up as two differences rather than
	// none.
	Columns []string
}

func (s *SQLSide) Name() string {
	if s.Schema == "" {
		return s.Table
	}
	return s.Schema + "." + s.Table
}

func (s *SQLSide) Rows(ctx context.Context, after string, limit int) ([]Row, error) {
	columns := strings.Join(quoteAll(s.Columns), ", ")
	query := fmt.Sprintf("SELECT %s, %s FROM %s", quote(s.Key), columns, s.Name())
	args := []interface{}{}
	if after != "" {
		query += fmt.Sprintf(" WHERE %s > ?", quote(s.Key))
		args = append(args, after)
	}
	query += fmt.Sprintf(" ORDER BY %s LIMIT %d", quote(s.Key), limit)

	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		cells := make([]sql.NullString, len(s.Columns)+1)
		scan := make([]interface{}, len(cells))
		for i := range cells {
			scan[i] = &cells[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return nil, err
		}
		out = append(out, Row{Key: cells[0].String, Digest: Digest(cells[1:])})
	}
	return out, rows.Err()
}

// quote renders an identifier. Backticks are what MySQL uses and SQLite
// accepts, which is what the hermetic suite drives this against.
func quote(name string) string { return "`" + name + "`" }

func quoteAll(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = quote(name)
	}
	return out
}

// Digest hashes a row's cells.
//
// Each cell is written with its length in front, so two rows whose cells run
// together the same way — ("ab", "c") and ("a", "bc") — hash differently. A NULL
// is distinguished from an empty string for the same reason.
func Digest(cells []sql.NullString) string {
	h := sha256.New()
	for _, cell := range cells {
		if !cell.Valid {
			h.Write([]byte("n:"))
			continue
		}
		h.Write([]byte("v" + strconv.Itoa(len(cell.String)) + ":"))
		h.Write([]byte(cell.String))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// DigestValues hashes a row given as arbitrary values, which is what a document
// store hands back.
func DigestValues(values []interface{}) string {
	cells := make([]sql.NullString, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		cells[i] = sql.NullString{String: fmt.Sprint(v), Valid: true}
	}
	return Digest(cells)
}

// SQLColumns reports the columns of a table, in the order the server lists
// them, so both sides hash the same thing in the same order.
func SQLColumns(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns
		 WHERE table_schema = ? AND table_name = ?
		 ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("read the columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

// ---------------------------------------------------------------- repair

// Repairer fixes what a comparison found.
type Repairer interface {
	// Repair makes the named rows right and reports how many it fixed.
	Repair(ctx context.Context, differences []Difference) (int, error)
}

// SQLRepairer copies rows from a source table to a target table.
type SQLRepairer struct {
	Source *SQLSide
	Target *SQLSide
	// Upsert renders the statement that writes one row. It is supplied rather
	// than built here because the two flavours spell an upsert differently, and
	// the syncer already has a builder for its own dialect.
	Upsert func(schema, table string, columns []string) string
}

// Repair re-reads each named row from the source and writes it to the target,
// deleting the ones the source no longer has.
//
// It is deliberately row by row. A repair runs after something has already gone
// wrong, so being slow and obvious beats being fast and hard to reason about.
func (r *SQLRepairer) Repair(ctx context.Context, differences []Difference) (int, error) {
	fixed := 0
	for _, d := range differences {
		var err error
		switch d.Kind {
		case Extra:
			err = r.deleteRow(ctx, d.Key)
		case Missing, Differing:
			err = r.copyRow(ctx, d.Key)
		}
		if err != nil {
			return fixed, fmt.Errorf("repair %s %s: %w", d.Kind, d.Key, err)
		}
		fixed++
	}
	return fixed, nil
}

func (r *SQLRepairer) deleteRow(ctx context.Context, key string) error {
	_, err := r.Target.DB.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s = ?", r.Target.Name(), quote(r.Target.Key)), key)
	return err
}

func (r *SQLRepairer) copyRow(ctx context.Context, key string) error {
	columns := r.Source.Columns
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		strings.Join(quoteAll(columns), ", "), r.Source.Name(), quote(r.Source.Key))

	cells := make([]sql.NullString, len(columns))
	scan := make([]interface{}, len(cells))
	for i := range cells {
		scan[i] = &cells[i]
	}
	switch err := r.Source.DB.QueryRowContext(ctx, query, key).Scan(scan...); {
	case err == sql.ErrNoRows:
		// The source has lost the row since the comparison, so the target
		// should not have it either.
		return r.deleteRow(ctx, key)
	case err != nil:
		return err
	}

	values := make([]interface{}, len(cells))
	for i, cell := range cells {
		if cell.Valid {
			values[i] = cell.String
		}
	}
	_, err := r.Target.DB.ExecContext(ctx,
		r.Upsert(r.Target.Schema, r.Target.Table, columns), values...)
	return err
}

// SortDifferences orders differences by key, so two reports of the same problem
// read the same way.
func SortDifferences(differences []Difference) {
	sort.Slice(differences, func(i, j int) bool {
		if differences[i].Key != differences[j].Key {
			return differences[i].Key < differences[j].Key
		}
		return differences[i].Kind < differences[j].Kind
	})
}

// ----------------------------------------------------- ordering-free compare

// Cursor streams one side's rows in that side's own order, once through.
type Cursor interface {
	// Next reports up to limit more rows, or none when the side is exhausted.
	Next(ctx context.Context, limit int) ([]Row, error)
	Name() string
}

// Lookup finds specific rows by key.
type Lookup interface {
	Lookup(ctx context.Context, keys []string) (map[string]Row, error)
}

// End is one side of a comparison that does not assume both sides sort keys the
// same way.
type End interface {
	Cursor
	Lookup
}

// CompareByKey compares two sides without relying on them ordering keys
// identically.
//
// Compare merges two ordered streams, which is the cheaper approach and the
// right one for two SQL tables: both sort a primary key the same way. It is the
// wrong one for a document store, where the key can be an ObjectId, a string or
// a number, and the order the server sorts those in is not the order their
// rendered forms sort in. Getting that wrong would not merely miss differences,
// it would invent them — every row after the first disagreement reported as both
// missing and extra.
//
// This walks each side once and looks the keys up on the other, so only equality
// of keys matters.
func CompareByKey(ctx context.Context, source, target End, chunkSize int) (Result, error) {
	return CompareByKeyAndRepair(ctx, source, target, chunkSize, nil)
}

// CompareByKeyAndRepair is CompareByKey with a repair applied to each difference
// as it is found.
func CompareByKeyAndRepair(ctx context.Context, source, target End, chunkSize int, fix func(Difference) error) (Result, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	var result Result
	record := result.recorder(fix)

	// Walk the source: anything the target does not have is missing, anything it
	// has with a different digest is differing.
	for {
		batch, err := source.Next(ctx, chunkSize)
		if err != nil {
			return result, fmt.Errorf("read %s: %w", source.Name(), err)
		}
		if len(batch) == 0 {
			break
		}
		result.SourceRows += int64(len(batch))

		keys := make([]string, 0, len(batch))
		for _, row := range batch {
			keys = append(keys, row.Key)
		}
		found, err := target.Lookup(ctx, keys)
		if err != nil {
			return result, fmt.Errorf("look up in %s: %w", target.Name(), err)
		}
		for _, row := range batch {
			other, ok := found[row.Key]
			switch {
			case !ok:
				record(row.Key, Missing)
			case other.Digest != row.Digest:
				record(row.Key, Differing)
			}
		}
	}

	// Walk the target for rows the source does not have. The digests were
	// already compared above, so this pass only asks whether the key exists.
	for {
		batch, err := target.Next(ctx, chunkSize)
		if err != nil {
			return result, fmt.Errorf("read %s: %w", target.Name(), err)
		}
		if len(batch) == 0 {
			break
		}
		result.TargetRows += int64(len(batch))

		keys := make([]string, 0, len(batch))
		for _, row := range batch {
			keys = append(keys, row.Key)
		}
		found, err := source.Lookup(ctx, keys)
		if err != nil {
			return result, fmt.Errorf("look up in %s: %w", source.Name(), err)
		}
		for _, row := range batch {
			if _, ok := found[row.Key]; !ok {
				record(row.Key, Extra)
			}
		}
	}

	SortDifferences(result.Sample)
	return result, nil
}

// recorder returns the function a comparison calls for each difference it finds.
//
// It counts every difference, keeps the first DefaultReportLimit of them for the
// report, and — when a repair was asked for — fixes each one as it is found
// rather than from the capped sample afterwards.
func (r *Result) recorder(fix func(Difference) error) func(string, Kind) {
	return func(key string, kind Kind) {
		switch kind {
		case Missing:
			r.Missing++
		case Extra:
			r.Extra++
		case Differing:
			r.Differing++
		}

		difference := Difference{Key: key, Kind: kind}
		if len(r.Sample) < DefaultReportLimit {
			r.Sample = append(r.Sample, difference)
		} else {
			r.Truncated = true
		}

		if fix == nil {
			return
		}
		// A repair that fails is counted and the walk carries on: stopping at
		// the first failure would leave the rest of the table wrong for the sake
		// of one row.
		if err := fix(difference); err != nil {
			r.RepairFailed++
			return
		}
		r.Repaired++
	}
}

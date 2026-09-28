// Package verify compares a replicated table against its source, because
// replication only reports that it applied what it read.
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

// DefaultChunkSize is how many rows are read from each side at a time: larger
// means fewer round trips, smaller means less held in memory.
const DefaultChunkSize = 1000

// DefaultReportLimit caps how many differing keys are named. The hundredth has
// told the operator what they need; the rest would be a data dump.
const DefaultReportLimit = 100

// Row is one row as the comparison sees it: what identifies it, and a hash of
// everything else.
type Row struct {
	// Key is only ever tested for equality, never ordered, so any one-to-one
	// rendering will do — which is what lets a composite primary key be used.
	Key string
	// Digest is a hash of the row's contents.
	Digest string
}

type Difference struct {
	Key string
	// Kind is what is wrong with it.
	Kind Kind
}

type Kind string

const (
	// Missing: the source has the row and the target does not. This is the one
	// that means data loss.
	Missing Kind = "missing"
	// Extra: the target has a row the source does not — a lost delete, or somebody
	// wrote to the replica.
	Extra Kind = "extra"
	// Differing: both sides have the row and its contents disagree.
	Differing Kind = "differing"
)

type Result struct {
	// SourceRows and TargetRows are how many rows each side held.
	SourceRows int64
	TargetRows int64
	// Missing, Extra and Differing count every difference found, including those
	// past the report limit.
	Missing   int64
	Extra     int64
	Differing int64
	// Sample names up to the report limit of differences, so an operator has
	// something to look at.
	Sample []Difference
	// Truncated says the sample is not the whole story.
	Truncated bool
	// Repaired and RepairFailed count what a repair pass managed; zero when the
	// comparison was only asked to look.
	Repaired     int64
	RepairFailed int64
}

func (r Result) Identical() bool {
	return r.Missing == 0 && r.Extra == 0 && r.Differing == 0
}

func (r Result) Total() int64 { return r.Missing + r.Extra + r.Differing }

func (r Result) Summary() string {
	if r.Identical() {
		return fmt.Sprintf("identical: %d rows on both sides", r.SourceRows)
	}
	return fmt.Sprintf("%d differences across %d source and %d target rows "+
		"(%d missing, %d extra, %d differing)",
		r.Total(), r.SourceRows, r.TargetRows, r.Missing, r.Extra, r.Differing)
}

// recorder returns the function a comparison calls per difference: it counts
// every one, keeps the first DefaultReportLimit, and repairs as it goes rather
// than from the capped sample afterwards.
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
		// A repair that fails is counted and the walk carries on: stopping at the
		// first would leave the rest of the table wrong for one row.
		if err := fix(difference); err != nil {
			r.RepairFailed++
			return
		}
		r.Repaired++
	}
}

type Cursor interface {
	// Next reports up to limit more rows, or none when the side is exhausted.
	Next(ctx context.Context, limit int) ([]Row, error)
	Name() string
}

type Lookup interface {
	Lookup(ctx context.Context, keys []string) (map[string]Row, error)
}

type End interface {
	Cursor
	Lookup
}

func Compare(ctx context.Context, source, target End, chunkSize int) (Result, error) {
	return CompareAndRepair(ctx, source, target, chunkSize, nil)
}

// CompareAndRepair hands each difference to fix as it is found. Repairing from
// the sample instead only ever fixed the first hundred, so a table a thousand
// rows apart needed ten passes to converge.
func CompareAndRepair(ctx context.Context, source, target End, chunkSize int, fix func(Difference) error) (Result, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	var result Result
	record := result.recorder(fix)

	// Walk the source: what the target lacks is missing, what it has with a
	// different digest is differing.
	for {
		batch, err := source.Next(ctx, chunkSize)
		if err != nil {
			return result, fmt.Errorf("read %s: %w", source.Name(), err)
		}
		if len(batch) == 0 {
			break
		}
		result.SourceRows += int64(len(batch))

		found, err := target.Lookup(ctx, keysOf(batch))
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

	// Walk the target for rows the source does not have; the digests were compared
	// above, so this only asks whether the key exists.
	for {
		batch, err := target.Next(ctx, chunkSize)
		if err != nil {
			return result, fmt.Errorf("read %s: %w", target.Name(), err)
		}
		if len(batch) == 0 {
			break
		}
		result.TargetRows += int64(len(batch))

		found, err := source.Lookup(ctx, keysOf(batch))
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

func keysOf(batch []Row) []string {
	keys := make([]string, 0, len(batch))
	for _, row := range batch {
		keys = append(keys, row.Key)
	}
	return keys
}

// encodeKey renders key columns as one string, each part carrying its length so
// ("ab","c") and ("a","bc") cannot collide and a NULL differs from an empty
// string.
func encodeKey(values []sql.NullString) string {
	parts := make([]string, len(values))
	for i, v := range values {
		if !v.Valid {
			parts[i] = "n"
			continue
		}
		parts[i] = "v" + strconv.Itoa(len(v.String)) + ":" + v.String
	}
	return strings.Join(parts, "|")
}

// decodeKey reverses encodeKey, so a repair can address the row the comparison
// named.
func decodeKey(key string) ([]sql.NullString, error) {
	malformed := func() error { return fmt.Errorf("read the comparison key %q", key) }

	var values []sql.NullString
	rest := key
	for {
		switch {
		case rest == "":
			return values, nil
		case rest[0] == 'n':
			values = append(values, sql.NullString{})
			rest = rest[1:]
		case rest[0] == 'v':
			colon := strings.IndexByte(rest, ':')
			if colon < 0 {
				return nil, malformed()
			}
			length, err := strconv.Atoi(rest[1:colon])
			if err != nil || length < 0 || colon+1+length > len(rest) {
				return nil, malformed()
			}
			values = append(values, sql.NullString{String: rest[colon+1 : colon+1+length], Valid: true})
			rest = rest[colon+1+length:]
		default:
			return nil, malformed()
		}

		switch {
		case rest == "":
			return values, nil
		case rest[0] == '|':
			// A separator has to be followed by another part: a trailing one would
			// decode as if absent, and a key one part short would be looked up as valid.
			if rest = rest[1:]; rest == "" {
				return nil, malformed()
			}
		default:
			return nil, malformed()
		}
	}
}

// DescribeKey renders a comparison key for a person to read. The encoded forms
// round-trip rather than read — a composite key carries its lengths, an _id is
// hex BSON — and an alert nobody can act on is most of the way to no alert.
func DescribeKey(key string) string {
	if values, err := decodeKey(key); err == nil {
		parts := make([]string, 0, len(values))
		for _, v := range values {
			if !v.Valid {
				parts = append(parts, "NULL")
				continue
			}
			parts = append(parts, v.String)
		}
		return strings.Join(parts, "/")
	}
	if id, err := idFromKey(key); err == nil {
		return describeID(id)
	}
	if len(key) > 24 {
		return key[:24] + "…"
	}
	return key
}

// Digest hashes a row's cells, each with its length in front so ("ab","c") and
// ("a","bc") differ, and a NULL differs from an empty string.
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

// SQLColumns reports a table's columns in the server's own order, so both sides
// hash the same thing in the same order.
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

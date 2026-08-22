package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ordersEnd creates one side of a comparison over a table keyed by a single
// integer column, which is the shape a payment table most often has.
func ordersEnd(t *testing.T, name string, rows ...[2]string) *SQLEnd {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), name+".db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, amount TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO orders (id, amount) VALUES (?, ?)`, r[0], r[1]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return &SQLEnd{DB: db, Table: "orders", Keys: []string{"id"}, Columns: []string{"id", "amount"}}
}

// ledgerEnd creates one side over a table keyed by a pair, which is how a
// double-entry ledger is normally keyed.
func ledgerEnd(t *testing.T, name string, rows ...[3]string) *SQLEnd {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), name+".db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE ledger (
		account TEXT, entry INTEGER, amount TEXT, PRIMARY KEY (account, entry))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO ledger (account, entry, amount) VALUES (?, ?, ?)`, r[0], r[1], r[2]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return &SQLEnd{
		DB: db, Table: "ledger",
		Keys:    []string{"account", "entry"},
		Columns: []string{"account", "entry", "amount"},
	}
}

// memoryEnd is a side whose rows a test dictates, in an order that deliberately
// does not match the other side's.
type memoryEnd struct {
	name   string
	rows   []Row
	cursor int
	err    error
}

func (e *memoryEnd) Name() string { return e.name }

func (e *memoryEnd) Next(_ context.Context, limit int) ([]Row, error) {
	if e.err != nil {
		return nil, e.err
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	end := e.cursor + limit
	if end > len(e.rows) {
		end = len(e.rows)
	}
	batch := e.rows[e.cursor:end]
	e.cursor = end
	return batch, nil
}

func (e *memoryEnd) Lookup(_ context.Context, keys []string) (map[string]Row, error) {
	if e.err != nil {
		return nil, e.err
	}
	wanted := map[string]bool{}
	for _, k := range keys {
		wanted[k] = true
	}
	found := map[string]Row{}
	for _, row := range e.rows {
		if wanted[row.Key] {
			found[row.Key] = row
		}
	}
	return found, nil
}

func rowsOf(pairs ...[2]string) []Row {
	out := make([]Row, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, Row{Key: p[0], Digest: p[1]})
	}
	return out
}

func compare(t *testing.T, source, target End, chunk int) Result {
	t.Helper()

	result, err := Compare(context.Background(), source, target, chunk)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	return result
}

// keyOf renders a single-column key the way the comparison does.
func keyOf(id string) string {
	return encodeKey([]sql.NullString{{String: id, Valid: true}})
}

// ------------------------------------------------------------- comparison

func TestTwoIdenticalTablesAgree(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"}, [2]string{"2", "200"})
	target := ordersEnd(t, "target", [2]string{"1", "100"}, [2]string{"2", "200"})

	got := compare(t, source, target, 0)

	if !got.Identical() {
		t.Errorf("result = %s", got.Summary())
	}
	if got.SourceRows != 2 || got.TargetRows != 2 {
		t.Errorf("counted %d source and %d target rows", got.SourceRows, got.TargetRows)
	}
	if !strings.Contains(got.Summary(), "identical") {
		t.Errorf("summary = %q", got.Summary())
	}
}

// TestAnIntegerKeyIsNotComparedAsText is the defect this comparison was rebuilt
// to remove. Merging two ordered streams needs both sides to order keys the way
// the comparison does; a database orders an integer column numerically while Go
// compares the decimal strings, so 10 sorts before 9 and every row after the
// first disagreement would be reported as both missing and extra. Twelve rows
// cross that boundary.
func TestAnIntegerKeyIsNotComparedAsText(t *testing.T) {
	var rows [][2]string
	for i := 1; i <= 12; i++ {
		rows = append(rows, [2]string{fmt.Sprint(i), "same"})
	}
	source := ordersEnd(t, "source", rows...)
	target := ordersEnd(t, "target", rows...)

	got := compare(t, source, target, 3)

	if !got.Identical() {
		t.Errorf("result = %s; the two sides hold the same twelve rows", got.Summary())
	}
	if got.SourceRows != 12 || got.TargetRows != 12 {
		t.Errorf("counted %d and %d rows, want twelve each", got.SourceRows, got.TargetRows)
	}
}

// TestARowTheTargetNeverGotIsReported is the difference that means data loss,
// which is the whole reason for comparing.
func TestARowTheTargetNeverGotIsReported(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"}, [2]string{"2", "200"})
	target := ordersEnd(t, "target", [2]string{"1", "100"})

	got := compare(t, source, target, 0)

	if got.Missing != 1 || got.Extra != 0 || got.Differing != 0 {
		t.Fatalf("result = %s", got.Summary())
	}
	if len(got.Sample) != 1 || got.Sample[0].Kind != Missing {
		t.Errorf("sample = %+v", got.Sample)
	}
	if got.Sample[0].Key != keyOf("2") {
		t.Errorf("the missing row is %q", got.Sample[0].Key)
	}
}

// TestARowOnlyTheTargetHasIsReported catches a lost delete, and somebody writing
// to the replica by hand.
func TestARowOnlyTheTargetHasIsReported(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"})
	target := ordersEnd(t, "target", [2]string{"1", "100"}, [2]string{"2", "200"})

	got := compare(t, source, target, 0)

	if got.Extra != 1 || got.Missing != 0 {
		t.Fatalf("result = %s", got.Summary())
	}
	if got.Sample[0].Kind != Extra {
		t.Errorf("sample = %+v", got.Sample)
	}
}

// TestARowWithDifferentContentsIsReported is the one a row count cannot find,
// and the reason the comparison hashes rather than counts.
func TestARowWithDifferentContentsIsReported(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"})
	target := ordersEnd(t, "target", [2]string{"1", "999"})

	got := compare(t, source, target, 0)

	if got.Differing != 1 || got.Missing != 0 || got.Extra != 0 {
		t.Fatalf("result = %s", got.Summary())
	}
	if got.SourceRows != 1 || got.TargetRows != 1 {
		t.Errorf("counted %d and %d rows; a differing row exists on both sides",
			got.SourceRows, got.TargetRows)
	}
}

func TestTwoEmptyTablesAgree(t *testing.T) {
	got := compare(t, ordersEnd(t, "source"), ordersEnd(t, "target"), 0)

	if !got.Identical() || got.SourceRows != 0 {
		t.Errorf("result = %s", got.Summary())
	}
}

func TestAnEmptyTargetIsAllMissing(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "a"}, [2]string{"2", "b"}, [2]string{"3", "c"})

	got := compare(t, source, ordersEnd(t, "target"), 0)

	if got.Missing != 3 {
		t.Errorf("result = %s", got.Summary())
	}
}

// TestTheComparisonPagesThroughBothSides pins that neither side is held whole in
// memory, which is what makes the comparison usable on a table that does not fit
// in it.
func TestTheComparisonPagesThroughBothSides(t *testing.T) {
	var rows [][2]string
	for i := 1; i <= 10; i++ {
		rows = append(rows, [2]string{fmt.Sprint(i), "same"})
	}
	source := ordersEnd(t, "source", rows...)
	target := ordersEnd(t, "target", rows...)

	got := compare(t, source, target, 3)

	if !got.Identical() || got.SourceRows != 10 {
		t.Fatalf("result = %s", got.Summary())
	}
}

// TestTheStreamOrderDoesNotMatter is what lets a document store be compared at
// all: its _id may be an ObjectId, a string or a number, and the server's order
// over those is not the order their rendered forms take.
func TestTheStreamOrderDoesNotMatter(t *testing.T) {
	source := &memoryEnd{name: "source", rows: rowsOf(
		[2]string{"zz", "a"}, [2]string{"aa", "b"}, [2]string{"mm", "c"})}
	target := &memoryEnd{name: "target", rows: rowsOf(
		[2]string{"mm", "c"}, [2]string{"zz", "a"}, [2]string{"aa", "b"})}

	got := compare(t, source, target, 2)

	if !got.Identical() {
		t.Errorf("result = %s; the two sides hold the same rows in a different order",
			got.Summary())
	}
}

func TestEachKindIsFound(t *testing.T) {
	source := &memoryEnd{name: "source", rows: rowsOf(
		[2]string{"same", "a"}, [2]string{"changed", "a"}, [2]string{"lost", "a"})}
	target := &memoryEnd{name: "target", rows: rowsOf(
		[2]string{"same", "a"}, [2]string{"changed", "b"}, [2]string{"unexpected", "a"})}

	got := compare(t, source, target, 10)

	if got.Missing != 1 || got.Differing != 1 || got.Extra != 1 {
		t.Fatalf("result = %s", got.Summary())
	}
	kinds := map[string]Kind{}
	for _, d := range got.Sample {
		kinds[d.Key] = d.Kind
	}
	if kinds["lost"] != Missing || kinds["changed"] != Differing || kinds["unexpected"] != Extra {
		t.Errorf("sample = %+v", got.Sample)
	}
}

// TestTheSampleIsCappedButTheCountIsNot means a badly diverged table reports
// something an operator can read rather than a million lines.
func TestTheSampleIsCappedButTheCountIsNot(t *testing.T) {
	pairs := make([][2]string, 0, DefaultReportLimit+50)
	for i := 0; i < DefaultReportLimit+50; i++ {
		pairs = append(pairs, [2]string{fmt.Sprintf("%05d", i), "a"})
	}
	source := &memoryEnd{name: "source", rows: rowsOf(pairs...)}

	got := compare(t, source, &memoryEnd{name: "target"}, 0)

	if got.Missing != int64(DefaultReportLimit+50) {
		t.Errorf("counted %d missing rows, want %d", got.Missing, DefaultReportLimit+50)
	}
	if len(got.Sample) != DefaultReportLimit {
		t.Errorf("the sample holds %d differences, want %d", len(got.Sample), DefaultReportLimit)
	}
	if !got.Truncated {
		t.Error("the result does not say the sample was truncated")
	}
}

func TestAnUnreadableSideIsReported(t *testing.T) {
	source := &memoryEnd{name: "source", err: errors.New("connection refused")}

	_, err := Compare(context.Background(), source, &memoryEnd{name: "target"}, 0)
	if err == nil {
		t.Fatal("Compare succeeded against a side it could not read")
	}
	if !strings.Contains(err.Error(), "source") {
		t.Errorf("error = %v, want the side named", err)
	}
}

func TestAnUnreadableTargetIsReported(t *testing.T) {
	target := &memoryEnd{name: "target", err: errors.New("connection refused")}

	if _, err := Compare(context.Background(), &memoryEnd{name: "source"}, target, 0); err == nil {
		t.Fatal("Compare succeeded against a target it could not read")
	}
}

// ---------------------------------------------------------- composite keys

// TestACompositeKeyIsCompared is why the key is encoded rather than taken as a
// column value. A payment ledger's tables are commonly keyed by a pair, and
// refusing them left exactly the tables that matter most unverifiable.
func TestACompositeKeyIsCompared(t *testing.T) {
	source := ledgerEnd(t, "source",
		[3]string{"acct-1", "1", "100"}, [3]string{"acct-1", "2", "200"},
		[3]string{"acct-2", "1", "300"})
	target := ledgerEnd(t, "target",
		[3]string{"acct-1", "1", "100"}, [3]string{"acct-2", "1", "300"})

	got := compare(t, source, target, 0)

	if got.Missing != 1 || got.Extra != 0 || got.Differing != 0 {
		t.Fatalf("result = %s", got.Summary())
	}
	values, err := decodeKey(got.Sample[0].Key)
	if err != nil {
		t.Fatalf("decodeKey: %v", err)
	}
	if len(values) != 2 || values[0].String != "acct-1" || values[1].String != "2" {
		t.Errorf("the missing row is %v, want acct-1/2", values)
	}
}

// TestACompositeKeyPagesAcrossItsParts covers the paging, which is where a
// composite key is easiest to get wrong: the second page has to continue from
// the pair, not from its first column.
func TestACompositeKeyPagesAcrossItsParts(t *testing.T) {
	var rows [][3]string
	for entry := 1; entry <= 6; entry++ {
		rows = append(rows, [3]string{"acct-1", fmt.Sprint(entry), "100"})
	}
	source := ledgerEnd(t, "source", rows...)
	target := ledgerEnd(t, "target", rows...)

	got := compare(t, source, target, 2)

	if !got.Identical() || got.SourceRows != 6 {
		t.Errorf("result = %s, want six identical rows", got.Summary())
	}
}

// TestOnlyOneRowOfACompositeKeyDiffers checks the key is compared whole: two
// rows sharing their first column must not be conflated.
func TestOnlyOneRowOfACompositeKeyDiffers(t *testing.T) {
	source := ledgerEnd(t, "source",
		[3]string{"acct-1", "1", "100"}, [3]string{"acct-1", "2", "200"})
	target := ledgerEnd(t, "target",
		[3]string{"acct-1", "1", "100"}, [3]string{"acct-1", "2", "999"})

	got := compare(t, source, target, 0)

	if got.Differing != 1 || got.Missing != 0 || got.Extra != 0 {
		t.Errorf("result = %s", got.Summary())
	}
}

// TestACompositeKeyIsRepaired closes the loop for the tables that matter most.
func TestACompositeKeyIsRepaired(t *testing.T) {
	source := ledgerEnd(t, "source",
		[3]string{"acct-1", "1", "100"}, [3]string{"acct-1", "2", "200"})
	target := ledgerEnd(t, "target", [3]string{"acct-1", "1", "100"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	got, err := CompareAndRepair(context.Background(), source, target, 0,
		func(d Difference) error {
			_, err := r.Repair(context.Background(), []Difference{d})
			return err
		})
	if err != nil {
		t.Fatalf("CompareAndRepair: %v", err)
	}
	if got.Repaired != 1 {
		t.Fatalf("repaired %d of %d", got.Repaired, got.Total())
	}

	var amount string
	if err := target.DB.QueryRow(
		`SELECT amount FROM ledger WHERE account = ? AND entry = ?`, "acct-1", 2).Scan(&amount); err != nil {
		t.Fatalf("read the repaired row: %v", err)
	}
	if amount != "200" {
		t.Errorf("the repaired row holds %q", amount)
	}
}

// -------------------------------------------------------------------- keys

// TestTheKeySeparatesItsParts pins why each part carries its length: without it
// ("ab", "c") and ("a", "bc") encode the same, and two different ledger entries
// would compare as one.
func TestTheKeySeparatesItsParts(t *testing.T) {
	first := encodeKey([]sql.NullString{
		{String: "ab", Valid: true}, {String: "c", Valid: true}})
	second := encodeKey([]sql.NullString{
		{String: "a", Valid: true}, {String: "bc", Valid: true}})

	if first == second {
		t.Error("two keys whose parts run together the same way encode identically")
	}
}

func TestAKeyRoundTrips(t *testing.T) {
	for name, values := range map[string][]sql.NullString{
		"one part":         {{String: "1", Valid: true}},
		"two parts":        {{String: "acct-1", Valid: true}, {String: "2", Valid: true}},
		"a null part":      {{String: "a", Valid: true}, {}},
		"an empty part":    {{String: "", Valid: true}},
		"a pipe inside":    {{String: "a|b", Valid: true}, {String: "c", Valid: true}},
		"a colon inside":   {{String: "v3:xy", Valid: true}},
		"looks like a key": {{String: "v1:a|n", Valid: true}},
	} {
		t.Run(name, func(t *testing.T) {
			encoded := encodeKey(values)
			got, err := decodeKey(encoded)
			if err != nil {
				t.Fatalf("decodeKey(%q): %v", encoded, err)
			}
			if len(got) != len(values) {
				t.Fatalf("decoded %d parts, want %d", len(got), len(values))
			}
			for i := range values {
				if got[i] != values[i] {
					t.Errorf("part %d = %+v, want %+v", i, got[i], values[i])
				}
			}
		})
	}
}

// TestANullPartIsNotAnEmptyOne matters because the two mean different things in
// a ledger.
func TestANullPartIsNotAnEmptyOne(t *testing.T) {
	null := encodeKey([]sql.NullString{{}})
	empty := encodeKey([]sql.NullString{{String: "", Valid: true}})

	if null == empty {
		t.Error("a NULL key part and an empty one encode identically")
	}
}

func TestAMalformedKeyIsReported(t *testing.T) {
	for _, key := range []string{"x", "v", "v:", "vabc:x", "v9:ab", "v1:a-", "v1:a|"} {
		if _, err := decodeKey(key); err == nil {
			t.Errorf("decodeKey(%q) returned no error", key)
		}
	}
}

// ----------------------------------------------------------------- digests

// TestTheDigestSeparatesTheCells pins why each cell carries its length: without
// it ("ab", "c") and ("a", "bc") hash the same, and a row whose values shifted
// between two columns would compare as identical.
func TestTheDigestSeparatesTheCells(t *testing.T) {
	first := Digest([]sql.NullString{
		{String: "ab", Valid: true}, {String: "c", Valid: true}})
	second := Digest([]sql.NullString{
		{String: "a", Valid: true}, {String: "bc", Valid: true}})

	if first == second {
		t.Error("two rows whose cells run together the same way hash identically")
	}
}

// TestANullIsNotAnEmptyString matters because the two mean different things in a
// ledger, and replicating one as the other is a difference worth reporting.
func TestANullIsNotAnEmptyString(t *testing.T) {
	null := Digest([]sql.NullString{{}})
	empty := Digest([]sql.NullString{{String: "", Valid: true}})

	if null == empty {
		t.Error("NULL and the empty string hash identically")
	}
}

func TestTheDigestIsStable(t *testing.T) {
	cells := []sql.NullString{{String: "1", Valid: true}, {}, {String: "x", Valid: true}}

	first := Digest(cells)
	for i := 0; i < 5; i++ {
		if got := Digest(cells); got != first {
			t.Fatalf("call %d returned a different digest", i)
		}
	}
}

func TestValuesAndCellsAgree(t *testing.T) {
	fromCells := Digest([]sql.NullString{{String: "1", Valid: true}, {}})
	fromValues := DigestValues([]interface{}{"1", nil})

	if fromCells != fromValues {
		t.Error("the two digest helpers disagree about the same row")
	}
}

// ------------------------------------------------------------------ repair

// upsertFor renders the statement the repairer writes with. SQLite spells it
// this way; the syncer supplies its own for MySQL.
func upsertFor(schema, table string, columns []string) string {
	placeholders := make([]string, len(columns))
	quoted := make([]string, len(columns))
	for i, c := range columns {
		placeholders[i] = "?"
		quoted[i] = "`" + c + "`"
	}
	name := table
	if schema != "" {
		name = schema + "." + table
	}
	return fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		name, strings.Join(quoted, ", "), strings.Join(placeholders, ", "))
}

func amountOf(t *testing.T, end *SQLEnd, id string) (string, bool) {
	t.Helper()

	var amount sql.NullString
	err := end.DB.QueryRow(`SELECT amount FROM orders WHERE id = ?`, id).Scan(&amount)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("read %s: %v", id, err)
	}
	return amount.String, true
}

// TestRepairCopiesAMissingRow closes the loop: finding out a row is missing and
// having to put it back by hand is most of the work.
func TestRepairCopiesAMissingRow(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"}, [2]string{"2", "200"})
	target := ordersEnd(t, "target", [2]string{"1", "100"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	fixed, err := r.Repair(context.Background(), []Difference{{Key: keyOf("2"), Kind: Missing}})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if fixed != 1 {
		t.Errorf("fixed = %d, want 1", fixed)
	}

	if amount, ok := amountOf(t, target, "2"); !ok || amount != "200" {
		t.Errorf("the target holds %q/%v after the repair", amount, ok)
	}
}

func TestRepairOverwritesADifferingRow(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"})
	target := ordersEnd(t, "target", [2]string{"1", "999"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	if _, err := r.Repair(context.Background(), []Difference{{Key: keyOf("1"), Kind: Differing}}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	if amount, _ := amountOf(t, target, "1"); amount != "100" {
		t.Errorf("the target holds %q after the repair", amount)
	}
}

func TestRepairRemovesARowTheSourceDoesNotHave(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"})
	target := ordersEnd(t, "target", [2]string{"1", "100"}, [2]string{"2", "200"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	if _, err := r.Repair(context.Background(), []Difference{{Key: keyOf("2"), Kind: Extra}}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	if _, ok := amountOf(t, target, "2"); ok {
		t.Error("the extra row is still on the target")
	}
}

// TestARowThatVanishedFromTheSourceIsRemoved covers the race between the
// comparison and the repair: by the time the repair runs, the source may have
// deleted the row it was going to copy.
func TestARowThatVanishedFromTheSourceIsRemoved(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"})
	target := ordersEnd(t, "target", [2]string{"1", "100"}, [2]string{"2", "200"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	if _, err := r.Repair(context.Background(), []Difference{{Key: keyOf("2"), Kind: Missing}}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	if _, ok := amountOf(t, target, "2"); ok {
		t.Error("a row the source no longer has was left on the target")
	}
}

func TestARepairFailureIsReportedWithWhatItGotThrough(t *testing.T) {
	source := ordersEnd(t, "source", [2]string{"1", "100"})
	target := ordersEnd(t, "target")
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}
	_ = target.DB.Close()

	fixed, err := r.Repair(context.Background(), []Difference{{Key: keyOf("1"), Kind: Missing}})
	if err == nil {
		t.Fatal("Repair succeeded against a closed target")
	}
	if fixed != 0 {
		t.Errorf("fixed = %d against a closed target", fixed)
	}
}

// TestEveryDifferenceIsRepairedNotJustTheSampled is the other repair fix.
// Repairing from the reported sample only ever fixed the first hundred, so a
// table a thousand rows apart needed ten passes to converge.
func TestEveryDifferenceIsRepairedNotJustTheSampled(t *testing.T) {
	const differences = DefaultReportLimit + 50
	pairs := make([][2]string, 0, differences)
	for i := 0; i < differences; i++ {
		pairs = append(pairs, [2]string{fmt.Sprintf("%05d", i), "a"})
	}
	source := &memoryEnd{name: "source", rows: rowsOf(pairs...)}

	var fixed []string
	result, err := CompareAndRepair(context.Background(), source, &memoryEnd{name: "target"}, 0,
		func(d Difference) error {
			fixed = append(fixed, d.Key)
			return nil
		})
	if err != nil {
		t.Fatalf("CompareAndRepair: %v", err)
	}

	if len(fixed) != differences {
		t.Errorf("repaired %d of %d differences", len(fixed), differences)
	}
	if result.Repaired != int64(differences) {
		t.Errorf("Repaired = %d, want %d", result.Repaired, differences)
	}
	if len(result.Sample) != DefaultReportLimit || !result.Truncated {
		t.Errorf("sample holds %d, truncated=%v", len(result.Sample), result.Truncated)
	}
}

// TestARepairFailureDoesNotStopTheRest matters because stopping at the first
// failure would leave the rest of the table wrong for the sake of one row.
func TestARepairFailureDoesNotStopTheRest(t *testing.T) {
	source := &memoryEnd{name: "source", rows: rowsOf(
		[2]string{"1", "a"}, [2]string{"2", "a"}, [2]string{"3", "a"})}

	attempted := 0
	result, err := CompareAndRepair(context.Background(), source, &memoryEnd{name: "target"}, 0,
		func(d Difference) error {
			attempted++
			if d.Key == "2" {
				return errors.New("read only replica")
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CompareAndRepair: %v", err)
	}

	if attempted != 3 {
		t.Errorf("%d repairs were attempted, want all three", attempted)
	}
	if result.Repaired != 2 || result.RepairFailed != 1 {
		t.Errorf("repaired %d, failed %d", result.Repaired, result.RepairFailed)
	}
}

// TestComparingWithoutRepairingChangesNothing keeps the read-only case honest:
// the counts stay zero so a report cannot claim a repair that never ran.
func TestComparingWithoutRepairingChangesNothing(t *testing.T) {
	got := compare(t, ordersEnd(t, "source", [2]string{"1", "a"}), ordersEnd(t, "target"), 0)

	if got.Repaired != 0 || got.RepairFailed != 0 {
		t.Errorf("repaired %d, failed %d for a comparison that was only asked to look",
			got.Repaired, got.RepairFailed)
	}
}

// ------------------------------------------------------------------- shape

func TestTheEndNamesItself(t *testing.T) {
	if got := (&SQLEnd{Table: "orders"}).Name(); got != "orders" {
		t.Errorf("Name() = %q", got)
	}
	if got := (&SQLEnd{Schema: "shop", Table: "orders"}).Name(); got != "shop.orders" {
		t.Errorf("Name() = %q", got)
	}
}

func TestTheEndStreamsEveryRowOnce(t *testing.T) {
	end := ordersEnd(t, "source", [2]string{"1", "a"}, [2]string{"2", "b"}, [2]string{"3", "c"})

	seen := 0
	for {
		batch, err := end.Next(context.Background(), 2)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		seen += len(batch)
	}

	if seen != 3 {
		t.Errorf("streamed %d rows, want 3", seen)
	}
	if again, _ := end.Next(context.Background(), 2); len(again) != 0 {
		t.Error("an exhausted stream produced more rows")
	}
}

func TestAnEmptyLookupAsksNothing(t *testing.T) {
	found, err := ordersEnd(t, "target").Lookup(context.Background(), nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(found) != 0 {
		t.Errorf("found = %v", found)
	}
}

// TestALookupKeyOfTheWrongShapeIsReported covers a key from a table with a
// different number of key columns, which would otherwise produce a query the
// server rejects with a message naming nothing useful.
func TestALookupKeyOfTheWrongShapeIsReported(t *testing.T) {
	end := ledgerEnd(t, "target")

	_, err := end.Lookup(context.Background(), []string{keyOf("1")})
	if err == nil {
		t.Fatal("a one-part key was accepted for a two-column key")
	}
	if !strings.Contains(err.Error(), "keyed by 2") {
		t.Errorf("error = %v", err)
	}
}

func TestDifferencesSortByKey(t *testing.T) {
	differences := []Difference{
		{Key: "3", Kind: Missing}, {Key: "1", Kind: Extra}, {Key: "2", Kind: Differing},
	}

	SortDifferences(differences)

	if differences[0].Key != "1" || differences[1].Key != "2" || differences[2].Key != "3" {
		t.Errorf("order = %+v", differences)
	}
}

// --------------------------------------------------------- document digests

func TestTheFieldOrderDoesNotChangeTheDigest(t *testing.T) {
	first := canonical(bson.M{"a": 1, "b": 2})
	second := canonical(bson.M{"b": 2, "a": 1})

	if first != second {
		t.Errorf("the same document rendered two ways:\n  %s\n  %s", first, second)
	}
}

// TestATypeChangeIsADifference matters because the string "1" and the number 1
// are different documents, and a replica holding one where the source holds the
// other is a real divergence.
func TestATypeChangeIsADifference(t *testing.T) {
	if canonical(bson.M{"v": "1"}) == canonical(bson.M{"v": 1}) {
		t.Error(`the string "1" and the number 1 render identically`)
	}
	if canonical(bson.M{"v": nil}) == canonical(bson.M{"v": ""}) {
		t.Error("null and the empty string render identically")
	}
}

// TestNestedFieldsAreOrderedToo covers the documents a payment ledger actually
// holds, which are not flat.
func TestNestedFieldsAreOrderedToo(t *testing.T) {
	first := canonical(bson.M{"outer": bson.M{"a": 1, "b": 2}, "list": bson.A{1, 2}})
	second := canonical(bson.M{"list": bson.A{1, 2}, "outer": bson.M{"b": 2, "a": 1}})

	if first != second {
		t.Errorf("the same nested document rendered two ways:\n  %s\n  %s", first, second)
	}
}

// TestAReorderedArrayIsADifference is the other side of that: an array's order
// is part of its value, unlike a document's field order.
func TestAReorderedArrayIsADifference(t *testing.T) {
	if canonical(bson.A{1, 2}) == canonical(bson.A{2, 1}) {
		t.Error("two arrays with the same items in a different order render identically")
	}
}

func TestAnIdRoundTripsThroughItsKey(t *testing.T) {
	id := primitive.NewObjectID()
	raw, err := bson.Marshal(bson.M{"_id": id, "v": 1})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	row, err := rowFromDocument(raw)
	if err != nil {
		t.Fatalf("rowFromDocument: %v", err)
	}

	recovered, err := idFromKey(row.Key)
	if err != nil {
		t.Fatalf("idFromKey: %v", err)
	}
	value, ok := recovered.(bson.RawValue)
	if !ok {
		t.Fatalf("idFromKey returned %T", recovered)
	}
	if got, ok := value.ObjectIDOK(); !ok || got != id {
		t.Errorf("recovered %v, want %v", got, id)
	}
}

// TestTwoIdTypesDoNotShareAKey pins that the key captures the type: an ObjectId
// and the string of its hex are different documents.
func TestTwoIdTypesDoNotShareAKey(t *testing.T) {
	id := primitive.NewObjectID()

	fromID, err := bson.Marshal(bson.M{"_id": id})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	fromString, err := bson.Marshal(bson.M{"_id": id.Hex()})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	first, err := rowFromDocument(fromID)
	if err != nil {
		t.Fatalf("rowFromDocument: %v", err)
	}
	second, err := rowFromDocument(fromString)
	if err != nil {
		t.Fatalf("rowFromDocument: %v", err)
	}
	if first.Key == second.Key {
		t.Error("an ObjectId and its hex string produced the same comparison key")
	}
}

func TestADocumentWithNoIdIsReported(t *testing.T) {
	raw, err := bson.Marshal(bson.M{"v": 1})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	if _, err := rowFromDocument(raw); err == nil {
		t.Error("a document with no _id was accepted")
	}
}

func TestAnUnreadableIdKeyIsReported(t *testing.T) {
	for _, key := range []string{"not hex", "00ff"} {
		if _, err := idFromKey(key); err == nil {
			t.Errorf("idFromKey(%q) returned no error", key)
		}
	}
}

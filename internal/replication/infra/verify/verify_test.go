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

// table creates one side of a comparison and returns the reader for it.
func table(t *testing.T, name string, rows ...[2]string) *SQLSide {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), name+".db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE orders (id TEXT PRIMARY KEY, amount TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO orders (id, amount) VALUES (?, ?)`, r[0], r[1]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return &SQLSide{DB: db, Table: "orders", Key: "id", Columns: []string{"id", "amount"}}
}

// memorySide is a side whose rows a test dictates, so the merge can be
// exercised at boundaries a database makes awkward to set up.
type memorySide struct {
	name string
	rows []Row
	err  error
	// calls counts how many chunks were asked for, which is what makes the
	// paging assertion possible.
	calls int
}

func (s *memorySide) Name() string { return s.name }

func (s *memorySide) Rows(_ context.Context, after string, limit int) ([]Row, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.calls++

	var out []Row
	for _, r := range s.rows {
		if after != "" && r.Key <= after {
			continue
		}
		out = append(out, r)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func rowsOf(pairs ...[2]string) []Row {
	out := make([]Row, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, Row{Key: p[0], Digest: p[1]})
	}
	return out
}

func compare(t *testing.T, source, target Side, chunk int) Result {
	t.Helper()

	result, err := Compare(context.Background(), source, target, chunk)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	return result
}

// ------------------------------------------------------------- comparison

func TestTwoIdenticalTablesAgree(t *testing.T) {
	source := table(t, "source", [2]string{"1", "100"}, [2]string{"2", "200"})
	target := table(t, "target", [2]string{"1", "100"}, [2]string{"2", "200"})

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

// TestARowTheTargetNeverGotIsReported is the difference that means data loss,
// which is the whole reason for comparing.
func TestARowTheTargetNeverGotIsReported(t *testing.T) {
	source := table(t, "source", [2]string{"1", "100"}, [2]string{"2", "200"})
	target := table(t, "target", [2]string{"1", "100"})

	got := compare(t, source, target, 0)

	if got.Missing != 1 || got.Extra != 0 || got.Differing != 0 {
		t.Fatalf("result = %s", got.Summary())
	}
	if len(got.Sample) != 1 || got.Sample[0].Key != "2" || got.Sample[0].Kind != Missing {
		t.Errorf("sample = %+v", got.Sample)
	}
}

// TestARowOnlyTheTargetHasIsReported catches a lost delete, and somebody
// writing to the replica by hand.
func TestARowOnlyTheTargetHasIsReported(t *testing.T) {
	source := table(t, "source", [2]string{"1", "100"})
	target := table(t, "target", [2]string{"1", "100"}, [2]string{"2", "200"})

	got := compare(t, source, target, 0)

	if got.Extra != 1 || got.Missing != 0 {
		t.Fatalf("result = %s", got.Summary())
	}
	if got.Sample[0].Kind != Extra || got.Sample[0].Key != "2" {
		t.Errorf("sample = %+v", got.Sample)
	}
}

// TestARowWithDifferentContentsIsReported is the one a row count cannot find,
// and the reason the comparison hashes rather than counts.
func TestARowWithDifferentContentsIsReported(t *testing.T) {
	source := table(t, "source", [2]string{"1", "100"})
	target := table(t, "target", [2]string{"1", "999"})

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
	got := compare(t, table(t, "source"), table(t, "target"), 0)

	if !got.Identical() || got.SourceRows != 0 {
		t.Errorf("result = %s", got.Summary())
	}
}

func TestAnEmptyTargetIsAllMissing(t *testing.T) {
	source := table(t, "source", [2]string{"1", "a"}, [2]string{"2", "b"}, [2]string{"3", "c"})

	got := compare(t, source, table(t, "target"), 0)

	if got.Missing != 3 {
		t.Errorf("result = %s", got.Summary())
	}
}

// TestTheComparisonPagesThroughBothSides pins that neither side is held whole in
// memory, which is what makes the comparison usable on a table that does not
// fit in it.
func TestTheComparisonPagesThroughBothSides(t *testing.T) {
	pairs := make([][2]string, 0, 10)
	for i := 0; i < 10; i++ {
		pairs = append(pairs, [2]string{fmt.Sprintf("%02d", i), "same"})
	}
	source := &memorySide{name: "source", rows: rowsOf(pairs...)}
	target := &memorySide{name: "target", rows: rowsOf(pairs...)}

	got := compare(t, source, target, 3)

	if !got.Identical() || got.SourceRows != 10 {
		t.Fatalf("result = %s", got.Summary())
	}
	if source.calls < 4 {
		t.Errorf("the source was read %d times for 10 rows in chunks of 3", source.calls)
	}
}

// TestTheDifferencesAreFoundAcrossChunkBoundaries covers the merge at its
// awkward point: one side running ahead of the other by more than a chunk.
func TestTheDifferencesAreFoundAcrossChunkBoundaries(t *testing.T) {
	source := &memorySide{name: "source", rows: rowsOf(
		[2]string{"01", "a"}, [2]string{"02", "a"}, [2]string{"03", "a"},
		[2]string{"04", "a"}, [2]string{"05", "a"})}
	target := &memorySide{name: "target", rows: rowsOf(
		[2]string{"01", "a"}, [2]string{"05", "a"})}

	got := compare(t, source, target, 2)

	if got.Missing != 3 {
		t.Fatalf("result = %s", got.Summary())
	}
	keys := []string{}
	for _, d := range got.Sample {
		keys = append(keys, d.Key)
	}
	if strings.Join(keys, ",") != "02,03,04" {
		t.Errorf("missing keys = %v", keys)
	}
}

// TestTheSampleIsCappedButTheCountIsNot means a badly diverged table reports
// something an operator can read rather than a million lines.
func TestTheSampleIsCappedButTheCountIsNot(t *testing.T) {
	pairs := make([][2]string, 0, DefaultReportLimit+50)
	for i := 0; i < DefaultReportLimit+50; i++ {
		pairs = append(pairs, [2]string{fmt.Sprintf("%05d", i), "a"})
	}
	source := &memorySide{name: "source", rows: rowsOf(pairs...)}

	got := compare(t, source, &memorySide{name: "target"}, 0)

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
	source := &memorySide{name: "source", err: errors.New("connection refused")}

	_, err := Compare(context.Background(), source, &memorySide{name: "target"}, 0)
	if err == nil {
		t.Fatal("Compare succeeded against a side it could not read")
	}
	if !strings.Contains(err.Error(), "source") {
		t.Errorf("error = %v, want the side named", err)
	}
}

func TestAnUnreadableTargetIsReported(t *testing.T) {
	target := &memorySide{name: "target", err: errors.New("connection refused")}

	if _, err := Compare(context.Background(), &memorySide{name: "source"}, target, 0); err == nil {
		t.Fatal("Compare succeeded against a target it could not read")
	}
}

// ---------------------------------------------------------------- digests

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

// TestANullIsNotAnEmptyString matters because the two mean different things in
// a ledger, and replicating one as the other is a difference worth reporting.
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

// ----------------------------------------------------------------- repair

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

func amountOf(t *testing.T, side *SQLSide, key string) (string, bool) {
	t.Helper()

	var amount sql.NullString
	err := side.DB.QueryRow(`SELECT amount FROM orders WHERE id = ?`, key).Scan(&amount)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return amount.String, true
}

// TestRepairCopiesAMissingRow closes the loop: finding out a row is missing and
// having to put it back by hand is most of the work.
func TestRepairCopiesAMissingRow(t *testing.T) {
	source := table(t, "source", [2]string{"1", "100"}, [2]string{"2", "200"})
	target := table(t, "target", [2]string{"1", "100"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	fixed, err := r.Repair(context.Background(), []Difference{{Key: "2", Kind: Missing}})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if fixed != 1 {
		t.Errorf("fixed = %d, want 1", fixed)
	}

	if amount, ok := amountOf(t, target, "2"); !ok || amount != "200" {
		t.Errorf("the target holds %q/%v after the repair", amount, ok)
	}
	if got := compare(t, source, target, 0); !got.Identical() {
		t.Errorf("the sides still differ: %s", got.Summary())
	}
}

func TestRepairOverwritesADifferingRow(t *testing.T) {
	source := table(t, "source", [2]string{"1", "100"})
	target := table(t, "target", [2]string{"1", "999"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	if _, err := r.Repair(context.Background(), []Difference{{Key: "1", Kind: Differing}}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	if amount, _ := amountOf(t, target, "1"); amount != "100" {
		t.Errorf("the target holds %q after the repair", amount)
	}
}

func TestRepairRemovesARowTheSourceDoesNotHave(t *testing.T) {
	source := table(t, "source", [2]string{"1", "100"})
	target := table(t, "target", [2]string{"1", "100"}, [2]string{"2", "200"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	if _, err := r.Repair(context.Background(), []Difference{{Key: "2", Kind: Extra}}); err != nil {
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
	source := table(t, "source", [2]string{"1", "100"})
	target := table(t, "target", [2]string{"1", "100"}, [2]string{"2", "200"})
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}

	// The comparison said the row was missing from the target; the source has
	// since lost it too.
	if _, err := r.Repair(context.Background(), []Difference{{Key: "2", Kind: Missing}}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	if _, ok := amountOf(t, target, "2"); ok {
		t.Error("a row the source no longer has was left on the target")
	}
}

func TestARepairFailureIsReportedWithWhatItGotThrough(t *testing.T) {
	source := table(t, "source", [2]string{"1", "100"})
	target := table(t, "target")
	r := &SQLRepairer{Source: source, Target: target, Upsert: upsertFor}
	_ = target.DB.Close()

	fixed, err := r.Repair(context.Background(), []Difference{{Key: "1", Kind: Missing}})
	if err == nil {
		t.Fatal("Repair succeeded against a closed target")
	}
	if fixed != 0 {
		t.Errorf("fixed = %d against a closed target", fixed)
	}
}

// ------------------------------------------------------------------ shape

func TestTheSideNamesItself(t *testing.T) {
	if got := (&SQLSide{Table: "orders"}).Name(); got != "orders" {
		t.Errorf("Name() = %q", got)
	}
	if got := (&SQLSide{Schema: "shop", Table: "orders"}).Name(); got != "shop.orders" {
		t.Errorf("Name() = %q", got)
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

// --------------------------------------------------- ordering-free compare

// memoryEnd is a side that streams its rows and can be asked about specific
// keys, in an order that deliberately does not match the other side's.
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

func compareByKey(t *testing.T, source, target End, chunk int) Result {
	t.Helper()

	result, err := CompareByKey(context.Background(), source, target, chunk)
	if err != nil {
		t.Fatalf("CompareByKey: %v", err)
	}
	return result
}

// TestTheOrderingFreeCompareIgnoresTheStreamOrder is the whole reason it exists.
// A document store sorts an _id that may be an ObjectId, a string or a number,
// and that order is not the order their rendered forms sort in. A merge of two
// ordered streams would not merely miss differences there — it would invent
// them, reporting every row after the first disagreement as both missing and
// extra.
func TestTheOrderingFreeCompareIgnoresTheStreamOrder(t *testing.T) {
	source := &memoryEnd{name: "source", rows: rowsOf(
		[2]string{"zz", "a"}, [2]string{"aa", "b"}, [2]string{"mm", "c"})}
	target := &memoryEnd{name: "target", rows: rowsOf(
		[2]string{"mm", "c"}, [2]string{"zz", "a"}, [2]string{"aa", "b"})}

	got := compareByKey(t, source, target, 2)

	if !got.Identical() {
		t.Errorf("result = %s; the two sides hold the same rows in a different order",
			got.Summary())
	}
	if got.SourceRows != 3 || got.TargetRows != 3 {
		t.Errorf("counted %d and %d rows", got.SourceRows, got.TargetRows)
	}
}

func TestTheOrderingFreeCompareFindsEachKind(t *testing.T) {
	source := &memoryEnd{name: "source", rows: rowsOf(
		[2]string{"same", "a"}, [2]string{"changed", "a"}, [2]string{"lost", "a"})}
	target := &memoryEnd{name: "target", rows: rowsOf(
		[2]string{"same", "a"}, [2]string{"changed", "b"}, [2]string{"unexpected", "a"})}

	got := compareByKey(t, source, target, 10)

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

func TestTheOrderingFreeCompareReportsAnUnreadableSide(t *testing.T) {
	source := &memoryEnd{name: "source", err: errors.New("connection refused")}

	if _, err := CompareByKey(context.Background(), source, &memoryEnd{name: "target"}, 0); err == nil {
		t.Fatal("CompareByKey succeeded against a side it could not read")
	}
}

// ---------------------------------------------------------------- SQL ends

func TestTheSQLEndStreamsEveryRowOnce(t *testing.T) {
	side := table(t, "source", [2]string{"1", "a"}, [2]string{"2", "b"}, [2]string{"3", "c"})
	end := &SQLEnd{Side: side}

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

func TestTheSQLEndLooksUpTheKeysItIsGiven(t *testing.T) {
	side := table(t, "target", [2]string{"1", "a"}, [2]string{"2", "b"})
	end := &SQLEnd{Side: side}

	found, err := end.Lookup(context.Background(), []string{"2", "absent"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("found %d rows, want one", len(found))
	}
	if _, ok := found["2"]; !ok {
		t.Errorf("found = %v", found)
	}
}

func TestAnEmptyLookupAsksNothing(t *testing.T) {
	end := &SQLEnd{Side: table(t, "target")}

	found, err := end.Lookup(context.Background(), nil)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(found) != 0 {
		t.Errorf("found = %v", found)
	}
}

// TestTheTwoComparisonsAgree checks the cheaper merge and the ordering-free
// walk reach the same conclusion about the same two tables.
func TestTheTwoComparisonsAgree(t *testing.T) {
	source := table(t, "source", [2]string{"1", "a"}, [2]string{"2", "b"}, [2]string{"3", "c"})
	target := table(t, "target", [2]string{"1", "a"}, [2]string{"3", "different"})

	merged := compare(t, source, target, 0)
	walked := compareByKey(t, &SQLEnd{Side: source}, &SQLEnd{Side: target}, 0)

	if merged.Missing != walked.Missing || merged.Differing != walked.Differing ||
		merged.Extra != walked.Extra {
		t.Errorf("the two comparisons disagree:\n  merge:  %s\n  lookup: %s",
			merged.Summary(), walked.Summary())
	}
}

// -------------------------------------------------------- document digests

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

func TestAnUnreadableKeyIsReported(t *testing.T) {
	for _, key := range []string{"not hex", "00ff"} {
		if _, err := idFromKey(key); err == nil {
			t.Errorf("idFromKey(%q) returned no error", key)
		}
	}
}

// ------------------------------------------------------- repair as it walks

// TestEveryDifferenceIsRepairedNotJustTheSampled is the fix. Repairing from the
// reported sample only ever fixed the first hundred, so a table a thousand rows
// apart needed ten passes to converge and nothing said how far along it was.
func TestEveryDifferenceIsRepairedNotJustTheSampled(t *testing.T) {
	const differences = DefaultReportLimit + 50
	pairs := make([][2]string, 0, differences)
	for i := 0; i < differences; i++ {
		pairs = append(pairs, [2]string{fmt.Sprintf("%05d", i), "a"})
	}
	source := &memorySide{name: "source", rows: rowsOf(pairs...)}

	var fixed []string
	result, err := CompareAndRepair(context.Background(), source, &memorySide{name: "target"}, 0,
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
	// The report is still capped, because a hundred lines is what somebody can
	// read.
	if len(result.Sample) != DefaultReportLimit || !result.Truncated {
		t.Errorf("sample holds %d, truncated=%v", len(result.Sample), result.Truncated)
	}
}

// TestARepairFailureDoesNotStopTheRest matters because stopping at the first
// failure would leave the rest of the table wrong for the sake of one row.
func TestARepairFailureDoesNotStopTheRest(t *testing.T) {
	source := &memorySide{name: "source", rows: rowsOf(
		[2]string{"1", "a"}, [2]string{"2", "a"}, [2]string{"3", "a"})}

	attempted := 0
	result, err := CompareAndRepair(context.Background(), source, &memorySide{name: "target"}, 0,
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
	source := table(t, "source", [2]string{"1", "a"})
	target := table(t, "target")

	got := compare(t, source, target, 0)

	if got.Repaired != 0 || got.RepairFailed != 0 {
		t.Errorf("repaired %d, failed %d for a comparison that was only asked to look",
			got.Repaired, got.RepairFailed)
	}
}

// TestTheOrderingFreeWalkRepairsToo covers the MongoDB path, which uses the
// other comparison.
func TestTheOrderingFreeWalkRepairsToo(t *testing.T) {
	source := &memoryEnd{name: "source", rows: rowsOf(
		[2]string{"a", "1"}, [2]string{"b", "1"})}

	var fixed []string
	result, err := CompareByKeyAndRepair(context.Background(), source, &memoryEnd{name: "target"}, 0,
		func(d Difference) error {
			fixed = append(fixed, d.Key)
			return nil
		})
	if err != nil {
		t.Fatalf("CompareByKeyAndRepair: %v", err)
	}
	if len(fixed) != 2 || result.Repaired != 2 {
		t.Errorf("repaired %v (%d)", fixed, result.Repaired)
	}
}

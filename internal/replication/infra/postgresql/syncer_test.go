package postgresql

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

// refusesOnce fails its first query the way a connection busy with another is refused.
type refusesOnce struct {
	refused bool
	next    sourceQuerier
}

func (r *refusesOnce) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !r.refused {
		r.refused = true
		return nil, errors.New("conn busy")
	}
	return r.next.Query(ctx, sql, args...)
}

// A failure means one refused lookup left the table addressed by every column, so its updates match nothing.
func TestAFailedKeyLookupIsNotRemembered(t *testing.T) {
	db := targetDB(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('7','Ada','ada@example.com')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	source := &refusesOnce{next: &answering{replies: []sourceReply{{
		match: "indisprimary", columns: []string{"attname"}, rows: [][]any{{"id"}},
	}}}}
	st := stateWith(t, db, relation(1, "main", "orders", "id", "customer", "email"))
	st.reader.Keys = (&Syncer{logger: quiet()}).keyLookup(context.Background(),
		&schemaWork{Source: source, Logger: quiet()})

	update := updateMessage(1, nil, tuple(text("7"), text("Grace"), text("grace@example.com")))

	if _, err := st.handleUpdate(update); err == nil {
		t.Error("a key lookup that failed was carried as a table with no key: the " +
			"update was addressed by the values it sets, which the target does not hold yet")
	}
	if _, err := st.handleUpdate(update); err != nil {
		t.Fatalf("the update after the source answered again: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "7|Grace|grace@example.com" {
		t.Errorf("rows = %v, want the update applied once the key could be read", got)
	}
}

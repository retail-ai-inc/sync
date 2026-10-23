package mysql

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// A failure means an operator's re-copy writes to the wrong table, restarts from the first key, or overwrites masked fields with plaintext.
func TestAResyncIsBuiltWithTheTasksSecurityAndTargets(t *testing.T) {
	store := &checkpoint.SQLStore{}
	s := &Syncer{logger: quietLogger(), cfg: config.SyncConfig{
		Type:             "mysql",
		SourceConnection: "u:p@tcp(h:3306)/shop",
		TargetConnection: "u:p@tcp(t:3306)/shop_bk",
		Resync:           []string{"users"},
		Mappings:         securedTable("users", "users_dr", "email"),
	}}

	resyncs := s.resyncs(store)
	if len(resyncs) != 1 {
		t.Fatalf("%d re-copies were built for one listed table", len(resyncs))
	}
	rs := resyncs[0]
	if rs.NS != (domain.Namespace{DB: "shop", Object: "users"}) {
		t.Errorf("NS = %v, want shop.users", rs.NS)
	}
	if rs.ProgressKey != "resync:shop.users" {
		t.Errorf("ProgressKey = %q, want resync:shop.users", rs.ProgressKey)
	}
	if rs.Progress != store {
		t.Error("the re-copy records its progress somewhere other than the task's store")
	}
	chunks, ok := rs.Reader.(*Chunks)
	if !ok {
		t.Fatalf("Reader is %T, want *Chunks", rs.Reader)
	}
	_ = chunks.Source.Close()
	if got := chunks.TargetOf("audit"); got != "audit" {
		t.Errorf("an unmapped table is re-copied into %q, want its own name", got)
	}

	fake := &fakeDB{replies: []reply{
		clockReply(1),
		oneColumn("KEY_COLUMN_USAGE", "id"),
		twoColumn("information_schema.COLUMNS", [][2]string{{"id", ""}, {"email", ""}}),
		{match: "FROM `shop`.`users`", columns: []string{"id", "email"},
			rows: [][]driver.Value{{int64(1), "a@b.com"}}},
	}}
	chunks.Source = fake.open(t)

	chunk, err := chunks.NextChunk(context.Background(), rs.NS, "", 10)
	if err != nil {
		t.Fatalf("NextChunk: %v", err)
	}
	if len(chunk.Events) != 1 {
		t.Fatalf("the chunk carries %d events, want 1", len(chunk.Events))
	}
	written := chunk.Events[0].Payload.(statement)
	if !strings.Contains(written.query, "INTO `shop_bk`.`users_dr`") {
		t.Errorf("the re-copy writes with %q, want shop_bk.users_dr", written.query)
	}
	if written.args[1] == "a@b.com" {
		t.Error("the re-copy sends the masked field in the clear")
	}
}

package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

func TestAnInitialCopyCutShortMidTableIsAnError(t *testing.T) {
	// A failure means the rows after the cut are never copied: the stream starts after the snapshot.
	dropped := errors.New("invalid connection")
	source := &fakeDB{replies: []reply{
		{match: "SHOW COLUMNS FROM `shop`.`orders`",
			columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
			rows: [][]driver.Value{
				{"id", "int", "NO", "PRI", nil, ""},
				{"amount", "int", "YES", "", nil, ""},
			}},
		{match: "SELECT `id`,`amount` FROM `shop`.`orders`",
			columns: []string{"id", "amount"},
			rows: [][]driver.Value{
				{int64(1), int64(10)}, {int64(2), int64(20)}, {int64(3), int64(30)},
			},
			rowsErr: dropped},
	}}
	target := &fakeDB{replies: []reply{
		{match: "information_schema.tables", columns: []string{"COUNT(*)"},
			rows: [][]driver.Value{{int64(1)}}},
		{match: "INSERT INTO"},
	}}

	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	s := &MySQLSyncer{logger: quiet, cfg: config.SyncConfig{
		ID:               1,
		Type:             "mysql",
		SourceConnection: "user:pass@tcp(source:3306)/shop",
		TargetConnection: "user:pass@tcp(target:3306)/shop_bk",
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{
			{SourceTable: "orders", TargetTable: "orders"},
		}}},
	}}

	ctx := context.Background()
	conn, err := source.open(t).Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	err = s.doInitialSync(ctx, conn, target.open(t))
	if err == nil {
		t.Fatal("a copy whose read of shop.orders died after 3 rows reported success")
	}
	if !strings.Contains(err.Error(), "shop.orders") || !strings.Contains(err.Error(), dropped.Error()) {
		t.Errorf("error = %v, want it to name shop.orders and the read's error", err)
	}
}

// firstCopy runs the first copy of shop into shop_bk between two fakes.
func firstCopy(t *testing.T, source, target *fakeDB, mappings []config.DatabaseMapping) error {
	t.Helper()

	s := &MySQLSyncer{logger: quietLogger(), cfg: config.SyncConfig{
		ID:               1,
		Type:             "mysql",
		SourceConnection: "user:pass@tcp(source:3306)/shop",
		TargetConnection: "user:pass@tcp(target:3306)/shop_bk",
		Mappings:         mappings,
	}}

	ctx := context.Background()
	conn, err := source.open(t).Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()
	return s.doInitialSync(ctx, conn, target.open(t))
}

// showColumns answers SHOW COLUMNS with plain, stored columns.
func showColumns(match string, names ...string) reply {
	rows := make([][]driver.Value, 0, len(names))
	for _, name := range names {
		rows = append(rows, []driver.Value{name, "int", "YES", "", nil, ""})
	}
	return reply{match: match,
		columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"}, rows: rows}
}

// targetHolds answers the target's check for whether a table exists.
func targetHolds(count int64) reply {
	return reply{match: "information_schema.tables", columns: []string{"COUNT(*)"},
		rows: [][]driver.Value{{count}}}
}

// A failure means a table with a reserved-word column or a hyphen in its name can never finish its first copy.
func TestTheFirstCopyQuotesTheNamesItReads(t *testing.T) {
	source := &fakeDB{replies: []reply{
		{match: "SHOW CREATE TABLE `shop`.`order-items`",
			columns: []string{"Table", "Create Table"},
			rows: [][]driver.Value{{"order-items",
				"CREATE TABLE `order-items` (`id` int NOT NULL, `rank` int, PRIMARY KEY (`id`))"}}},
		showColumns("SHOW COLUMNS FROM `shop`.`order-items`", "id", "rank"),
		{match: "SELECT `id`,`rank` FROM `shop`.`order-items`",
			columns: []string{"id", "rank"},
			rows:    [][]driver.Value{{int64(1), int64(7)}}},
	}}
	target := &fakeDB{replies: []reply{
		targetHolds(0),
		{match: "CREATE TABLE"},
		{match: "INSERT INTO"},
	}}

	if err := firstCopy(t, source, target, mapTable("order-items", "order-items")); err != nil {
		t.Fatalf("the first copy failed: %v", err)
	}
	if !target.wasAsked("INSERT INTO `shop_bk`.`order-items`") {
		t.Errorf("no row reached the target; it was asked %q", target.statements())
	}
	for _, asked := range append(source.statements(), target.statements()...) {
		if strings.Contains(asked, "order-items") && !strings.Contains(asked, "`order-items`") {
			t.Errorf("a statement names the table unquoted: %q", asked)
		}
	}
}

// showCreate answers SHOW CREATE TABLE as the server does.
func showCreate(match, table, statement string) reply {
	return reply{match: match, columns: []string{"Table", "Create Table"},
		rows: [][]driver.Value{{table, statement}}}
}

// A failure means a renamed target is created under the source's name, so every batch into the mapped table fails and the first copy never completes.
func TestARenamedTargetIsCreatedUnderItsMappedName(t *testing.T) {
	source := &fakeDB{replies: []reply{
		showCreate("SHOW CREATE TABLE `shop`.`users`", "users",
			"CREATE TABLE `users` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"),
		showColumns("SHOW COLUMNS FROM `shop`.`users`", "id"),
		{match: "SELECT `id` FROM `shop`.`users`", columns: []string{"id"},
			rows: [][]driver.Value{{int64(1)}}},
	}}
	target := &fakeDB{replies: []reply{
		targetHolds(0),
		{match: "CREATE TABLE"},
		{match: "INSERT INTO"},
	}}

	if err := firstCopy(t, source, target, mapTable("users", "users_dr")); err != nil {
		t.Fatalf("the first copy failed: %v", err)
	}
	if !target.wasAsked("CREATE TABLE `shop_bk`.`users_dr` (") {
		t.Errorf("users_dr was not created; the target was asked %q", target.statements())
	}
	if target.wasAsked("CREATE TABLE `users`") {
		t.Error("a table was created under the source's name")
	}
	if !target.wasAsked("INSERT INTO `shop_bk`.`users_dr`") {
		t.Errorf("no row reached users_dr; the target was asked %q", target.statements())
	}
}

// A failure means the target table is created under a name nobody mapped, or from a statement this cannot rename.
func TestTheCreatedTableTakesTheMappedNameHoweverTheSourceQuotesIt(t *testing.T) {
	for name, c := range map[string]struct {
		statement string
		want      string
	}{
		"quoted":   {"CREATE TABLE `users` (`id` int)", "CREATE TABLE `shop_bk`.`users_dr` (`id` int)"},
		"unquoted": {"CREATE TABLE users (id int)", "CREATE TABLE `shop_bk`.`users_dr` (id int)"},
		"ansi":     {`CREATE TABLE "users" ("id" int)`, ""},
	} {
		t.Run(name, func(t *testing.T) {
			source := &fakeDB{replies: []reply{showCreate("SHOW CREATE TABLE", "users", c.statement)}}
			conn, err := source.open(t).Conn(context.Background())
			if err != nil {
				t.Fatalf("conn: %v", err)
			}
			defer conn.Close()

			got, _, err := (&MySQLSyncer{}).generateCreateTableSQL(context.Background(), conn,
				"shop", "users", "shop_bk", "users_dr")
			if c.want == "" {
				if err == nil {
					t.Errorf("a statement that does not open with the table's name was renamed to %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("generateCreateTableSQL: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

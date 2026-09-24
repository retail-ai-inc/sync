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
		{match: "SHOW COLUMNS FROM shop.orders",
			columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
			rows: [][]driver.Value{
				{"id", "int", "NO", "PRI", nil, ""},
				{"amount", "int", "YES", "", nil, ""},
			}},
		{match: "SELECT id,amount FROM shop.orders",
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

//go:build integration

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/test/harness"
)

// They did not, for a while: they lived in the old syncer's Start, the shared
// pipeline replaced it, and the functions stayed behind with no callers.

// TestAMinimalRowImageStopsTheTaskBeforeItCopiesAnything is the one failure in
// this package that corrupts data without producing an error: with anything
// other than FULL the binlog carries only the columns that changed, and the
// UPDATE this syncer builds writes the rest as NULL over values nobody touched.
func TestAMinimalRowImageStopsTheTaskBeforeItCopiesAnything(t *testing.T) {
	source := open(t, harness.MySQLSource, sourceDB)

	var name, was string
	if err := source.QueryRow("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").
		Scan(&name, &was); err != nil {
		t.Fatalf("read binlog_row_image: %v", err)
	}
	mustExec(t, source, "SET GLOBAL binlog_row_image = 'MINIMAL'")
	t.Cleanup(func() {
		_, _ = source.Exec("SET GLOBAL binlog_row_image = ?", was)
	})

	cfg := syncTask(t, harness.UniqueName("rowimage"))
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := NewSyncer(cfg, logger).Start(ctx)

	if err == nil {
		t.Fatal("the task started against a MINIMAL row image; it must refuse")
	}
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("error = %v, want an unrecoverable one: retrying cannot fix a "+
			"server setting, and retrying hides it", err)
	}
	if !strings.Contains(err.Error(), "binlog_row_image") {
		t.Errorf("error = %q, want it to name the setting to change", err)
	}
	t.Logf("refused, as it should: %v", err)
}

// TestAFullRowImageIsAccepted is the other half: the check has to let a
// correctly configured source through, or the test above would pass for a
// syncer that refuses everything — including one that refuses every source.
func TestAFullRowImageIsAccepted(t *testing.T) {
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)

	var name, image string
	if err := src.QueryRow("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").
		Scan(&name, &image); err != nil {
		t.Fatalf("read binlog_row_image: %v", err)
	}
	if !strings.EqualFold(image, "FULL") {
		t.Skipf("the source logs %s row images, so this proves nothing", image)
	}

	table := harness.UniqueName("rowimage")
	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'a')", table))

	cfg := syncTask(t, table)
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Long enough to get past the checks, the snapshot and into the stream;
	// short enough not to hold the suite up. The deadline is the pass.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	started := time.Now()
	err := NewSyncer(cfg, logger).Start(ctx)
	ran := time.Since(started)

	if domain.IsUnrecoverable(err) {
		t.Fatalf("a correctly configured source was refused: %v", err)
	}
	// Returning immediately would mean it fell over somewhere before the
	// stream, which would make the assertion above vacuous.
	if ran < 7*time.Second {
		t.Fatalf("Start returned after %v (%v), so it never reached the stream and "+
			"this test proves nothing about the checks", ran, err)
	}

	// And it really did replicate, which is the point of letting it through.
	if n := countRows(t, tgt, table, ""); n != 1 {
		t.Errorf("target holds %d rows, want 1", n)
	}
}

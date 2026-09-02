package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// What the source has to be set up to do, checked before a row is copied.
// These used to run inside the old syncer's Start, which the shared pipeline
// replaced.

// preflight refuses, or warns about, a source this task cannot replicate
// correctly.
func preflight(ctx context.Context, source *sql.DB, flavour string, log logrus.FieldLogger) error {
	mariaDB := strings.EqualFold(flavour, "mariadb")

	if !mariaDB {
		// MariaDB has no such setting and always logs whole rows.
		image, err := globalVariable(ctx, source, "binlog_row_image")
		if err != nil {
			return fmt.Errorf("read binlog_row_image from the source: %w", err)
		}
		if err := requireFullRowImage(image); err != nil {
			return domain.Unrecoverable("%v", err)
		}
	}

	mode, err := globalVariable(ctx, source, "gtid_mode")
	if err != nil {
		// Not fatal: a source that will not answer this still replicates, it
		// just cannot be checked. MariaDB spells its own GTID settings
		// differently and has no gtid_mode at all.
		log.Debugf("[MySQL] Could not read gtid_mode from the source: %v", err)
		return nil
	}
	if warning := describeGTIDMode(mode, mariaDB); warning != "" {
		log.Warn(warning)
	}
	return nil
}

// globalVariable reads one server setting, empty when the server has no such
// setting. The name is interpolated rather than bound: SHOW GLOBAL VARIABLES
// does not take a placeholder, and MySQL answers a prepared one with a syntax
// error.
func globalVariable(ctx context.Context, db *sql.DB, name string) (string, error) {
	if !settingName(name) {
		return "", fmt.Errorf("%q is not a server setting name", name)
	}

	var variable, value string
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", name)).
		Scan(&variable, &value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return value, nil
}

// settingName reports whether a string is shaped like a server setting, which is
// what makes it safe to interpolate.
func settingName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		default:
			return false
		}
	}
	return true
}

// describeGTIDMode reports why a source without GTIDs is a problem here, or ""
// when it has them. This is a warning rather than a refusal because
// replication works either way.
func describeGTIDMode(mode string, mariaDB bool) string {
	if mariaDB || mode == "" {
		// MariaDB tracks GTIDs through different settings, and a server with no
		// gtid_mode at all predates them.
		return ""
	}
	if strings.EqualFold(mode, "ON") {
		return ""
	}
	return fmt.Sprintf("[MySQL] The source has gtid_mode=%s, so this task's position "+
		"is a binlog file name and an offset. Those only mean something on the server "+
		"that produced them: after a failover to a new primary the position either "+
		"cannot be found or points at unrelated bytes, and recovering means a fresh "+
		"copy. A source failover is the ordinary case in the setup this tool is for, "+
		"so set gtid_mode=ON and enforce_gtid_consistency=ON before relying on it.", mode)
}

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

// What the target has to be set up to do. Nothing checked the target at all:
// every one of these is a way for the copy to diverge quietly, with the task
// reporting that it applied everything -- because it did.

// targetPreflight refuses, or warns about, a target this task cannot replicate
// onto correctly. tables are the tables being replicated, which is what makes
// the trigger check specific rather than a scan of the whole schema.
func targetPreflight(
	ctx context.Context, target *sql.DB, schema string, tables []string,
	log logrus.FieldLogger,
) error {
	if err := refuseReadOnlyTarget(ctx, target); err != nil {
		return err
	}

	// A warning and not a refusal from here down: each one is a property of a
	// target somebody else administers, and stopping replication over it leaves
	// Osaka further behind than running with a caveat does.
	if scheduler, err := globalVariable(ctx, target, "event_scheduler"); err != nil {
		log.Debugf("[MySQL] Could not read event_scheduler from the target: %v", err)
	} else if warning := describeEventScheduler(scheduler); warning != "" {
		log.Warn(warning)
	}

	triggers, err := triggersOn(ctx, target, schema, tables)
	if err != nil {
		log.Debugf("[MySQL] Could not list the target's triggers: %v", err)
		return nil
	}
	if warning := describeTriggers(triggers); warning != "" {
		log.Warn(warning)
	}
	return nil
}

// refuseReadOnlyTarget stops a task whose target will not accept a write.
//
// This is a refusal rather than a warning because there is nothing to run: every
// statement fails, and the task would spend its backoff rediscovering that. It
// is also the state a target is left in by a failover that promoted the other
// side, so saying so plainly is more use than a thousand rejected writes.
func refuseReadOnlyTarget(ctx context.Context, target *sql.DB) error {
	for _, name := range []string{"read_only", "super_read_only"} {
		value, err := globalVariable(ctx, target, name)
		if err != nil {
			// A target that will not answer still replicates; the write itself
			// reports the problem if there is one.
			return nil
		}
		if isOn(value) {
			return domain.Unrecoverable("the target has %s=ON, so it accepts no "+
				"writes and nothing can be applied to it. A target is left this way "+
				"by a failover that promoted the other side: check which of the two "+
				"is the primary before turning it off", name)
		}
	}
	return nil
}

func isOn(value string) bool {
	return strings.EqualFold(value, "ON") || value == "1"
}

// describeEventScheduler reports why a target running events is a problem, or ""
// when it is not.
func describeEventScheduler(value string) string {
	if !isOn(value) {
		return ""
	}
	return "[MySQL] The target has event_scheduler=ON. A scheduled event on the " +
		"target changes rows the source knows nothing about, so the two drift " +
		"apart while this task reports that it applied everything -- because it " +
		"did. Turn it off on the target and leave the events to the source, " +
		"which is where they will be when the roles are swapped."
}

// describeTriggers reports why triggers on the replicated tables are a problem.
func describeTriggers(triggers []string) string {
	if len(triggers) == 0 {
		return ""
	}
	return fmt.Sprintf("[MySQL] The target has triggers on tables this task "+
		"replicates (%s). Replication applies the row the source ended up with, "+
		"after the source's own triggers ran; a trigger on the target then fires "+
		"again on that row and writes a change the source never made. Drop them "+
		"on the target.", strings.Join(triggers, ", "))
}

// triggersOn lists the triggers defined on any of the given tables.
//
// The table names are bound rather than interpolated, so a table named by a
// task's configuration cannot become part of the statement.
func triggersOn(ctx context.Context, db *sql.DB, schema string, tables []string) ([]string, error) {
	if schema == "" || len(tables) == 0 {
		return nil, nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(tables)), ",")
	params := make([]interface{}, 0, len(tables)+1)
	params = append(params, schema)
	for _, table := range tables {
		params = append(params, table)
	}

	rows, err := db.QueryContext(ctx, `
SELECT trigger_name, event_object_table
FROM information_schema.triggers
WHERE trigger_schema = ?
  AND event_object_table IN (`+placeholders+`)
ORDER BY event_object_table, trigger_name`, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var found []string
	for rows.Next() {
		var name, table string
		if err := rows.Scan(&name, &table); err != nil {
			return nil, err
		}
		found = append(found, table+"."+name)
	}
	return found, rows.Err()
}

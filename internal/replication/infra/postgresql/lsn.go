package postgresql

import (
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// Where the stream resumes from, and how it is stored.

// statement is what the applier writes: one SQL statement and its arguments.
type statement struct {
	query string
	args  []interface{}
}

// walCheckpoint is the stored position.
type walCheckpoint struct {
	LSN string `json:"lsn"`
	// Source names the server the position belongs to, without credentials. An
	// LSN means nothing on another server: read there it addresses unrelated
	// WAL, and the read succeeds, so the task resumes from somewhere arbitrary
	// with no error to show for it.
	Source string `json:"source,omitempty"`
}

func endpointOf(connection string) string {
	return dsn.Endpoint("postgresql", connection)
}

// encodeLSN renders a position for storage.
func encodeLSN(lsn pglogrepl.LSN, source string) (string, error) {
	return checkpoint.Encode(walCheckpoint{LSN: lsn.String(), Source: source})
}

// decodeLSN reads a stored position, reporting zero when there is none. A
// position recorded against a server other than source comes back with that
// server named, and its LSN must not be resumed from: it addresses unrelated WAL.
//
// A payload written by an older build holds the LSN as plain text rather than a
// document, so both forms are read: refusing the old form would make every task
// that had run before copy its source again.
func decodeLSN(payload, source string) (pglogrepl.LSN, string, error) {
	if strings.TrimSpace(payload) == "" {
		return 0, "", nil
	}

	var stored walCheckpoint
	found, err := checkpoint.Decode(payload, &stored)
	if err != nil || !found {
		lsn, parseErr := parseLSNFromString(strings.TrimSpace(payload))
		return lsn, "", parseErr
	}

	lsn, err := parseLSNFromString(stored.LSN)
	if err != nil {
		return 0, "", err
	}
	if stored.Source != "" && source != "" && stored.Source != source {
		return lsn, stored.Source, nil
	}
	return lsn, "", nil
}

// resumePoint must run before any slot is created: a slot left behind by a task
// that then stops makes the source retain WAL indefinitely.
func resumePoint(stored pglogrepl.LSN, elsewhere, here, slot string, slotExists bool,
	taskID int) (pglogrepl.LSN, error) {

	if stored == 0 && elsewhere == "" {
		return 0, nil
	}
	if slotExists {
		if elsewhere != "" {
			// Zero lets the server resume the slot from its own confirmed position.
			return 0, nil
		}
		return stored, nil
	}

	origin := here
	if elsewhere != "" {
		origin = elsewhere
	}
	return 0, domain.Unrecoverable("the target holds what was replicated from %s up to "+
		"%s, and %s has no replication slot %s to read the changes committed after it "+
		"from. A slot created there now starts at the current end of the log, so it "+
		"would skip them, and the next start would trust it. To recover, empty the "+
		"target tables, delete the rows with task_id = %d from _sync_checkpoint on the "+
		"target, and edit this task or restart sync: a fresh copy then runs",
		origin, stored, here, slot, taskID)
}

// parseLSNFromString reads the "X/Y" form PostgreSQL prints, and tolerates a
// bare number.
func parseLSNFromString(text string) (pglogrepl.LSN, error) {
	text = strings.TrimSpace(text)
	// An empty position is reported rather than read as zero. Zero means "start
	// from the beginning and copy everything", which is not something to arrive
	// at because a field was blank.
	if text == "" {
		return 0, fmt.Errorf("the stored position is empty")
	}

	// The shape is checked before the driver's parser, which accepts "1/2/3" by
	// reading the first two parts and ignoring the rest -- a position that is
	// not one would then be used as though it were.
	parts := strings.Split(text, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("%q is not a log position", text)
	}
	if lsn, err := pglogrepl.ParseLSN(text); err == nil {
		return lsn, nil
	}
	high, err := hexStrToUint32(parts[0])
	if err != nil {
		return 0, fmt.Errorf("%q is not a log position: %w", text, err)
	}
	low, err := hexStrToUint32(parts[1])
	if err != nil {
		return 0, fmt.Errorf("%q is not a log position: %w", text, err)
	}
	return pglogrepl.LSN(uint64(high)<<32 | uint64(low)), nil
}

func hexStrToUint32(text string) (uint32, error) {
	var value uint64
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, fmt.Errorf("no digits")
	}
	for _, r := range text {
		var digit uint64
		switch {
		case r >= '0' && r <= '9':
			digit = uint64(r - '0')
		case r >= 'a' && r <= 'f':
			digit = uint64(r-'a') + 10
		case r >= 'A' && r <= 'F':
			digit = uint64(r-'A') + 10
		default:
			return 0, fmt.Errorf("%q is not a hexadecimal number", text)
		}
		value = value<<4 | digit
		if value > 0xFFFFFFFF {
			return 0, fmt.Errorf("%q does not fit in 32 bits", text)
		}
	}
	return uint32(value), nil
}

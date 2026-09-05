package postgresql

import (
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"
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

// decodeLSN reads a stored position, reporting zero when there is none.
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

	if stored.Source != "" && source != "" && stored.Source != source {
		// An LSN addresses one server's WAL and nowhere else. Resuming from it
		// here would read unrelated bytes, and the read would succeed.
		return 0, stored.Source, nil
	}

	lsn, parseErr := parseLSNFromString(stored.LSN)
	return lsn, "", parseErr
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

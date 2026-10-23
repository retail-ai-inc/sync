package dbinspect

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// An id as it arrives from a browser.
//
// The API sends a task's id and a backup job's id as JSON numbers, because
// they are integers in the control database. The edit forms send them back --
// and one of them sent the number it was given while the other stringified it
// first, so a field declared as a string decoded from one form and refused the
// other with "cannot unmarshal number into Go struct field". The refusal was
// the whole request: nobody could test a backup job's connection.
//
// Both are accepted here. An id is an identifier to be handed back to a
// lookup, not a number to do arithmetic with, so the distinction between 10
// and "10" is noise at this boundary.
type flexibleID string

func (id flexibleID) String() string { return string(id) }

func (id *flexibleID) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*id = ""
		return nil
	}

	if trimmed[0] == '"' {
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return fmt.Errorf("read an id: %w", err)
		}
		*id = flexibleID(text)
		return nil
	}

	// A number, which is what the list endpoints answer with. Kept as the text
	// it arrived as: 10 and 10.0 name the same row, and a lookup takes a string.
	var number json.Number
	if err := json.Unmarshal(trimmed, &number); err != nil {
		return fmt.Errorf("read an id: %w", err)
	}
	if whole, err := strconv.ParseInt(number.String(), 10, 64); err == nil {
		*id = flexibleID(strconv.FormatInt(whole, 10))
		return nil
	}
	*id = flexibleID(number.String())
	return nil
}

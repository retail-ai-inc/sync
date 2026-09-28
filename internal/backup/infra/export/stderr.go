package export

import (
	"bytes"
	"io"
	"os"
	"strings"
)

// Keeping what a failed command said.
//
// The external commands wrote their standard error to this process's, so the
// reason a backup failed went into the container log and the job's recorded
// outcome said "exit status 1". By the time anybody read that outcome the log
// had rotated or the container had been replaced, and the answer to "why did
// last night's backup fail" was gone.

// stderrTailBytes is how much of a command's complaint is worth keeping. A
// failing mysql says its piece in a line or two; a command that fails per row
// could say megabytes, and the outcome is a column of the control database.
const stderrTailBytes = 4 << 10

// complaint collects what a command writes to standard error, keeping the last
// few kilobytes of it, and passes it through to this process's standard error
// so the container log still has the whole of it.
type complaint struct {
	kept bytes.Buffer
}

func (c *complaint) Write(p []byte) (int, error) {
	n, err := os.Stderr.Write(p)
	c.kept.Write(p)
	if c.kept.Len() > stderrTailBytes {
		trimmed := c.kept.Bytes()[c.kept.Len()-stderrTailBytes:]
		rest := make([]byte, len(trimmed))
		copy(rest, trimmed)
		c.kept.Reset()
		c.kept.Write(rest)
	}
	return n, err
}

// String is what to put in the error, or "" when the command said nothing.
func (c *complaint) String() string {
	return strings.TrimSpace(c.kept.String())
}

// said renders a command's complaint for an error message, with a leading
// separator when there is anything to say.
func (c *complaint) said() string {
	kept := c.String()
	if kept == "" {
		return " (the command said nothing on standard error)"
	}
	return ": " + kept
}

var _ io.Writer = (*complaint)(nil)

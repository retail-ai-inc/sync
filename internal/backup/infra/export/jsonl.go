package export

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// Merging one export into another, a line at a time.
//
// mongoexport writes JSONL: one document per line. The merge used to read a
// whole export with os.ReadFile, copy it into a string, and split that on
// newlines -- two copies of the export plus a string header per line, which at
// ten million documents is around 160MB of headers on its own. The collections
// large enough to be worth backing up were the ones that could not be, and the
// process died on its memory limit rather than on anything it could report.

// maxJSONLine is the longest line appendJSONLines will accept.
//
// A BSON document may be 16MB, and its JSON form is larger: numbers and binary
// grow, and every quote and backslash inside a string is escaped. 64MB leaves
// room for that without letting a corrupt file be read into memory unbounded.
const maxJSONLine = 64 << 20

// appendJSONLines copies the JSON objects of a JSONL file into out.
//
// Lines that are blank or do not begin with '{' are dropped, which is what the
// merge did before: mongoexport writes nothing else, and a line that is not an
// object would make the merged file unreadable as JSONL.
func appendJSONLines(path string, out *bufio.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1<<20)
	for {
		line, err := readLine(reader)
		if line != "" {
			// The newline is written separately rather than concatenated: a
			// concatenation allocates a copy of the line, once per document.
			if _, writeErr := out.WriteString(line); writeErr != nil {
				return fmt.Errorf("write a line of %s: %w", path, writeErr)
			}
			if writeErr := out.WriteByte('\n'); writeErr != nil {
				return fmt.Errorf("write a line of %s: %w", path, writeErr)
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
	}
}

// readLine returns the next line worth writing, trimmed, or "" for one to
// drop. The error is io.EOF on the last line, which may still carry content:
// a file with no trailing newline used to lose its last document.
func readLine(reader *bufio.Reader) (string, error) {
	// The common case is a document that fits the read buffer, and it is
	// returned without a second copy: the builder is only for a line long
	// enough to span two reads.
	chunk, err := reader.ReadString('\n')
	if err == nil || err == io.EOF {
		return keepObject(chunk), err
	}
	if err != bufio.ErrBufferFull {
		return "", err
	}

	var builder strings.Builder
	builder.WriteString(chunk)
	for {
		chunk, err = reader.ReadString('\n')
		if builder.Len()+len(chunk) > maxJSONLine {
			return "", fmt.Errorf("a line is longer than %d bytes, which no document is", maxJSONLine)
		}
		builder.WriteString(chunk)
		if err == nil || err == io.EOF {
			return keepObject(builder.String()), err
		}
		if err != bufio.ErrBufferFull {
			return "", err
		}
	}
}

// keepObject trims a line and returns "" for one the merge drops.
func keepObject(line string) string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "{") {
		return ""
	}
	return line
}

// appendFile copies one export into another verbatim.
//
// io.Copy moves it through a fixed buffer. This was os.ReadFile followed by a
// write of the whole thing, which held a dump of a large table in memory in one
// piece for no reason: nothing between the read and the write looks at it.
func appendFile(path string, out io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	if _, err := io.Copy(out, file); err != nil {
		return fmt.Errorf("copy %s: %w", path, err)
	}
	return nil
}

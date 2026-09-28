package export

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// bigExport writes a JSONL file of about the given size.
func bigExport(tb testing.TB, megabytes int) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "big.json")
	file, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 1<<20)
	payload := strings.Repeat("x", 200)
	for written := 0; written < megabytes<<20; {
		n, err := fmt.Fprintf(writer, "{\"_id\":%d,\"payload\":%q}\n", written, payload)
		if err != nil {
			tb.Fatalf("write: %v", err)
		}
		written += n
	}
	if err := writer.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}
	return path
}

// wholeFileMerge is what the merge used to do, kept here so the difference is
// measured rather than asserted.
func wholeFileMerge(path string, out *bufio.Writer) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	contentStr := strings.TrimSpace(string(content))
	if len(contentStr) == 0 {
		return nil
	}
	for _, line := range strings.Split(contentStr, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && strings.HasPrefix(line, "{") {
			if _, err := out.WriteString(line + "\n"); err != nil {
				return err
			}
		}
	}
	return nil
}

// peakHeap runs merge and reports the largest live heap seen while it ran.
//
// Total allocation is the wrong measure here: the streaming merge allocates
// more of it, in small short-lived pieces, and that is the point. What decides
// whether the process survives is how much is live at once.
func peakHeap(tb testing.TB, merge func(*bufio.Writer) error) uint64 {
	tb.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var peak uint64
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		var stats runtime.MemStats
		for {
			select {
			case <-done:
				return
			default:
			}
			runtime.ReadMemStats(&stats)
			if stats.HeapAlloc > peak {
				peak = stats.HeapAlloc
			}
			time.Sleep(time.Millisecond)
		}
	}()

	writer := bufio.NewWriterSize(io.Discard, 1<<20)
	if err := merge(writer); err != nil {
		tb.Fatal(err)
	}
	writer.Flush()
	close(done)
	<-stopped

	if peak < before.HeapAlloc {
		return 0
	}
	return peak - before.HeapAlloc
}

// TestTheMergeDoesNotHoldTheExportInMemory is the property the change is for.
func TestTheMergeDoesNotHoldTheExportInMemory(t *testing.T) {
	const megabytes = 64
	path := bigExport(t, megabytes)

	streaming := peakHeap(t, func(w *bufio.Writer) error { return appendJSONLines(path, w) })
	wholeFile := peakHeap(t, func(w *bufio.Writer) error { return wholeFileMerge(path, w) })

	t.Logf("%dMB export: streaming peak %.1fMB, whole-file peak %.1fMB",
		megabytes, float64(streaming)/1024/1024, float64(wholeFile)/1024/1024)

	// The streaming merge holds a read buffer and one line. Anything close to
	// the size of the export means it is holding the export.
	if streaming > 16<<20 {
		t.Errorf("the streaming merge held %.1fMB of a %dMB export",
			float64(streaming)/1024/1024, megabytes)
	}
	// And it has to be an actual improvement, not a rename.
	if wholeFile < streaming*2 {
		t.Errorf("the whole-file merge held %.1fMB and the streaming one %.1fMB, "+
			"which is not the difference this change is for",
			float64(wholeFile)/1024/1024, float64(streaming)/1024/1024)
	}
}

func BenchmarkMergeStreaming(b *testing.B) {
	path := bigExport(b, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer := bufio.NewWriterSize(io.Discard, 1<<20)
		if err := appendJSONLines(path, writer); err != nil {
			b.Fatal(err)
		}
		writer.Flush()
	}
}

func BenchmarkMergeWholeFile(b *testing.B) {
	path := bigExport(b, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer := bufio.NewWriterSize(io.Discard, 1<<20)
		if err := wholeFileMerge(path, writer); err != nil {
			b.Fatal(err)
		}
		writer.Flush()
	}
}

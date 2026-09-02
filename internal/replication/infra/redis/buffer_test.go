package redis

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newBuffer(t *testing.T, opts BufferOptions) *Buffer {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	buffer, err := OpenBuffer(opts)
	if err != nil {
		t.Fatalf("OpenBuffer: %v", err)
	}
	t.Cleanup(func() { buffer.Close() })
	return buffer
}

// TestOffsetsAreTheSumOfTheFramesWritten pins the arithmetic the whole design
// leans on: the replication stream is contiguous, so a frame's offset is the
// offset before it plus its own length. Nothing stores an offset per frame, and
// a discrepancy here would silently misplace every position.
func TestOffsetsAreTheSumOfTheFramesWritten(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(1000); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	frames := [][]byte{[]byte("abc"), []byte("de"), []byte("fghij")}
	for _, frame := range frames {
		if err := buffer.Append(frame); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if got, want := buffer.Newest(), int64(1000+3+2+5); got != want {
		t.Errorf("Newest() = %d, want %d", got, want)
	}
	if got, want := buffer.Oldest(), int64(1000); got != want {
		t.Errorf("Oldest() = %d, want %d", got, want)
	}

	cursor, err := buffer.Cursor(1000)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cursor.Close()

	wantEnds := []int64{1003, 1005, 1010}
	for i, want := range wantEnds {
		payload, end, err := cursor.Next(context.Background())
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if string(payload) != string(frames[i]) {
			t.Errorf("frame %d = %q, want %q", i, payload, frames[i])
		}
		if end != want {
			t.Errorf("frame %d ended at %d, want %d", i, end, want)
		}
	}
}

// TestACursorResumesFromAnOffset covers the ordinary restart: the position
// recorded on the target names a stream offset, and reading has to begin there.
func TestACursorResumesFromAnOffset(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	for _, frame := range []string{"aaa", "bbb", "ccc"} {
		if err := buffer.Append([]byte(frame)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	cursor, err := buffer.Cursor(6)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cursor.Close()

	payload, end, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(payload) != "ccc" || end != 9 {
		t.Errorf("resumed with %q ending at %d, want ccc ending at 9", payload, end)
	}
}

// TestAnOffsetInsideAFrameYieldsTheWholeFrame covers a position that does not
// land on a boundary.
//
// Re-delivering a frame is safe — the applier skips what a slot has already
// recorded — while guessing where a command started is not. The buffer therefore
// rewinds rather than seeking blindly.
func TestAnOffsetInsideAFrameYieldsTheWholeFrame(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := buffer.Append([]byte("abcdef")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	cursor, err := buffer.Cursor(3)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cursor.Close()

	payload, _, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(payload) != "abcdef" {
		t.Errorf("got %q, want the whole frame abcdef", payload)
	}
}

// TestAnOffsetOlderThanTheBufferIsRefusedAsTruncated covers the case that must
// never become a silent empty read.
//
// A position the buffer cannot reach means the history is gone. The caller
// repairs by value; what it must not do is treat the gap as "nothing to apply",
// or empty the target and start again.
func TestAnOffsetOlderThanTheBufferIsRefusedAsTruncated(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(5000); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := buffer.Append([]byte("xyz")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	_, err := buffer.Cursor(4000)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("Cursor(4000) = %v, want ErrTruncated", err)
	}
}

// TestAPartialFrameIsDiscardedOnRecovery is the crash case.
//
// A process killed between writing a frame's header and its bytes leaves a
// fragment on disk. Handing that fragment to the reader would deliver half a
// command; recovery therefore truncates to the last frame that is both complete
// and intact.
func TestAPartialFrameIsDiscardedOnRecovery(t *testing.T) {
	dir := t.TempDir()

	buffer := newBuffer(t, BufferOptions{Dir: dir})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	for _, frame := range []string{"one", "two"} {
		if err := buffer.Append([]byte(frame)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := buffer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	whole := buffer.Newest()
	buffer.Close()

	// Simulate the kill: a header and one byte of a frame that never finished.
	segment := filepath.Join(dir, nameForOffset(0))
	file, err := os.OpenFile(segment, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open the segment: %v", err)
	}
	if _, err := file.Write([]byte{0, 0, 0, 9, 1, 2, 3, 4, 'x'}); err != nil {
		t.Fatalf("write a partial frame: %v", err)
	}
	file.Close()

	reopened := newBuffer(t, BufferOptions{Dir: dir})
	if got := reopened.Newest(); got != whole {
		t.Errorf("after recovery Newest() = %d, want %d — the partial frame was kept",
			got, whole)
	}

	// And appending after recovery has to continue the stream, not overwrite.
	if err := reopened.Append([]byte("three")); err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	cursor, err := reopened.Cursor(0)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cursor.Close()

	for _, want := range []string{"one", "two", "three"} {
		payload, _, err := cursor.Next(context.Background())
		if err != nil {
			t.Fatalf("reading %q: %v", want, err)
		}
		if string(payload) != want {
			t.Fatalf("got %q, want %q", payload, want)
		}
	}
}

// TestACorruptFrameIsNotServed covers bit rot rather than a clean kill: the
// checksum is what tells the two apart.
func TestACorruptFrameIsNotServed(t *testing.T) {
	dir := t.TempDir()
	buffer := newBuffer(t, BufferOptions{Dir: dir})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := buffer.Append([]byte("payload")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := buffer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	buffer.Close()

	// Flip a byte inside the payload, leaving the header intact.
	path := filepath.Join(dir, nameForOffset(0))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the segment: %v", err)
	}
	data[frameHeaderBytes] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write the segment: %v", err)
	}

	reopened := newBuffer(t, BufferOptions{Dir: dir})
	if got := reopened.Newest(); got != 0 {
		t.Errorf("Newest() = %d, want 0 — a frame failing its checksum was kept", got)
	}
}

// TestTheStreamContinuesAcrossSegments covers rotation, where a cursor has to
// follow the stream into the next file without a gap.
func TestTheStreamContinuesAcrossSegments(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{SegmentBytes: 16})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	var written []string
	for i := 0; i < 12; i++ {
		frame := "frame" + string(rune('a'+i))
		written = append(written, frame)
		if err := buffer.Append([]byte(frame)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	cursor, err := buffer.Cursor(0)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cursor.Close()

	for i, want := range written {
		payload, _, err := cursor.Next(context.Background())
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if string(payload) != want {
			t.Fatalf("frame %d = %q, want %q", i, payload, want)
		}
	}
}

// TestOldSegmentsAreDiscardedOverTheLimit covers the bound on disk use. The
// buffer is a window, not an archive.
func TestOldSegmentsAreDiscardedOverTheLimit(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{SegmentBytes: 8, MaxBytes: 24})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := buffer.Append([]byte("12345678")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if got := buffer.Held(); got > 40 {
		t.Errorf("Held() = %d, want it trimmed towards the 24 byte limit", got)
	}
	if buffer.Oldest() == 0 {
		t.Error("nothing was discarded, so the buffer would grow without bound")
	}
	// What survives must still be readable from the oldest offset it reports.
	cursor, err := buffer.Cursor(buffer.Oldest())
	if err != nil {
		t.Fatalf("Cursor(Oldest()): %v", err)
	}
	cursor.Close()
}

// TestACursorAtTheEndWaitsForMore covers the live case: the reader blocks rather
// than reporting the end of the stream, because the stream has no end.
func TestACursorAtTheEndWaitsForMore(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	cursor, err := buffer.Cursor(0)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cursor.Close()

	got := make(chan string, 1)
	go func() {
		payload, _, err := cursor.Next(context.Background())
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		got <- string(payload)
	}()

	// Nothing yet.
	select {
	case early := <-got:
		t.Fatalf("Next returned %q before anything was appended", early)
	case <-time.After(50 * time.Millisecond):
	}

	if err := buffer.Append([]byte("arrived")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	select {
	case payload := <-got:
		if payload != "arrived" {
			t.Errorf("Next returned %q, want arrived", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not wake when a frame was appended")
	}
}

// TestAWaitingCursorGivesUpWhenCancelled keeps a blocked reader from outliving
// its task. Without it, stopping a task would leak a goroutine per shard.
func TestAWaitingCursorGivesUpWhenCancelled(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	cursor, err := buffer.Cursor(0)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cursor.Close()

	ctx, cancel := context.WithCancel(context.Background())
	failed := make(chan error, 1)
	go func() {
		_, _, err := cursor.Next(ctx)
		failed <- err
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-failed:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Next returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a waiting cursor did not return when its context was cancelled")
	}
}

func TestAWaitingCursorGivesUpWhenTheBufferCloses(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	cursor, err := buffer.Cursor(0)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cursor.Close()

	done := make(chan error, 1)
	go func() {
		_, _, err := cursor.Next(context.Background())
		done <- err
	}()

	time.Sleep(30 * time.Millisecond)
	buffer.Close()

	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Errorf("Next returned %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a waiting cursor did not return when the buffer closed")
	}
}

// TestResetAbandonsTheOldHistory covers a full resync, which starts a new
// replication id at a new offset. Reading across that seam would splice two
// unrelated histories together.
func TestResetAbandonsTheOldHistory(t *testing.T) {
	dir := t.TempDir()
	buffer := newBuffer(t, BufferOptions{Dir: dir})
	if err := buffer.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := buffer.Append([]byte("old history")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := buffer.Reset(9000); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if got, want := buffer.Oldest(), int64(9000); got != want {
		t.Errorf("Oldest() = %d, want %d", got, want)
	}
	if got, want := buffer.Newest(), int64(9000); got != want {
		t.Errorf("Newest() = %d, want %d", got, want)
	}
	if _, err := buffer.Cursor(0); !errors.Is(err, ErrTruncated) {
		t.Errorf("Cursor(0) = %v, want ErrTruncated after a reset", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the directory: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("the directory holds %d files, want 1 — the old segments were left behind",
			len(entries))
	}
}

// TestAppendingBeforeResetIsRefused keeps a buffer with no starting offset from
// inventing one. The offset only means something relative to the source's
// stream, so it has to come from the handshake.
func TestAppendingBeforeResetIsRefused(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Append([]byte("no starting point")); err == nil {
		t.Error("Append succeeded before Reset, so the frame landed at an invented offset")
	}
}

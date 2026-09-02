package redis

import (
	"errors"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// readerOverBuffer builds a reader whose link reads from a real buffer, which
// is all rate() needs: it measures how far the relay's head has moved.
func readerOverBuffer(t *testing.T) *Reader {
	t.Helper()
	b := newBuffer(t, BufferOptions{})
	// A buffer needs its starting offset before anything may be appended: the
	// stream is contiguous, so a frame's offset is the one before it plus its
	// own length, and there is no such thing as a first frame with no floor.
	if err := b.Reset(0); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	return &Reader{Shard: "0-16383", Link: &link{buffer: b}}
}

// The retention headroom is read by somebody deciding whether there is still
// time to restart the task or whether a fresh copy is now unavoidable.
func TestTheFirstMeasurementRefusesToGuessARate(t *testing.T) {
	r := readerOverBuffer(t)

	_, err := r.rate()
	if err == nil {
		t.Fatal("the first measurement produced a rate out of one sample")
	}
	if !errors.Is(err, domain.ErrWindowNotYet) {
		t.Errorf("error is %v, want one that says to ask again later", err)
	}
}

// TestAQuietSourceHasNoRateRatherThanAnInfiniteWindow.
func TestAQuietSourceHasNoRateRatherThanAnInfiniteWindow(t *testing.T) {
	r := readerOverBuffer(t)

	if _, err := r.rate(); err == nil {
		t.Fatal("the first call answered")
	}
	// Nothing has been appended between the two calls.
	r.sampledAt = time.Now().Add(-time.Second)
	_, err := r.rate()
	if err == nil {
		t.Fatal("a source nobody wrote to produced a rate")
	}
	if !errors.Is(err, domain.ErrWindowNotYet) {
		t.Errorf("error is %v, want one that says to ask again later", err)
	}
}

// TestARateIsTheBytesWrittenOverTheTimeBetweenSamples, which is the arithmetic
// the whole headroom figure rests on.
func TestARateIsTheBytesWrittenOverTheTimeBetweenSamples(t *testing.T) {
	r := readerOverBuffer(t)

	if _, err := r.rate(); err == nil {
		t.Fatal("the first call answered")
	}
	if err := r.Link.buffer.Append(make([]byte, 2000)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Two seconds between the samples, 2,000 bytes written: 1,000 a second.
	r.sampledAt = time.Now().Add(-2 * time.Second)

	rate, err := r.rate()
	if err != nil {
		t.Fatalf("rate: %v", err)
	}
	if rate < 900 || rate > 1100 {
		t.Errorf("rate = %.0f bytes/s, want about 1000", rate)
	}
}

// TestNoTimeBetweenSamplesIsRefused guards the other division by zero, which a
// tight caller can produce.
func TestNoTimeBetweenSamplesIsRefused(t *testing.T) {
	r := readerOverBuffer(t)

	if _, err := r.rate(); err == nil {
		t.Fatal("the first call answered")
	}
	r.sampledAt = time.Now().Add(time.Second) // the clock appears to have gone backwards
	if _, err := r.rate(); err == nil {
		t.Fatal("a rate was produced with no time between samples")
	}
}

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

type fakeChunks struct {
	mu     sync.Mutex
	chunks []Chunk
	i      int
	asked  []string
	err    error
}

func (f *fakeChunks) NextChunk(_ context.Context, _ domain.Namespace, after string, _ int) (Chunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, after)
	if f.err != nil {
		return Chunk{}, f.err
	}
	if f.i >= len(f.chunks) {
		return Chunk{Done: true}, nil
	}
	chunk := f.chunks[f.i]
	f.i++
	return chunk, nil
}

func chunkRow(key string) *domain.Event {
	return &domain.Event{
		NS:      domain.Namespace{DB: "shop", Object: "orders"},
		Op:      domain.OpInsert,
		Key:     key,
		Payload: statementLike{key},
	}
}

type statementLike struct{ key string }

// A chunk read at source time T, applied before a change the source made
// before T that the stream has not delivered yet, would put the record back as
// it was — a silent regression in the middle of a repair.
func TestAChunkWaitsForTheStreamToPassIt(t *testing.T) {
	readAt := time.Now()
	reader := &fakeChunks{chunks: []Chunk{
		{Events: []*domain.Event{chunkRow("a")}, After: "a", ReadAt: readAt},
	}}
	r := &Resync{NS: domain.Namespace{DB: "shop", Object: "orders"}, Reader: reader}

	var streamAt time.Time
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return streamAt }

	emitted := make(chan int, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = r.run(ctx, clock, func(events []*domain.Event) error {
			emitted <- len(events)
			return nil
		})
	}()

	// The stream is behind the chunk, so nothing may be applied yet.
	select {
	case n := <-emitted:
		t.Fatalf("a chunk of %d events was applied before the stream reached it", n)
	case <-time.After(300 * time.Millisecond):
	}

	// The stream passes the chunk's read point.
	mu.Lock()
	streamAt = readAt.Add(time.Second)
	mu.Unlock()

	select {
	case n := <-emitted:
		if n != 1 {
			t.Errorf("applied %d events, want 1", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the chunk was never applied after the stream passed it")
	}
}

// TestAChunkWithNoTimestampIsRefused keeps a re-copy from being applied in an
// order nothing can vouch for.
func TestAChunkWithNoTimestampIsRefused(t *testing.T) {
	reader := &fakeChunks{chunks: []Chunk{
		{Events: []*domain.Event{chunkRow("a")}, After: "a"},
	}}
	r := &Resync{NS: domain.Namespace{DB: "shop", Object: "orders"}, Reader: reader}

	err := r.run(context.Background(), func() time.Time { return time.Now() },
		func([]*domain.Event) error { return nil })
	if err == nil {
		t.Fatal("run accepted a chunk with no source timestamp")
	}
}

// TestAStreamThatNeverReportsItsPositionStopsTheReCopy covers the failure this
// design walked into: a quiet source whose heartbeats carried no timestamp left
// the stream's clock at zero, so the re-copy waited for ever while looking
// exactly like one that was running.
func TestAStreamThatNeverReportsItsPositionStopsTheReCopy(t *testing.T) {
	original := silenceLimit
	silenceLimit = 300 * time.Millisecond
	defer func() { silenceLimit = original }()

	reader := &fakeChunks{chunks: []Chunk{
		{Events: []*domain.Event{chunkRow("a")}, After: "a", ReadAt: time.Now()},
	}}
	r := &Resync{NS: domain.Namespace{DB: "shop", Object: "orders"}, Reader: reader}

	err := r.run(context.Background(),
		func() time.Time { return time.Time{} },
		func([]*domain.Event) error { return nil })

	if err == nil {
		t.Fatal("run waited on a stream that never reported its position")
	}
	if !strings.Contains(err.Error(), "has not reported its position") {
		t.Errorf("error = %v, want it to say the stream is silent", err)
	}
}

// TestABehindButReportingStreamIsWaitedFor is the other side: a stream hours
// behind is still catching up, and giving up on it would abandon a repair that
// would have finished.
func TestABehindButReportingStreamIsWaitedFor(t *testing.T) {
	original := silenceLimit
	silenceLimit = 200 * time.Millisecond
	defer func() { silenceLimit = original }()

	readAt := time.Now()
	reader := &fakeChunks{chunks: []Chunk{
		{Events: []*domain.Event{chunkRow("a")}, After: "a", ReadAt: readAt, Done: true},
	}}
	r := &Resync{NS: domain.Namespace{DB: "shop", Object: "orders"}, Reader: reader}

	var behind = readAt.Add(-time.Hour)
	var mu sync.Mutex
	done := make(chan error, 1)
	go func() {
		done <- r.run(context.Background(),
			func() time.Time { mu.Lock(); defer mu.Unlock(); return behind },
			func([]*domain.Event) error { return nil })
	}()

	// Well past the silence limit, but the stream is reporting, so it is waited
	// for rather than given up on.
	time.Sleep(500 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("run gave up on a stream that was reporting: %v", err)
	default:
	}

	mu.Lock()
	behind = readAt.Add(time.Second)
	mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run never finished after the stream caught up")
	}
}

// TestTheReCopyResumesFromItsProgress means an interrupted repair does not start
// the object again.
func TestTheReCopyResumesFromItsProgress(t *testing.T) {
	store := newStore()
	store.values["resync:shop.orders"] = "k100"

	reader := &fakeChunks{chunks: []Chunk{{Done: true}}}
	r := &Resync{
		NS:          domain.Namespace{DB: "shop", Object: "orders"},
		Reader:      reader,
		Progress:    store,
		ProgressKey: "resync:shop.orders",
	}

	if err := r.run(context.Background(), func() time.Time { return time.Now() },
		func([]*domain.Event) error { return nil }); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(reader.asked) == 0 || reader.asked[0] != "k100" {
		t.Errorf("the first chunk was asked for after %q, want k100", reader.asked)
	}
}

// TestProgressMovesOnlyAfterTheChunkIsHandedOver means an interrupted re-copy
// repeats a chunk rather than skipping one. Repeating is safe; skipping is not.
func TestProgressMovesOnlyAfterTheChunkIsHandedOver(t *testing.T) {
	store := newStore()
	readAt := time.Now()
	reader := &fakeChunks{chunks: []Chunk{
		{Events: []*domain.Event{chunkRow("a")}, After: "a", ReadAt: readAt},
	}}
	r := &Resync{
		NS:          domain.Namespace{DB: "shop", Object: "orders"},
		Reader:      reader,
		Progress:    store,
		ProgressKey: "resync:shop.orders",
	}

	handOverFailed := errors.New("the queue closed")
	err := r.run(context.Background(),
		func() time.Time { return readAt.Add(time.Second) },
		func([]*domain.Event) error { return handOverFailed })

	if !errors.Is(err, handOverFailed) {
		t.Fatalf("run returned %v, want the hand-over failure", err)
	}
	if got := store.value("resync:shop.orders"); got != "" {
		t.Errorf("progress = %q after a failed hand-over, want none", got)
	}
}

// TestAChunkThatReturnsNothingAndNoEndIsRefused keeps a misbehaving reader from
// looping for ever.
func TestAChunkThatReturnsNothingAndNoEndIsRefused(t *testing.T) {
	reader := &fakeChunks{chunks: []Chunk{{}}}
	r := &Resync{NS: domain.Namespace{DB: "shop", Object: "orders"}, Reader: reader}

	err := r.run(context.Background(), func() time.Time { return time.Now() },
		func([]*domain.Event) error { return nil })
	if err == nil {
		t.Fatal("run accepted a chunk that held nothing and reported no end")
	}
}

// TestTheReCopyNeverMovesTheStreamsPosition is what keeps a repair from being
// mistaken for progress through the log.
func TestTheReCopyNeverMovesTheStreamsPosition(t *testing.T) {
	readAt := time.Now()
	reader := &fakeChunks{chunks: []Chunk{
		{Events: []*domain.Event{chunkRow("a"), chunkRow("b")}, After: "b", ReadAt: readAt, Done: true},
	}}
	applier := &fakeApplier{}
	store := newStore()

	// A heartbeat is what tells the re-copy where the stream has got to. Its
	// timestamp is the source's own clock, and it means "everything up to here
	// has been delivered".
	stream := &fakeReader{events: []*domain.Event{
		{Heartbeat: true, EndsTransaction: true, SourceTime: time.Now()},
	}}
	r := newRunner(t, stream, applier, store)
	r.Resyncs = []*Resync{{
		NS:     domain.Namespace{DB: "shop", Object: "orders"},
		Reader: reader,
	}}
	reader.chunks[0].ReadAt = time.Now().Add(-time.Hour)

	if err := runFor(t, r, 250*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(applier.applied()); got == 0 {
		t.Fatal("the re-copy applied nothing")
	}
	if got := store.value(""); got != "" {
		t.Errorf("the stream's position moved to %q because of a re-copy", got)
	}
}

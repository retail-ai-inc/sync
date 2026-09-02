package redis

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The replication stream, written to disk on the way through.
//
// A Redis master keeps its backlog in memory, one megabyte by default, and a
// replica that falls further behind gets a full resync — tens of gigabytes
// across regions, into a target that has to be emptied first. Buffering to disk
// turns "the link was down for twenty minutes" into a partial resync, and is
// the whole reason this relay exists rather than a replica pointed at the
// source.
//
// The stream is a contiguous byte sequence, so offsets are arithmetic: a segment
// records the absolute offset it starts at and every frame accounts for its own
// length. Nothing stores an offset per frame.

// ErrTruncated says the requested offset is older than anything still held.
//
// It is not a failure to retry: the bytes are gone. The caller repairs by value
// instead, which is what keeps a lost position from turning into an emptied
// target.
var ErrTruncated = errors.New("the replication buffer no longer reaches back that far")

const (
	frameHeaderBytes    = 8
	defaultSegmentBytes = 64 << 20
	defaultMaxBytes     = 8 << 30
	segmentSuffix       = ".log"
	// maxFrameBytes bounds what a single frame may claim, so a corrupt header
	// cannot make recovery allocate wildly.
	maxFrameBytes = 512 << 20
)

type BufferOptions struct {
	// Dir holds the segments. One directory per source shard.
	Dir string
	// MaxBytes is roughly how much history to keep. Zero means the default.
	MaxBytes int64
	// MaxAge discards segments older than this regardless of size. Zero means
	// no age limit.
	MaxAge time.Duration
	// SegmentBytes is the size a segment grows to before the next one starts.
	// Zero means the default.
	SegmentBytes int64
}

func (o BufferOptions) maxBytes() int64 {
	if o.MaxBytes > 0 {
		return o.MaxBytes
	}
	return defaultMaxBytes
}

func (o BufferOptions) segmentBytes() int64 {
	if o.SegmentBytes > 0 {
		return o.SegmentBytes
	}
	return defaultSegmentBytes
}

// Buffer is the on-disk replication stream for one source shard.
//
// One writer appends; any number of cursors read, blocking when they reach the
// end and waking when more arrives.
type Buffer struct {
	opts BufferOptions

	mu sync.Mutex
	// grown is broadcast when bytes are appended or the buffer is closed, so a
	// cursor waiting at the end wakes.
	grown *sync.Cond

	segments []*segment
	active   *segment
	closed   bool
	// sealed says the writer has finished, so a cursor that reaches the end has
	// reached the end for good rather than being merely up to date. Without it, a
	// connection that dies while a cursor is waiting leaves the cursor waiting
	// for ever: the condition it blocks on is only signalled by an append, and
	// there will not be another one.
	sealed bool
}

type segment struct {
	path string
	// start is the absolute stream offset of the segment's first byte.
	start int64
	// end is the absolute stream offset after its last complete frame.
	end int64

	// overhead is the framing bytes already on disk ahead of end: one header
	// per intact frame. A segment's file is longer than the stream bytes it
	// holds, so recovery needs this to truncate to a byte position.
	overhead int64

	file *os.File
	// dirty says something has been written since the last fsync.
	dirty bool
}

func (s *segment) fileBytes() int64 { return s.end - s.start + s.overhead }

func (s *segment) bytes() int64 { return s.end - s.start }

// OpenBuffer opens or recovers the buffer in a directory.
//
// Recovery walks the newest segment and truncates it at the last complete,
// intact frame. A process killed mid-append leaves a partial frame; keeping it
// would hand the reader a fragment of a command.
func OpenBuffer(opts BufferOptions) (*Buffer, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("the replication buffer needs a directory")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create the buffer directory: %w", err)
	}

	b := &Buffer{opts: opts}
	b.grown = sync.NewCond(&b.mu)

	names, err := segmentNames(opts.Dir)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		start, err := offsetFromName(name)
		if err != nil {
			return nil, err
		}
		seg := &segment{path: filepath.Join(opts.Dir, name), start: start}
		end, overhead, err := scanSegment(seg.path, start)
		if err != nil {
			return nil, err
		}
		seg.end, seg.overhead = end, overhead
		b.segments = append(b.segments, seg)
	}

	if len(b.segments) > 0 {
		last := b.segments[len(b.segments)-1]
		// Drop whatever followed the last intact frame.
		if err := os.Truncate(last.path, last.fileBytes()); err != nil {
			return nil, fmt.Errorf("truncate the partial tail of %s: %w", last.path, err)
		}
		file, err := os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("reopen %s for appending: %w", last.path, err)
		}
		last.file = file
		b.active = last
	}
	return b, nil
}

// Append writes one frame: a contiguous run of stream bytes.
//
// A frame is whatever the caller chose to hand over — for this package, one
// parsed command, so a cursor never sees half of one.
func (b *Buffer) Append(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if len(payload) > maxFrameBytes {
		return fmt.Errorf("a %d byte frame is larger than the %d byte limit",
			len(payload), maxFrameBytes)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("the replication buffer is closed")
	}
	if b.active == nil {
		return fmt.Errorf("the replication buffer has no starting offset; call Reset first")
	}
	if b.active.bytes() >= b.opts.segmentBytes() {
		if err := b.rotate(); err != nil {
			return err
		}
	}

	var header [frameHeaderBytes]byte
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	if _, err := b.active.file.Write(header[:]); err != nil {
		return fmt.Errorf("append a frame header: %w", err)
	}
	if _, err := b.active.file.Write(payload); err != nil {
		return fmt.Errorf("append a frame: %w", err)
	}
	b.active.end += int64(len(payload))
	b.active.overhead += frameHeaderBytes
	b.active.dirty = true

	b.grown.Broadcast()
	return b.trimLocked()
}

// Seal says no more will be appended, waking anything waiting at the end.
//
// It is called when the connection filling the buffer stops, for any reason. The
// buffer itself stays readable and on disk.
func (b *Buffer) Seal() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sealed = true
	b.grown.Broadcast()
}

// Sync flushes the active segment to disk.
//
// Called once per applied batch rather than once per frame: the point of the
// buffer is that a crash does not cost a full resync, and one fsync per batch
// bounds the loss to that batch — which is re-read from the source anyway.
func (b *Buffer) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == nil || !b.active.dirty {
		return nil
	}
	if err := b.active.file.Sync(); err != nil {
		return fmt.Errorf("flush the replication buffer: %w", err)
	}
	b.active.dirty = false
	return nil
}

// Reset discards everything and starts again at an offset.
//
// A full resync gives a new replication id and a new offset, so the bytes
// already held belong to a history the source no longer continues. Keeping them
// would let a cursor read across the seam.
func (b *Buffer) Reset(start int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.active != nil {
		if err := b.active.file.Close(); err != nil {
			return fmt.Errorf("close the active segment: %w", err)
		}
	}
	for _, seg := range b.segments {
		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", seg.path, err)
		}
	}
	b.segments = nil
	b.active = nil

	seg, err := b.createSegment(start)
	if err != nil {
		return err
	}
	b.segments = []*segment{seg}
	b.active = seg
	b.sealed = false
	b.grown.Broadcast()
	return nil
}

func (b *Buffer) Oldest() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.segments) == 0 {
		return 0
	}
	return b.segments[0].start
}

func (b *Buffer) Newest() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == nil {
		return 0
	}
	return b.active.end
}

func (b *Buffer) Held() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var total int64
	for _, seg := range b.segments {
		total += seg.bytes()
	}
	return total
}

func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.grown.Broadcast()
	if b.active == nil {
		return nil
	}
	err := b.active.file.Close()
	b.active.file = nil
	return err
}

func (b *Buffer) rotate() error {
	if err := b.active.file.Sync(); err != nil {
		return fmt.Errorf("flush before rotating: %w", err)
	}
	if err := b.active.file.Close(); err != nil {
		return fmt.Errorf("close before rotating: %w", err)
	}
	b.active.file = nil

	seg, err := b.createSegment(b.active.end)
	if err != nil {
		return err
	}
	b.segments = append(b.segments, seg)
	b.active = seg
	return nil
}

func (b *Buffer) createSegment(start int64) (*segment, error) {
	path := filepath.Join(b.opts.Dir, nameForOffset(start))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	return &segment{path: path, start: start, end: start, file: file}, nil
}

// trimLocked drops whole segments from the old end once the buffer is over its
// limits. The active segment is never dropped.
func (b *Buffer) trimLocked() error {
	var total int64
	for _, seg := range b.segments {
		total += seg.bytes()
	}
	limit := b.opts.maxBytes()

	for len(b.segments) > 1 && total > limit {
		oldest := b.segments[0]
		if err := os.Remove(oldest.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("discard %s: %w", oldest.path, err)
		}
		total -= oldest.bytes()
		b.segments = b.segments[1:]
	}

	if b.opts.MaxAge <= 0 {
		return nil
	}
	cutoff := time.Now().Add(-b.opts.MaxAge)
	for len(b.segments) > 1 {
		oldest := b.segments[0]
		info, err := os.Stat(oldest.path)
		if err != nil || info.ModTime().After(cutoff) {
			return nil
		}
		if err := os.Remove(oldest.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("discard %s: %w", oldest.path, err)
		}
		b.segments = b.segments[1:]
	}
	return nil
}

type Cursor struct {
	buffer *Buffer

	file   *os.File
	reader *bufio.Reader
	// segment is which segment file is open, and offset the stream position the
	// next frame starts at.
	segment *segment
	offset  int64
}

// Cursor opens a reader positioned at an offset.
//
// An offset inside a frame rather than on its boundary yields that whole frame,
// so the caller may see a little already-applied history. That is deliberate:
// the applier skips what a slot has already recorded, and re-reading is always
// safer than guessing where a command started.
func (b *Buffer) Cursor(offset int64) (*Cursor, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.segments) == 0 {
		return nil, ErrTruncated
	}
	if offset < b.segments[0].start {
		return nil, fmt.Errorf("%w: asked for %d, holding from %d",
			ErrTruncated, offset, b.segments[0].start)
	}
	if offset > b.active.end {
		return nil, fmt.Errorf("asked for offset %d but the buffer only reaches %d",
			offset, b.active.end)
	}

	// The segment holding the offset is the last one starting at or before it.
	index := 0
	for i, seg := range b.segments {
		if seg.start <= offset {
			index = i
		}
	}
	cursor := &Cursor{buffer: b, segment: b.segments[index]}
	if err := cursor.openSegment(b.segments[index]); err != nil {
		return nil, err
	}
	if err := cursor.skipTo(offset); err != nil {
		cursor.Close()
		return nil, err
	}
	return cursor, nil
}

func (c *Cursor) openSegment(seg *segment) error {
	if c.file != nil {
		c.file.Close()
	}
	file, err := os.Open(seg.path)
	if err != nil {
		return fmt.Errorf("open %s: %w", seg.path, err)
	}
	c.file = file
	c.reader = bufio.NewReaderSize(file, 256<<10)
	c.segment = seg
	c.offset = seg.start
	return nil
}

func (c *Cursor) skipTo(offset int64) error {
	for c.offset < offset {
		length, err := c.peekLength()
		if err != nil {
			return err
		}
		if c.offset+int64(length) > offset {
			// The offset falls inside this frame, so it is where we start.
			return nil
		}
		if _, err := c.reader.Discard(frameHeaderBytes + length); err != nil {
			return fmt.Errorf("skip a frame in %s: %w", c.segment.path, err)
		}
		c.offset += int64(length)
	}
	return nil
}

func (c *Cursor) peekLength() (int, error) {
	header, err := c.reader.Peek(frameHeaderBytes)
	if err != nil {
		return 0, fmt.Errorf("read a frame header in %s: %w", c.segment.path, err)
	}
	length := int(binary.BigEndian.Uint32(header[0:4]))
	if length <= 0 || length > maxFrameBytes {
		return 0, fmt.Errorf("%s claims a %d byte frame", c.segment.path, length)
	}
	return length, nil
}

// Next returns the next frame and the stream offset after it.
//
// It blocks while the cursor is at the end of the stream, waking when the writer
// appends or the buffer closes.
func (c *Cursor) Next(ctx context.Context) ([]byte, int64, error) {
	for {
		payload, end, err := c.read()
		switch {
		case err == nil:
			return payload, end, nil
		case !errors.Is(err, io.EOF):
			return nil, 0, err
		}

		advanced, err := c.waitOrAdvance(ctx)
		if err != nil {
			return nil, 0, err
		}
		if advanced {
			continue
		}
	}
}

// read pulls one frame, returning io.EOF when the cursor is at the end of the
// segment's written bytes.
func (c *Cursor) read() ([]byte, int64, error) {
	c.buffer.mu.Lock()
	limit := c.segment.end
	c.buffer.mu.Unlock()

	if c.offset >= limit {
		return nil, 0, io.EOF
	}

	var header [frameHeaderBytes]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return nil, 0, fmt.Errorf("read a frame header in %s: %w", c.segment.path, err)
	}
	length := int(binary.BigEndian.Uint32(header[0:4]))
	sum := binary.BigEndian.Uint32(header[4:8])
	if length <= 0 || length > maxFrameBytes {
		return nil, 0, fmt.Errorf("%s claims a %d byte frame", c.segment.path, length)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return nil, 0, fmt.Errorf("read a frame in %s: %w", c.segment.path, err)
	}
	if crc32.ChecksumIEEE(payload) != sum {
		return nil, 0, fmt.Errorf("a frame in %s at offset %d failed its checksum",
			c.segment.path, c.offset)
	}
	c.offset += int64(length)
	return payload, c.offset, nil
}

// waitOrAdvance moves to the next segment when one exists, and otherwise waits
// for the writer. It reports whether the cursor can read again.
func (c *Cursor) waitOrAdvance(ctx context.Context) (bool, error) {
	c.buffer.mu.Lock()

	next := c.nextSegmentLocked()
	if next != nil {
		c.buffer.mu.Unlock()
		return true, c.openSegment(next)
	}
	if c.buffer.closed || c.buffer.sealed {
		c.buffer.mu.Unlock()
		return false, io.EOF
	}
	if c.offset < c.segment.end {
		c.buffer.mu.Unlock()
		return true, nil
	}

	// Wake the wait when the context is cancelled: sync.Cond cannot select.
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		c.buffer.mu.Lock()
		c.buffer.grown.Broadcast()
		c.buffer.mu.Unlock()
		close(done)
	})
	c.buffer.grown.Wait()
	c.buffer.mu.Unlock()
	if !stop() {
		<-done
		return false, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

// nextSegmentLocked returns the segment after the one being read, if the cursor
// has reached the end of it and another exists.
func (c *Cursor) nextSegmentLocked() *segment {
	if c.offset < c.segment.end {
		return nil
	}
	for i, seg := range c.buffer.segments {
		if seg == c.segment && i+1 < len(c.buffer.segments) {
			return c.buffer.segments[i+1]
		}
	}
	return nil
}

func (c *Cursor) Offset() int64 { return c.offset }

func (c *Cursor) Close() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	return err
}

func segmentNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read the buffer directory: %w", err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), segmentSuffix) {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
}

func nameForOffset(offset int64) string {
	return fmt.Sprintf("%020d%s", offset, segmentSuffix)
}

func offsetFromName(name string) (int64, error) {
	trimmed := strings.TrimSuffix(name, segmentSuffix)
	offset, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is not a segment name: %w", name, err)
	}
	return offset, nil
}

// scanSegment walks a segment's frames and reports the stream offset after the
// last intact one, recording how much framing overhead precedes it.
//
// It stops at the first frame that is short or fails its checksum, which is what
// a process killed mid-append leaves behind.
func scanSegment(path string, start int64) (end, overhead int64, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 256<<10)
	end = start

	for {
		var header [frameHeaderBytes]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			return end, overhead, nil
		}
		length := int(binary.BigEndian.Uint32(header[0:4]))
		sum := binary.BigEndian.Uint32(header[4:8])
		if length <= 0 || length > maxFrameBytes {
			return end, overhead, nil
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return end, overhead, nil
		}
		if crc32.ChecksumIEEE(payload) != sum {
			return end, overhead, nil
		}
		end += int64(length)
		overhead += frameHeaderBytes
	}
}

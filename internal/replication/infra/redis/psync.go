package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The replication protocol, spoken from the replica's side.
//
// This is what makes the relay a replica rather than a client watching for
// changes. Keyspace notifications, the obvious alternative, are published with
// no acknowledgement, no replay and no position: a subscriber that misses a
// window has no way to learn what it missed. The replication stream has all
// three, and the commands in it have already been rewritten by the master into
// deterministic form — SPOP arrives as SREM of the member that was actually
// removed, EXPIRE as PEXPIREAT of an absolute time — so what arrives is what
// the master's own replicas apply.

// Point is a position in a master's replication stream.
//
// ReplID identifies the history the offset belongs to. A master that has been
// failed over continues a different history, so an offset without its
// replication id is meaningless.
type Point struct {
	ReplID string
	Offset int64
}

func (p Point) IsZero() bool { return p.ReplID == "" }

type Handshake struct {
	// Full says the master would not continue from the offset asked for and is
	// sending its whole data set instead.
	Full bool
	// ReplID is the history the stream now belongs to.
	ReplID string
	// Offset is where the stream starts.
	Offset int64
}

type Command struct {
	// Args is the command and its arguments, as the master sent them.
	Args [][]byte
	// Raw is the bytes it occupied in the stream. The offset arithmetic and the
	// buffer both work in these, so they are kept exactly as received.
	Raw []byte
	// End is the absolute stream offset after this command.
	End int64
}

func (c *Command) Name() string {
	if len(c.Args) == 0 {
		return ""
	}
	return strings.ToUpper(string(c.Args[0]))
}

type StreamOptions struct {
	// Addr is the master to replicate from, as host:port.
	Addr string
	// Username and Password authenticate. An empty username with a password
	// sends the old two-argument AUTH.
	Username string
	Password string
	// DialTimeout bounds connecting. Zero means the default.
	DialTimeout time.Duration
	// IdleTimeout is how long the master may say nothing before the link is
	// treated as dead. A healthy master pings every repl-ping-replica-period,
	// ten seconds by default, so silence past this is not quietness — it is a
	// connection that has gone away without saying so. Zero means the default.
	IdleTimeout time.Duration
	// ListeningPort is reported to the master so it appears in INFO replication.
	// Zero is allowed and means this replica serves nothing.
	ListeningPort int
}

const (
	defaultDialTimeout = 10 * time.Second
	defaultIdleTimeout = 60 * time.Second
	// ackPeriod is how often the applied offset is reported back. One second is
	// what a real replica uses.
	ackPeriod = time.Second
)

func (o StreamOptions) dialTimeout() time.Duration {
	if o.DialTimeout > 0 {
		return o.DialTimeout
	}
	return defaultDialTimeout
}

func (o StreamOptions) idleTimeout() time.Duration {
	if o.IdleTimeout > 0 {
		return o.IdleTimeout
	}
	return defaultIdleTimeout
}

// Stream is one replication connection to one master.
//
// It is not safe for concurrent readers. Ack may be called from another
// goroutine.
type Stream struct {
	opts StreamOptions

	conn   net.Conn
	reader *bufio.Reader

	// writing guards sending to the master, because acknowledgements are sent
	// from the applying side while this side is reading.
	writing sync.Mutex

	// offset is how far the stream has been read.
	offset int64
	// raw accumulates the bytes of the command being read.
	raw []byte

	stopWatch func() bool
	closeOnce sync.Once
}

func Dial(ctx context.Context, opts StreamOptions) (*Stream, error) {
	if opts.Addr == "" {
		return nil, fmt.Errorf("no address to replicate from")
	}
	dialer := net.Dialer{Timeout: opts.dialTimeout()}
	conn, err := dialer.DialContext(ctx, "tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", opts.Addr, err)
	}

	s := &Stream{opts: opts, conn: conn, reader: bufio.NewReaderSize(conn, 256<<10)}
	// Closing the connection is the only way to interrupt a blocking read.
	s.stopWatch = context.AfterFunc(ctx, func() { conn.Close() })

	if err := s.authenticate(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Stream) authenticate() error {
	if s.opts.Password == "" {
		return nil
	}
	args := [][]byte{[]byte("AUTH")}
	if s.opts.Username != "" {
		args = append(args, []byte(s.opts.Username))
	}
	args = append(args, []byte(s.opts.Password))

	reply, err := s.call(args...)
	if err != nil {
		return fmt.Errorf("authenticate with the source: %w", err)
	}
	if !strings.HasPrefix(reply, "+OK") {
		// Wrong credentials will never start working, so this is not worth
		// retrying against.
		return domain.Unrecoverable("the source refused the credentials: %s", reply)
	}
	return nil
}

// Sync performs the replication handshake and reports what the master agreed to.
//
// On a full resync the caller must consume the data set with SkipRDB before
// reading commands; the stream is not positioned until it has.
func (s *Stream) Sync(from Point) (Handshake, error) {
	if _, err := s.call([]byte("REPLCONF"), []byte("listening-port"),
		[]byte(strconv.Itoa(s.opts.ListeningPort))); err != nil {
		return Handshake{}, fmt.Errorf("announce the listening port: %w", err)
	}
	// eof asks for the data set without a length prefix, which is how a master
	// streams it straight from the fork instead of writing a file first. psync2
	// is what allows a partial resync to survive the source failing over.
	if _, err := s.call([]byte("REPLCONF"), []byte("capa"), []byte("eof"),
		[]byte("capa"), []byte("psync2")); err != nil {
		return Handshake{}, fmt.Errorf("announce capabilities: %w", err)
	}

	id, offset := "?", "-1"
	if !from.IsZero() {
		// The protocol asks for the first byte wanted, which is one past what
		// has been read.
		id, offset = from.ReplID, strconv.FormatInt(from.Offset+1, 10)
	}
	reply, err := s.call([]byte("PSYNC"), []byte(id), []byte(offset))
	if err != nil {
		return Handshake{}, fmt.Errorf("start replication: %w", err)
	}
	// A master waiting for its background save to start sends newlines to keep
	// the connection alive, and they arrive before the answer to PSYNC rather
	// than only before the data set. Reading one line and believing it is the
	// reply is how this fails against a real server while passing against a
	// scripted one.
	for reply == "" {
		if reply, err = s.readLine(); err != nil {
			return Handshake{}, fmt.Errorf("wait for the answer to PSYNC: %w", err)
		}
	}

	switch {
	case strings.HasPrefix(reply, "+FULLRESYNC"):
		fields := strings.Fields(reply)
		if len(fields) < 3 {
			return Handshake{}, fmt.Errorf("the source answered PSYNC with %q", reply)
		}
		at, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return Handshake{}, fmt.Errorf("the source gave a full resync offset of %q", fields[2])
		}
		s.offset = at
		return Handshake{Full: true, ReplID: fields[1], Offset: at}, nil

	case strings.HasPrefix(reply, "+CONTINUE"):
		// The stream picks up exactly where the position said, so the count of
		// bytes read stays as it was.
		s.offset = from.Offset
		replID := from.ReplID
		if fields := strings.Fields(reply); len(fields) >= 2 {
			// psync2: the master may hand over a new identifier for the same
			// history, which the position has to record from now on.
			replID = fields[1]
		}
		return Handshake{ReplID: replID, Offset: from.Offset}, nil

	case strings.HasPrefix(reply, "-NOMASTERLINK"), strings.HasPrefix(reply, "-LOADING"):
		return Handshake{}, fmt.Errorf("the source is not ready to replicate yet: %s", reply)

	case strings.HasPrefix(reply, "-ERR") && strings.Contains(reply, "not allowed"):
		// Managed Redis disables the replication commands outright. No amount of
		// retrying changes that, and the fallback is a different reader.
		return Handshake{}, domain.Unrecoverable(
			"the source does not allow PSYNC (%s). A managed instance blocks the "+
				"replication commands; use the scanning reader or the provider's own "+
				"cross-region replication instead", reply)
	}
	return Handshake{}, fmt.Errorf("the source answered PSYNC with %q", reply)
}

// SkipRDB consumes the data set a full resync sends, discarding it.
//
// Nothing here parses it. The format changes with almost every Redis release —
// new encodings for hashes, lists, streams and the bundled module types — and a
// parser that has to keep up with them is a parser that silently misreads the
// day it falls behind. The first copy is taken with SCAN and DUMP instead, which
// speaks only stable commands, and the fuzziness that introduces is resolved by
// repairing keys by value until the stream has passed it.
//
// The bytes still have to be read: they are on the wire, and the command stream
// is behind them. They do not count towards the offset.
func (s *Stream) SkipRDB(ctx context.Context) (int64, error) {
	for {
		line, err := s.readLine()
		if err != nil {
			return 0, fmt.Errorf("read the data set header: %w", err)
		}
		if line == "" {
			// A newline on its own: the master saying it is still forking.
			continue
		}
		if !strings.HasPrefix(line, "$") {
			return 0, fmt.Errorf("expected the data set, got %q", line)
		}

		var read int64
		if marker, ok := strings.CutPrefix(line, "$EOF:"); ok {
			read, err = s.skipDelimited(ctx, []byte(marker))
		} else {
			size, sizeErr := strconv.ParseInt(line[1:], 10, 64)
			if sizeErr != nil {
				return 0, fmt.Errorf("the data set announced a length of %q", line[1:])
			}
			read, err = s.skipLength(ctx, size)
		}
		if err != nil {
			return read, err
		}

		// The master will not start sending commands until the replica
		// acknowledges. A data set streamed without a length gives the master no
		// way to know when the replica finished loading it, so it holds the
		// command stream back until the first REPLCONF ACK arrives. Without this
		// the connection stays open, the master's offset climbs, and nothing is
		// ever delivered — which looks exactly like a quiet source.
		if err := s.Ack(s.offset); err != nil {
			return read, fmt.Errorf("acknowledge the data set: %w", err)
		}
		return read, nil
	}
}

func (s *Stream) skipLength(ctx context.Context, size int64) (int64, error) {
	var read int64
	chunk := make([]byte, 64<<10)
	for read < size {
		if err := ctx.Err(); err != nil {
			return read, err
		}
		want := int64(len(chunk))
		if remaining := size - read; remaining < want {
			want = remaining
		}
		if err := s.deadline(); err != nil {
			return read, err
		}
		n, err := s.reader.Read(chunk[:want])
		if n > 0 {
			read += int64(n)
		}
		if err != nil {
			return read, fmt.Errorf("read the data set: %w", err)
		}
	}
	return read, nil
}

// skipDelimited discards a data set streamed without a length, which ends with
// the forty-byte marker the header announced.
func (s *Stream) skipDelimited(ctx context.Context, marker []byte) (int64, error) {
	var read int64
	window := make([]byte, 0, len(marker))

	for {
		if err := ctx.Err(); err != nil {
			return read, err
		}
		if err := s.deadline(); err != nil {
			return read, err
		}
		b, err := s.reader.ReadByte()
		if err != nil {
			return read, fmt.Errorf("read the data set: %w", err)
		}
		read++

		if len(window) == len(marker) {
			window = append(window[:0], window[1:]...)
		}
		window = append(window, b)
		if len(window) == len(marker) && string(window) == string(marker) {
			return read - int64(len(marker)), nil
		}
	}
}

// Next reads one command from the stream.
//
// The bytes are kept exactly as they arrived: the offset is a count of them, and
// so is everything the source will accept in an acknowledgement.
func (s *Stream) Next(ctx context.Context) (*Command, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.raw = s.raw[:0]

	for {
		b, err := s.peek()
		if err != nil {
			return nil, err
		}
		if b == '\n' || b == '\r' {
			// A keepalive, sent while the master was forking. It is written
			// straight to the connection rather than through the replication
			// backlog, so the master does not count it either — counting it here
			// would drift this side's offset ahead of the master's for good.
			if _, err := s.reader.Discard(1); err != nil {
				return nil, s.describe(err)
			}
			s.raw = s.raw[:0]
			continue
		}
		if b != '*' {
			return nil, fmt.Errorf("expected a command in the replication stream, "+
				"got a byte %q at offset %d", b, s.offset)
		}
		break
	}

	line, err := s.readLineRaw()
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil || count < 0 {
		return nil, fmt.Errorf("a command in the stream announced %q arguments", line[1:])
	}

	// Where each argument sits inside the raw bytes. The arguments are handed
	// out as slices of the copy taken below, never of the scratch buffer: that
	// gets reused for the next command, and an argument pointing into it would
	// change under the caller's feet.
	type span struct{ at, length int }
	spans := make([]span, 0, count)

	for i := 0; i < count; i++ {
		header, err := s.readLineRaw()
		if err != nil {
			return nil, err
		}
		if len(header) == 0 || header[0] != '$' {
			return nil, fmt.Errorf("expected an argument length, got %q", header)
		}
		size, err := strconv.Atoi(header[1:])
		if err != nil || size < 0 {
			return nil, fmt.Errorf("an argument announced a length of %q", header[1:])
		}
		at := len(s.raw)
		// The argument and the CRLF that follows it.
		if _, err := s.readRaw(size + 2); err != nil {
			return nil, err
		}
		spans = append(spans, span{at: at, length: size})
	}

	raw := make([]byte, len(s.raw))
	copy(raw, s.raw)
	args := make([][]byte, 0, count)
	for _, sp := range spans {
		args = append(args, raw[sp.at:sp.at+sp.length])
	}

	s.offset += int64(len(raw))
	return &Command{Args: args, Raw: raw, End: s.offset}, nil
}

func (s *Stream) Offset() int64 { return s.offset }

// Ack reports an offset back to the master.
//
// A real replica sends this every second, and the master uses it for WAIT, for
// min-replicas-to-write and for choosing which replica to promote. The offset
// reported here is the one written to the buffer rather than the one applied to
// the target: once it is on disk it will be applied, and reporting less would
// let the master purge history this relay still needs.
func (s *Stream) Ack(offset int64) error {
	s.writing.Lock()
	defer s.writing.Unlock()
	return s.write([][]byte{[]byte("REPLCONF"), []byte("ACK"),
		[]byte(strconv.FormatInt(offset, 10))})
}

func (s *Stream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.stopWatch != nil {
			s.stopWatch()
		}
		err = s.conn.Close()
	})
	return err
}

// ------------------------------------------------------------------ plumbing

func (s *Stream) call(args ...[]byte) (string, error) {
	s.writing.Lock()
	err := s.write(args)
	s.writing.Unlock()
	if err != nil {
		return "", err
	}
	return s.readLine()
}

func (s *Stream) write(args [][]byte) error {
	var out []byte
	out = append(out, '*')
	out = strconv.AppendInt(out, int64(len(args)), 10)
	out = append(out, '\r', '\n')
	for _, arg := range args {
		out = append(out, '$')
		out = strconv.AppendInt(out, int64(len(arg)), 10)
		out = append(out, '\r', '\n')
		out = append(out, arg...)
		out = append(out, '\r', '\n')
	}
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.opts.dialTimeout())); err != nil {
		return err
	}
	if _, err := s.conn.Write(out); err != nil {
		return fmt.Errorf("send to the source: %w", err)
	}
	return nil
}

func (s *Stream) deadline() error {
	return s.conn.SetReadDeadline(time.Now().Add(s.opts.idleTimeout()))
}

// readLine reads one CRLF-terminated line without counting it, for the
// handshake and the data set header.
func (s *Stream) readLine() (string, error) {
	if err := s.deadline(); err != nil {
		return "", err
	}
	line, err := s.reader.ReadString('\n')
	if err != nil {
		return "", s.describe(err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (s *Stream) readLineRaw() (string, error) {
	if err := s.deadline(); err != nil {
		return "", err
	}
	line, err := s.reader.ReadString('\n')
	if err != nil {
		return "", s.describe(err)
	}
	s.raw = append(s.raw, line...)
	return strings.TrimRight(line, "\r\n"), nil
}

func (s *Stream) readRaw(n int) ([]byte, error) {
	if err := s.deadline(); err != nil {
		return nil, err
	}
	start := len(s.raw)
	s.raw = append(s.raw, make([]byte, n)...)
	if _, err := io.ReadFull(s.reader, s.raw[start:]); err != nil {
		s.raw = s.raw[:start]
		return nil, s.describe(err)
	}
	return s.raw[start:], nil
}

func (s *Stream) peek() (byte, error) {
	if err := s.deadline(); err != nil {
		return 0, err
	}
	head, err := s.reader.Peek(1)
	if err != nil {
		return 0, s.describe(err)
	}
	return head[0], nil
}

func (s *Stream) describe(err error) error {
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return fmt.Errorf("the source said nothing for %s. A master pings its "+
			"replicas every ten seconds by default, so this is a connection that "+
			"has gone away rather than a quiet source: %w", s.opts.idleTimeout(), err)
	}
	return fmt.Errorf("read from the source: %w", err)
}

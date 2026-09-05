package redis

import (
	"bufio"
	"context"
	"crypto/tls"
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

// The replication protocol from the replica's side, which is what makes this a
// replica rather than a client watching for changes.

// Point is a position in a master's replication stream. ReplID identifies the
// history it belongs to: a failed-over master continues a different one, so an
// offset alone is meaningless.
type Point struct {
	ReplID string
	Offset int64
}

func (p Point) IsZero() bool { return p.ReplID == "" }

type Handshake struct {
	// Full says the master would not continue from the offset asked for and is
	// sending its whole data set.
	Full bool
	// ReplID is the history the stream now belongs to.
	ReplID string
	// Offset is where the stream starts.
	Offset int64
}

type Command struct {
	// Args is the command and its arguments, as the master sent them.
	Args [][]byte
	// Raw is the bytes it occupied in the stream, kept exactly as received because
	// the offset arithmetic and the buffer both work in them.
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
	// Username and Password authenticate; an empty username with a password sends
	// the old two-argument AUTH.
	Username string
	Password string
	// DialTimeout bounds connecting. Zero means the default.
	DialTimeout time.Duration
	// IdleTimeout is how long the master may say nothing before the link counts as
	// dead. A healthy one pings every ten seconds, so longer silence is a
	// connection gone away quietly. Zero means the default.
	IdleTimeout time.Duration
	// ListeningPort is reported to the master so it appears in INFO replication;
	// zero means this replica serves nothing.
	ListeningPort int
	// TLS, when set, wraps the connection. A rediss:// source is reached over TLS
	// by every other client this program opens, and the replication link dialled
	// plain TCP regardless -- so such a source passed the connection check and
	// then could not be replicated from at all.
	//
	// Nil means plain TCP, which is what an ordinary redis:// source wants.
	TLS *tls.Config
}

const (
	defaultDialTimeout = 10 * time.Second
	defaultIdleTimeout = 60 * time.Second
	// ackPeriod is how often the applied offset is reported back — one second, as
	// a real replica does.
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

// Stream is one replication connection to one master. Not safe for concurrent
// readers, though Ack may be called from another goroutine.
type Stream struct {
	opts StreamOptions

	conn   net.Conn
	reader *bufio.Reader

	// writing guards sending to the master, because acknowledgements go out from
	// the applying side while this side reads.
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
	var conn net.Conn
	var err error
	if opts.TLS != nil {
		// The server name is the host the caller asked for, so certificate
		// verification checks what was dialled rather than whatever the
		// configuration happened to carry.
		config := opts.TLS.Clone()
		if config.ServerName == "" {
			if host, _, splitErr := net.SplitHostPort(opts.Addr); splitErr == nil {
				config.ServerName = host
			}
		}
		conn, err = (&tls.Dialer{NetDialer: &dialer, Config: config}).
			DialContext(ctx, "tcp", opts.Addr)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", opts.Addr)
	}
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
		// Wrong credentials will never start working, so this is not worth retrying
		// against.
		return domain.Unrecoverable("the source refused the credentials: %s", reply)
	}
	return nil
}

// Sync performs the handshake and reports what the master agreed to. On a full
// resync the caller must consume the data set with SkipRDB first; the stream is
// not positioned until it has.
func (s *Stream) Sync(from Point) (Handshake, error) {
	if _, err := s.call([]byte("REPLCONF"), []byte("listening-port"),
		[]byte(strconv.Itoa(s.opts.ListeningPort))); err != nil {
		return Handshake{}, fmt.Errorf("announce the listening port: %w", err)
	}
	// eof asks for the data set without a length prefix, so the master streams it
	// from the fork instead of writing a file. psync2 is what lets a partial
	// resync survive a source failover.
	if _, err := s.call([]byte("REPLCONF"), []byte("capa"), []byte("eof"),
		[]byte("capa"), []byte("psync2")); err != nil {
		return Handshake{}, fmt.Errorf("announce capabilities: %w", err)
	}

	id, offset := "?", "-1"
	if !from.IsZero() {
		// The protocol asks for the first byte wanted, which is one past what has
		// been read.
		id, offset = from.ReplID, strconv.FormatInt(from.Offset+1, 10)
	}
	reply, err := s.call([]byte("PSYNC"), []byte(id), []byte(offset))
	if err != nil {
		return Handshake{}, fmt.Errorf("start replication: %w", err)
	}
	// A master waiting for its background save sends newlines to keep the
	// connection alive, and they arrive before the PSYNC reply. Reading one line
	// and believing it is the reply is how this fails against a real server while
	// passing against a scripted one.
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
		// The stream picks up exactly where the position said, so the count of bytes
		// read stays as it was.
		s.offset = from.Offset
		replID := from.ReplID
		if fields := strings.Fields(reply); len(fields) >= 2 {
			// psync2: the master may hand over a new identifier for the same history,
			// which the position records from now on.
			replID = fields[1]
		}
		return Handshake{ReplID: replID, Offset: from.Offset}, nil

	case strings.HasPrefix(reply, "-NOMASTERLINK"), strings.HasPrefix(reply, "-LOADING"):
		return Handshake{}, fmt.Errorf("the source is not ready to replicate yet: %s", reply)

	case strings.HasPrefix(reply, "-ERR") && strings.Contains(reply, "not allowed"):
		// A managed Redis that disables the replication commands cannot be
		// replicated by this tool at all, and retrying changes nothing.
		//
		// The message used to send the reader to "the scanning reader", which
		// does not exist: there is no fallback here, and telling somebody to
		// reach for one costs them the time it takes to find that out. Not every
		// managed Redis blocks PSYNC -- Memorystore does not -- so this is about
		// the ones that do.
		return Handshake{}, domain.Unrecoverable(
			"the source does not allow PSYNC (%s), so this tool cannot replicate "+
				"from it: reading the stream is the only way it has. Use the "+
				"provider's own cross-region replication, or a source that permits "+
				"replication", reply)
	}
	return Handshake{}, fmt.Errorf("the source answered PSYNC with %q", reply)
}

// SkipRDB consumes and discards the data set a full resync sends. Nothing here
// parses it.
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

		// The master holds the command stream back until the first REPLCONF ACK: a
		// data set streamed without a length gives it no way to know the replica
		// finished loading. Without this the offset climbs and nothing is delivered,
		// which looks exactly like a quiet source.
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

// Next reads one command, keeping the bytes exactly as they arrived: the offset
// is a count of them, and so is anything the source will accept in an
// acknowledgement.
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
			// A keepalive sent while the master was forking, written straight to the
			// connection rather than through the backlog. The master does not count it,
			// so counting it here would drift this side's offset ahead for good.
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

	// Where each argument sits in the raw bytes. Arguments are slices of the copy
	// taken below, never of the scratch buffer, which is reused for the next
	// command.
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

// Ack reports an offset back to the master, which uses it for WAIT, min-
// replicas-to-write and choosing which replica to promote.
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

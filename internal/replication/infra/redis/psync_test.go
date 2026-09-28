package redis

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// fakeMaster is enough of a Redis master to exercise the handshake and the
// stream parser.
type fakeMaster struct {
	t        *testing.T
	listener net.Listener
	// replies are written in order, one per command received, until they run out.
	replies []string
	// afterHandshake is written raw once the replies are exhausted.
	afterHandshake []byte
	// received collects the commands the replica sent.
	received chan []string
	// hangUp, when closed, drops the connection the way a restarted source does.
	hangUp <-chan struct{}
}

func startFakeMaster(t *testing.T, replies []string, afterHandshake []byte) *fakeMaster {
	return startFakeMasterThatHangsUp(t, replies, afterHandshake, nil)
}

func startFakeMasterThatHangsUp(t *testing.T, replies []string, afterHandshake []byte,
	hangUp <-chan struct{}) *fakeMaster {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	m := &fakeMaster{
		t: t, listener: listener, replies: replies,
		afterHandshake: afterHandshake,
		received:       make(chan []string, 16),
		hangUp:         hangUp,
	}
	t.Cleanup(func() { listener.Close() })

	go m.serve()
	return m
}

func (m *fakeMaster) addr() string { return m.listener.Addr().String() }

func (m *fakeMaster) serve() {
	conn, err := m.listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	served := make(chan struct{})
	defer close(served)
	go func() {
		select {
		case <-m.hangUp:
			conn.Close()
		case <-served:
		}
	}()

	for _, reply := range m.replies {
		args, err := readCommandFrom(conn)
		if err != nil {
			return
		}
		select {
		case m.received <- args:
		default:
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
	if len(m.afterHandshake) > 0 {
		if _, err := conn.Write(m.afterHandshake); err != nil {
			return
		}
	}
	// Keep reading what the replica sends — acknowledgements, mainly — both so
	// the connection stays open and so tests can assert on them.
	for {
		args, err := readCommandFrom(conn)
		if err != nil {
			return
		}
		select {
		case m.received <- args:
		default:
		}
	}
}

func readCommandFrom(conn net.Conn) ([]string, error) {
	reader := make([]byte, 0, 256)
	one := make([]byte, 1)
	readLine := func() (string, error) {
		reader = reader[:0]
		for {
			if _, err := conn.Read(one); err != nil {
				return "", err
			}
			if one[0] == '\n' {
				return strings.TrimRight(string(reader), "\r"), nil
			}
			reader = append(reader, one[0])
		}
	}

	header, err := readLine()
	if err != nil {
		return nil, err
	}
	if len(header) == 0 || header[0] != '*' {
		return nil, fmt.Errorf("expected an array, got %q", header)
	}
	var count int
	if _, err := fmt.Sscanf(header[1:], "%d", &count); err != nil {
		return nil, err
	}
	var args []string
	for i := 0; i < count; i++ {
		if _, err := readLine(); err != nil { // the $len line
			return nil, err
		}
		arg, err := readLine()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func resp(args ...string) []byte {
	out := []byte(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		out = append(out, fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)...)
	}
	return out
}

func dialFake(t *testing.T, m *fakeMaster) *Stream {
	t.Helper()
	stream, err := Dial(context.Background(), StreamOptions{
		Addr: m.addr(), IdleTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { stream.Close() })
	return stream
}

// TestAFullResyncSetsTheStartingOffset covers a first connection: the master
// names the history and the offset its data set was taken at, and the stream
// counts from there.
func TestAFullResyncSetsTheStartingOffset(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n",
		"+FULLRESYNC 8f3ca1b2c3d4e5f60718293a4b5c6d7e8f901234 5000\r\n",
	}, nil)

	stream := dialFake(t, master)
	got, err := stream.Sync(Point{})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if !got.Full {
		t.Error("the handshake did not report a full resync")
	}
	if got.ReplID != "8f3ca1b2c3d4e5f60718293a4b5c6d7e8f901234" || got.Offset != 5000 {
		t.Errorf("Sync returned %+v, want the id and offset the master gave", got)
	}
	if stream.Offset() != 5000 {
		t.Errorf("Offset() = %d, want 5000", stream.Offset())
	}
}

// TestAResumeAsksForTheByteAfterWhatWasRead pins the one place the protocol is
// off by one from the position.
func TestAResumeAsksForTheByteAfterWhatWasRead(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+CONTINUE\r\n",
	}, nil)

	stream := dialFake(t, master)
	if _, err := stream.Sync(Point{ReplID: "abc", Offset: 4242}); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	var psync []string
	for i := 0; i < 3; i++ {
		select {
		case args := <-master.received:
			if len(args) > 0 && strings.EqualFold(args[0], "PSYNC") {
				psync = args
			}
		case <-time.After(2 * time.Second):
			t.Fatal("the master never saw PSYNC")
		}
	}
	if len(psync) != 3 {
		t.Fatalf("PSYNC was sent as %v", psync)
	}
	if psync[1] != "abc" || psync[2] != "4243" {
		t.Errorf("PSYNC %s %s, want PSYNC abc 4243", psync[1], psync[2])
	}
}

// TestAContinuedStreamKeepsTheOffsetItResumedFrom covers the counting after a
// partial resync: the bytes that follow extend the count, they do not restart it.
func TestAContinuedStreamKeepsTheOffsetItResumedFrom(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+CONTINUE\r\n",
	}, resp("SET", "foo", "bar"))

	stream := dialFake(t, master)
	got, err := stream.Sync(Point{ReplID: "abc", Offset: 100})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got.Full {
		t.Error("a partial resync was reported as full")
	}
	if got.Offset != 100 {
		t.Errorf("Sync returned offset %d, want 100", got.Offset)
	}

	command, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if want := int64(100 + len(resp("SET", "foo", "bar"))); command.End != want {
		t.Errorf("the command ended at %d, want %d", command.End, want)
	}
}

// TestANewReplicationIDFromAPartialResyncIsAdopted covers psync2: a master that
// was itself failed over hands over a new identifier for the same history, and
// the position has to record the new one or the next resume is refused.
func TestANewReplicationIDFromAPartialResyncIsAdopted(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+CONTINUE deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\r\n",
	}, nil)

	stream := dialFake(t, master)
	got, err := stream.Sync(Point{ReplID: "old", Offset: 7})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got.ReplID != "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef" {
		t.Errorf("ReplID = %q, want the identifier the master handed over", got.ReplID)
	}
}

// TestAManagedInstanceRefusingPSYNCStopsRatherThanRetries covers Memorystore
// and its equivalents, which disable the replication commands.
func TestAManagedInstanceRefusingPSYNCStopsRatherThanRetries(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n",
		"-ERR unknown command 'PSYNC', or command is not allowed\r\n",
	}, nil)

	stream := dialFake(t, master)
	_, err := stream.Sync(Point{})
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("Sync returned %v, want an unrecoverable error", err)
	}
	// It used to name "the scanning reader" as the way out. There is no such
	// reader, so the message sent whoever read it looking for one.
	if strings.Contains(err.Error(), "scanning reader") {
		t.Errorf("error = %v, want it not to offer a reader that does not exist", err)
	}
	if !strings.Contains(err.Error(), "cross-region replication") {
		t.Errorf("error = %v, want it to name something that does exist", err)
	}
}

// TestASourceStillLoadingIsRetried keeps a restarting source from being treated
// as a permanent failure.
func TestASourceStillLoadingIsRetried(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "-LOADING Redis is loading the dataset in memory\r\n",
	}, nil)

	stream := dialFake(t, master)
	_, err := stream.Sync(Point{})
	if err == nil {
		t.Fatal("Sync succeeded against a loading source")
	}
	if domain.IsUnrecoverable(err) {
		t.Errorf("a loading source gave %v, which stops the task instead of retrying", err)
	}
}

// TestWrongCredentialsStopRatherThanRetry keeps the supervisor from spinning on
// something only a human can fix.
func TestWrongCredentialsStopRatherThanRetry(t *testing.T) {
	master := startFakeMaster(t, []string{"-WRONGPASS invalid username-password pair\r\n"}, nil)

	_, err := Dial(context.Background(), StreamOptions{
		Addr: master.addr(), Password: "wrong", IdleTimeout: 2 * time.Second,
	})
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("Dial returned %v, want an unrecoverable error", err)
	}
}

// TestALengthPrefixedDataSetIsConsumedWithoutBeingParsed covers the ordinary
// full resync.
func TestALengthPrefixedDataSetIsConsumedWithoutBeingParsed(t *testing.T) {
	body := strings.Repeat("R", 4096)
	after := append([]byte(fmt.Sprintf("$%d\r\n%s", len(body), body)), resp("SET", "k", "v")...)

	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 0\r\n",
	}, after)

	stream := dialFake(t, master)
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	size, err := stream.SkipRDB(context.Background())
	if err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}
	if size != int64(len(body)) {
		t.Errorf("SkipRDB reported %d bytes, want %d", size, len(body))
	}

	// The command behind it must be the next thing read, and the data set must
	// not have counted towards the offset.
	command, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if command.Name() != "SET" {
		t.Errorf("read %q after the data set, want SET", command.Name())
	}
	if want := int64(len(resp("SET", "k", "v"))); command.End != want {
		t.Errorf("the command ended at %d, want %d — the data set was counted "+
			"towards the offset", command.End, want)
	}
}

// TestADisklessDataSetIsConsumedToItsMarker covers repl-diskless-sync, the
// default since Redis 7: no length is known in advance, so the payload ends with
// the forty-byte marker the header announced.
func TestADisklessDataSetIsConsumedToItsMarker(t *testing.T) {
	marker := strings.Repeat("m", 40)
	body := strings.Repeat("R", 1000)
	after := append([]byte("$EOF:"+marker+"\r\n"+body+marker), resp("PING")...)

	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 0\r\n",
	}, after)

	stream := dialFake(t, master)
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	size, err := stream.SkipRDB(context.Background())
	if err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}
	if size != int64(len(body)) {
		t.Errorf("SkipRDB reported %d bytes, want %d", size, len(body))
	}

	command, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if command.Name() != "PING" {
		t.Errorf("read %q after the data set, want PING", command.Name())
	}
}

// TestNewlinesWhileTheMasterForksAreSkipped covers the keepalive a master sends
// while its fork is still running.
func TestNewlinesWhileTheMasterForksAreSkipped(t *testing.T) {
	body := "RDB"
	after := []byte("\n\n\n$3\r\n" + body)

	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 0\r\n",
	}, after)

	stream := dialFake(t, master)
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := stream.SkipRDB(context.Background()); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}
}

// TestTheOffsetIsTheSumOfTheBytesReceived is the arithmetic everything else
// depends on.
func TestTheOffsetIsTheSumOfTheBytesReceived(t *testing.T) {
	first := resp("SET", "a", "1")
	second := resp("INCR", "counter")
	third := resp("LPUSH", "queue", "job")

	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 200\r\n",
	}, append(append(append([]byte("$0\r\n"), first...), second...), third...))

	stream := dialFake(t, master)
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := stream.SkipRDB(context.Background()); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}

	want := int64(200)
	for _, raw := range [][]byte{first, second, third} {
		command, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		want += int64(len(raw))
		if command.End != want {
			t.Errorf("%s ended at %d, want %d", command.Name(), command.End, want)
		}
		if string(command.Raw) != string(raw) {
			t.Errorf("%s carried %q, want %q", command.Name(), command.Raw, raw)
		}
	}
}

// TestArgumentsSurviveTheNextCommand covers a bug this parser is easy to write.
func TestArgumentsSurviveTheNextCommand(t *testing.T) {
	stream, first, second := twoCommandStream(t,
		resp("SET", "first-key", "first-value"),
		resp("SET", "second-key", "second-value"))

	if got := string(first.Args[1]); got != "first-key" {
		t.Errorf("after reading the second command, the first one's key reads %q, "+
			"want first-key", got)
	}
	if got := string(second.Args[1]); got != "second-key" {
		t.Errorf("the second command's key reads %q, want second-key", got)
	}
	_ = stream
}

func twoCommandStream(t *testing.T, a, b []byte) (*Stream, *Command, *Command) {
	t.Helper()
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 0\r\n",
	}, append(append([]byte("$0\r\n"), a...), b...))

	stream := dialFake(t, master)
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := stream.SkipRDB(context.Background()); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}
	first, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	second, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	return stream, first, second
}

// TestAnEmptyArgumentIsCarried covers SET k "" and friends, where a zero-length
// bulk string is a value and not an absence.
func TestAnEmptyArgumentIsCarried(t *testing.T) {
	_, first, _ := twoCommandStream(t, resp("SET", "k", ""), resp("PING"))
	if len(first.Args) != 3 {
		t.Fatalf("the command carried %d arguments, want 3", len(first.Args))
	}
	if len(first.Args[2]) != 0 {
		t.Errorf("the empty value came through as %q", first.Args[2])
	}
}

// TestABinaryArgumentIsCarriedUnchanged covers RESTORE.
func TestABinaryArgumentIsCarriedUnchanged(t *testing.T) {
	payload := string([]byte{0, 1, '\r', '\n', 0xFF, '$', '*', 0x7F})
	_, first, _ := twoCommandStream(t, resp("RESTORE", "k", "0", payload), resp("PING"))

	if len(first.Args) != 4 {
		t.Fatalf("the command carried %d arguments, want 4", len(first.Args))
	}
	if string(first.Args[3]) != payload {
		t.Errorf("the payload came through as %q, want %q", first.Args[3], payload)
	}
}

// TestAQuietSourceIsReportedAsADeadLink covers the case monitoring cannot see
// otherwise: a connection that has gone away without closing.
func TestAQuietSourceIsReportedAsADeadLink(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 0\r\n",
	}, []byte("$0\r\n"))

	stream, err := Dial(context.Background(), StreamOptions{
		Addr: master.addr(), IdleTimeout: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer stream.Close()

	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := stream.SkipRDB(context.Background()); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}

	_, err = stream.Next(context.Background())
	if err == nil {
		t.Fatal("Next succeeded against a silent source")
	}
	if !strings.Contains(err.Error(), "said nothing") {
		t.Errorf("error = %v, want it to explain that silence means a dead link", err)
	}
}

// TestClosingTheContextEndsTheRead keeps a stopped task from leaving a goroutine
// blocked on a socket.
func TestClosingTheContextEndsTheRead(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 0\r\n",
	}, []byte("$0\r\n"))

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := Dial(ctx, StreamOptions{Addr: master.addr(), IdleTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := stream.SkipRDB(context.Background()); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}

	done := make(chan struct{})
	go func() {
		stream.Next(ctx)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not return when the context was cancelled")
	}
}

// TestGarbageInTheStreamIsRefusedRatherThanGuessedAt covers a desynchronised
// connection.
func TestGarbageInTheStreamIsRefusedRatherThanGuessedAt(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 0\r\n",
	}, []byte("$0\r\nnot a command at all\r\n"))

	stream := dialFake(t, master)
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := stream.SkipRDB(context.Background()); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}
	if _, err := stream.Next(context.Background()); err == nil {
		t.Fatal("Next accepted a byte that cannot start a command")
	}
}

// A master that has to wait for its background save sends newlines to hold the
// connection open, and they arrive *before* the answer to PSYNC — not only
// before the data set.
func TestKeepalivesBeforeTheAnswerToPSYNCAreSkipped(t *testing.T) {
	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n",
		"\n\n\n+FULLRESYNC 8f3ca1b2c3d4e5f60718293a4b5c6d7e8f901234 77\r\n",
	}, nil)

	stream := dialFake(t, master)
	got, err := stream.Sync(Point{})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if !got.Full || got.Offset != 77 {
		t.Errorf("Sync returned %+v, want a full resync at offset 77", got)
	}
}

// A data set streamed without a length gives the master no way to know when
// the replica finished loading it, so it holds the command stream back until
// the first REPLCONF ACK.
func TestTheDataSetIsAcknowledgedSoTheStreamStarts(t *testing.T) {
	marker := strings.Repeat("m", 40)
	after := []byte("$EOF:" + marker + "\r\nDATA" + marker)

	master := startFakeMaster(t, []string{
		"+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 4000\r\n",
	}, after)

	stream := dialFake(t, master)
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := stream.SkipRDB(context.Background()); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case args := <-master.received:
			if len(args) >= 3 && strings.EqualFold(args[0], "REPLCONF") &&
				strings.EqualFold(args[1], "ACK") {
				if args[2] != "4000" {
					t.Errorf("acknowledged offset %s, want 4000", args[2])
				}
				return
			}
		case <-deadline:
			t.Fatal("no REPLCONF ACK was sent after the data set, so a real master " +
				"would never start the command stream")
		}
	}
}

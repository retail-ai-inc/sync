package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// fakeRedis is a minimal RESP server. It exists because the syncer talks to
// Redis through a client rather than an interface, so the only way to exercise
// the replication branches without a live server is to answer the protocol.
// Only the commands this package issues are understood; anything else comes
// back as an error, which is what a real server would say too.
type fakeRedis struct {
	t        *testing.T
	listener net.Listener

	mu       sync.Mutex
	commands []string          // every command received, upper-cased verb plus args
	replies  map[string]string // verb -> canned RESP reply
	pubsub   chan [2]string    // channel/payload pairs to publish to a subscriber
}

func newFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeRedis{
		t:        t,
		listener: ln,
		replies:  map[string]string{},
		pubsub:   make(chan [2]string, 8),
	}
	t.Cleanup(func() { _ = ln.Close() })

	go f.serve()
	return f
}

// on installs the reply for a command verb. The value is raw RESP.
func (f *fakeRedis) on(verb, reply string) *fakeRedis {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replies[strings.ToUpper(verb)] = reply
	return f
}

func (f *fakeRedis) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.commands...)
}

func (f *fakeRedis) sawCommand(verb string) bool {
	for _, c := range f.seen() {
		if strings.HasPrefix(c, strings.ToUpper(verb)+" ") || c == strings.ToUpper(verb) {
			return true
		}
	}
	return false
}

func (f *fakeRedis) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeRedis) handle(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		verb := strings.ToUpper(args[0])

		f.mu.Lock()
		f.commands = append(f.commands, strings.Join(append([]string{verb}, args[1:]...), " "))
		reply, canned := f.replies[verb]
		f.mu.Unlock()

		switch {
		case canned:
			_, _ = io.WriteString(conn, reply)
		case verb == "HELLO":
			// Answering with an error is how a RESP2-only server responds; the
			// client falls back rather than failing.
			_, _ = io.WriteString(conn, "-ERR unknown command 'HELLO'\r\n")
		case verb == "PING":
			_, _ = io.WriteString(conn, "+PONG\r\n")
		case verb == "PSUBSCRIBE":
			pattern := ""
			if len(args) > 1 {
				pattern = args[1]
			}
			_, _ = io.WriteString(conn, fmt.Sprintf(
				"*3\r\n$10\r\npsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(pattern), pattern))
			f.pump(conn, pattern)
			return
		default:
			_, _ = io.WriteString(conn, "-ERR unknown command '"+verb+"'\r\n")
		}
	}
}

// pump forwards queued messages to a pattern subscriber.
func (f *fakeRedis) pump(conn net.Conn, pattern string) {
	for msg := range f.pubsub {
		payload := fmt.Sprintf("*4\r\n$8\r\npmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(pattern), pattern, len(msg[0]), msg[0], len(msg[1]), msg[1])
		if _, err := io.WriteString(conn, payload); err != nil {
			return
		}
	}
}

// publish queues one keyspace notification for the subscriber to receive.
func (f *fakeRedis) publish(channel, payload string) {
	f.pubsub <- [2]string{channel, payload}
}

func (f *fakeRedis) closePubSub() { close(f.pubsub) }

func (f *fakeRedis) client(t *testing.T) *goredis.Client {
	t.Helper()

	client := goredis.NewClient(&goredis.Options{
		Addr:         f.listener.Addr().String(),
		Protocol:     2,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		MaxRetries:   -1,
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// readCommand reads one RESP array of bulk strings, which is the only shape a
// client sends.
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		// An inline command; the clients here never send one.
		return strings.Fields(line), nil
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		header, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(strings.TrimRight(header[1:], "\r\n"))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length+2) // payload plus CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:length]))
	}
	return args, nil
}

// bulk renders a RESP bulk string reply.
func bulk(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }

// newRedisSyncerWithFakes wires a syncer to two stub servers.
func newRedisSyncerWithFakes(t *testing.T, source, target *fakeRedis) *RedisSyncer {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	s := NewRedisSyncer(sampleConfig(), logger)
	s.source = source.client(t)
	s.target = target.client(t)
	return s
}

// TestTheStubAnswersAPing guards the harness itself: every test below assumes
// the client can talk to it.
func TestTheStubAnswersAPing(t *testing.T) {
	f := newFakeRedis(t)

	if err := f.client(t).Ping(ctxFor(t)).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if !f.sawCommand("PING") {
		t.Errorf("commands = %v", f.seen())
	}
}

// TestMain silences go-redis's own logger: the pub/sub reconnect loop writes to
// it every time the stub server hangs up, which buries the test output.
func TestMain(m *testing.M) {
	goredis.SetLogger(quietLogger{})
	os.Exit(m.Run())
}

type quietLogger struct{}

func (quietLogger) Printf(context.Context, string, ...interface{}) {}

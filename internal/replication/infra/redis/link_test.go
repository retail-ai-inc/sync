package redis

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func linkTo(t *testing.T, m *fakeMaster, buffer *Buffer) *link {
	t.Helper()
	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	l := &link{
		opts:   StreamOptions{Addr: m.addr(), IdleTimeout: 10 * time.Second},
		buffer: buffer, shard: "0-16383", logger: quiet,
	}
	t.Cleanup(l.close)
	return l
}

func psyncSent(t *testing.T, m *fakeMaster) []string {
	t.Helper()
	for i := 0; i < 3; i++ {
		select {
		case args := <-m.received:
			if len(args) > 0 && strings.EqualFold(args[0], "PSYNC") {
				return args
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the master never saw PSYNC")
		}
	}
	t.Fatal("the handshake sent no PSYNC")
	return nil
}

func encodedPosition(t *testing.T, p streamPosition) domain.Position {
	t.Helper()
	payload, err := p.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return domain.Position{Payload: payload}
}

func TestAResumeWithABufferBehindThePositionRestartsTheBufferThere(t *testing.T) {
	for _, c := range []struct {
		name string
		held bool
	}{
		{"a buffer holding older history", true},
		{"an empty buffer directory", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			buffer := newBuffer(t, BufferOptions{})
			if c.held {
				if err := buffer.Reset(100); err != nil {
					t.Fatalf("Reset: %v", err)
				}
				if err := buffer.Append(resp("SET", "old", "v")); err != nil {
					t.Fatalf("Append: %v", err)
				}
			}
			arriving := resp("SET", "k", "v")
			master := startFakeMaster(t, []string{"+OK\r\n", "+OK\r\n", "+CONTINUE\r\n"}, arriving)
			l := linkTo(t, master, buffer)

			if _, err := l.start(context.Background(),
				streamPosition{ReplID: "abc", Offset: 500, Phase: phaseCommand}); err != nil {
				t.Fatalf("start: %v", err)
			}
			if psync := psyncSent(t, master); len(psync) != 3 || psync[1] != "abc" || psync[2] != "501" {
				t.Errorf("sent %v, want PSYNC abc 501", psync)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cursor, err := l.cursor(500)
			if err != nil {
				t.Fatalf("cursor at the applied position: %v", err)
			}
			defer cursor.Close()
			raw, end, err := cursor.Next(ctx)
			if err != nil {
				t.Fatalf("read what arrived: %v", err)
			}
			// Numbered from the old head, every later offset misstates what the target has applied.
			if string(raw) != string(arriving) || end != 500+int64(len(arriving)) {
				t.Errorf("read %q ending at %d, want the SET ending at %d",
					raw, end, 500+len(arriving))
			}
			if buffer.Oldest() != 500 {
				t.Errorf("the buffer starts at %d, want 500", buffer.Oldest())
			}
		})
	}
}

func TestABufferTrimmedPastThePositionStopsTheShard(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(1000); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := buffer.Append(resp("SET", "k", "v")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	master := startFakeMaster(t, []string{"+OK\r\n", "+OK\r\n", "+CONTINUE\r\n"}, nil)
	r := &Reader{Shard: "0-16383", Link: linkTo(t, master, buffer), Commands: table()}
	t.Cleanup(func() { _ = r.Close() })

	err := r.Open(context.Background(),
		encodedPosition(t, streamPosition{ReplID: "abc", Offset: 500, Phase: phaseCommand}))
	// Returned as an ordinary error, the supervisor retries a shard that only an operator can restart.
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("Open returned %v, want an unrecoverable error", err)
	}
}

func TestARefusedResumeIsPositionUnusable(t *testing.T) {
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(100); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	held := resp("SET", "k", "v")
	if err := buffer.Append(held); err != nil {
		t.Fatalf("Append: %v", err)
	}
	head := buffer.Newest()
	master := startFakeMaster(t, []string{"+OK\r\n", "+OK\r\n", "+FULLRESYNC newid 9000\r\n"},
		[]byte("$0\r\n"))
	l := linkTo(t, master, buffer)

	_, err := l.start(context.Background(),
		streamPosition{ReplID: "old", Offset: head, Phase: phaseCommand})
	// Taken as a first connection, the task carries on from the new history without a copy or a sweep.
	if !domain.IsPositionUnusable(err) {
		t.Fatalf("start returned %v, want the position reported unusable", err)
	}
	if buffer.Oldest() != 100 || buffer.Newest() != head {
		t.Errorf("the buffer holds %d to %d, want it untouched at 100 to %d",
			buffer.Oldest(), buffer.Newest(), head)
	}
	if l.stream != nil || l.pumpErr != nil {
		t.Error("the refused connection was kept as the link's stream")
	}
}

func TestALostReplicationConnectionStopsTheReaderWithItsReason(t *testing.T) {
	hangUp := make(chan struct{})
	master := startFakeMasterThatHangsUp(t, []string{"+OK\r\n", "+OK\r\n", "+CONTINUE\r\n"},
		resp("SET", "k", "v"), hangUp)
	buffer := newBuffer(t, BufferOptions{})
	if err := buffer.Reset(100); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	r := &Reader{Shard: "0-16383", Link: linkTo(t, master, buffer), Commands: table()}
	t.Cleanup(func() { _ = r.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.Open(ctx,
		encodedPosition(t, streamPosition{ReplID: "abc", Offset: 100, Phase: phaseCommand})); err != nil {
		t.Fatalf("Open: %v", err)
	}
	first, err := r.Next(ctx)
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if first.Key != "k" {
		t.Fatalf("first event is %q, want the SET", first.Key)
	}

	close(hangUp)
	stopped := make(chan error, 1)
	go func() {
		_, err := r.Next(ctx)
		stopped <- err
	}()
	select {
	case err := <-stopped:
		if err == nil || err == io.EOF || !strings.Contains(err.Error(), "read from the source") {
			t.Errorf("Next returned %v, want the connection's own failure", err)
		}
	case <-time.After(5 * time.Second):
		// Waiting here, the shard stops replicating and nothing restarts it.
		t.Fatal("the reader is still waiting on a connection that has gone")
	}
}

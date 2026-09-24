package mongodb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The one-request write needs the bulkWrite command of MongoDB 8.0, which the
// test stack's 6.0 does not have. bulkWriteTarget answers the wire protocol as
// far as the driver needs for it, so the production path runs against a socket.

const opMsg = 2013

type receivedCommand struct {
	name      string
	body      bson.Raw
	sequences map[string][]bson.Raw
}

type bulkWriteTarget struct {
	mu       sync.Mutex
	received []receivedCommand
	replies  []bson.D
}

// newBulkWriteTarget serves replies, in order, to the commands after the
// handshake, and returns a client connected to it.
func newBulkWriteTarget(t *testing.T, replies ...bson.D) (*bulkWriteTarget, *mongo.Client) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	target := &bulkWriteTarget{replies: replies}

	var served sync.WaitGroup
	var connsMu sync.Mutex
	var conns []net.Conn
	served.Add(1)
	go func() {
		defer served.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connsMu.Lock()
			conns = append(conns, conn)
			connsMu.Unlock()
			served.Add(1)
			go func() {
				defer served.Done()
				target.serve(conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + listener.Addr().String() + "/?directConnection=true").
		// A declared API version makes the handshake an OP_MSG, the one message
		// kind this answers.
		SetServerAPIOptions(options.ServerAPI(options.ServerAPIVersion1)))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Disconnect(context.Background())
		_ = listener.Close()
		connsMu.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		connsMu.Unlock()
		served.Wait()
	})
	return target, client
}

func (b *bulkWriteTarget) commands() []receivedCommand {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]receivedCommand(nil), b.received...)
}

func (b *bulkWriteTarget) serve(conn net.Conn) {
	for {
		requestID, command, err := readCommand(conn)
		if err != nil {
			return
		}
		if err := writeReply(conn, requestID, b.answer(command)); err != nil {
			return
		}
	}
}

func (b *bulkWriteTarget) answer(command receivedCommand) bson.D {
	switch command.name {
	case "hello", "isMaster", "ismaster":
		return bson.D{
			{Key: "helloOk", Value: true},
			{Key: "isWritablePrimary", Value: true},
			{Key: "maxBsonObjectSize", Value: int32(16 << 20)},
			{Key: "maxMessageSizeBytes", Value: int32(48000000)},
			{Key: "maxWriteBatchSize", Value: int32(100000)},
			{Key: "logicalSessionTimeoutMinutes", Value: int32(30)},
			{Key: "connectionId", Value: int32(1)},
			{Key: "minWireVersion", Value: int32(0)},
			{Key: "maxWireVersion", Value: int32(25)},
			{Key: "ok", Value: 1.0},
		}
	case "endSessions":
		return bson.D{{Key: "ok", Value: 1.0}}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.received = append(b.received, command)
	if len(b.replies) == 0 {
		return bson.D{
			{Key: "ok", Value: 0.0},
			{Key: "code", Value: int32(8)},
			{Key: "errmsg", Value: "the test queued no reply for " + command.name},
		}
	}
	reply := b.replies[0]
	b.replies = b.replies[1:]
	return reply
}

func readCommand(r io.Reader) (int32, receivedCommand, error) {
	var header [16]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, receivedCommand{}, err
	}
	length := binary.LittleEndian.Uint32(header[0:4])
	requestID := int32(binary.LittleEndian.Uint32(header[4:8]))
	if code := binary.LittleEndian.Uint32(header[12:16]); code != opMsg {
		return 0, receivedCommand{}, fmt.Errorf("op code %d is not OP_MSG", code)
	}
	payload := make([]byte, length-16)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, receivedCommand{}, err
	}

	flags := binary.LittleEndian.Uint32(payload[0:4])
	sections := payload[4:]
	if flags&1 != 0 {
		sections = sections[:len(sections)-4]
	}
	command := receivedCommand{sequences: map[string][]bson.Raw{}}
	for len(sections) > 0 {
		kind := sections[0]
		sections = sections[1:]
		size := int(binary.LittleEndian.Uint32(sections[0:4]))
		switch kind {
		case 0:
			command.body = bson.Raw(sections[:size])
		case 1:
			sequence := sections[4:size]
			end := 0
			for sequence[end] != 0 {
				end++
			}
			identifier := string(sequence[:end])
			for documents := sequence[end+1:]; len(documents) > 0; {
				n := int(binary.LittleEndian.Uint32(documents[0:4]))
				command.sequences[identifier] = append(command.sequences[identifier], bson.Raw(documents[:n]))
				documents = documents[n:]
			}
		default:
			return 0, receivedCommand{}, fmt.Errorf("section kind %d", kind)
		}
		sections = sections[size:]
	}
	elements, err := command.body.Elements()
	if err != nil || len(elements) == 0 {
		return 0, receivedCommand{}, fmt.Errorf("a command with no body: %v", err)
	}
	command.name = elements[0].Key()
	return requestID, command, nil
}

func writeReply(w io.Writer, responseTo int32, reply bson.D) error {
	document, err := bson.Marshal(reply)
	if err != nil {
		return err
	}
	message := make([]byte, 21, 21+len(document))
	binary.LittleEndian.PutUint32(message[0:4], uint32(21+len(document)))
	binary.LittleEndian.PutUint32(message[8:12], uint32(responseTo))
	binary.LittleEndian.PutUint32(message[12:16], opMsg)
	_, err = w.Write(append(message, document...))
	return err
}

func bulkWriteReply(matched, upserted int32, writeErrors ...bson.D) bson.D {
	batch := bson.A{}
	for _, e := range writeErrors {
		batch = append(batch, e)
	}
	return bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "nErrors", Value: int32(len(writeErrors))},
		{Key: "nInserted", Value: int32(0)},
		{Key: "nMatched", Value: matched},
		{Key: "nModified", Value: matched},
		{Key: "nUpserted", Value: upserted},
		{Key: "nDeleted", Value: int32(0)},
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)},
			{Key: "firstBatch", Value: batch},
			{Key: "ns", Value: "admin.$cmd.bulkWrite"},
		}},
	}
}

func oneRequestApplier(client *mongo.Client) *Applier {
	return &Applier{
		Client:         client,
		TargetDatabase: "shop",
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders_archive"}},
		}},
		NoTransaction: true,
	}
}

func deltaTo(collection, id string) *domain.Event {
	return &domain.Event{
		NS: domain.Namespace{DB: "shop", Object: collection},
		Op: domain.OpUpdate,
		Payload: mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": id}).
			SetUpdate(bson.M{"$set": bson.M{"status": 2}}),
	}
}

func TestARunIsWrittenInOneOrderedRequestOnATargetThatHasBulkWrite(t *testing.T) {
	target, client := newBulkWriteTarget(t, bulkWriteReply(1, 1))
	applier := oneRequestApplier(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run := []*domain.Event{deltaTo("orders", "a"), writeEvent("payments", "b")}
	if _, err := applier.Apply(ctx, [][]*domain.Event{run}, domain.Position{}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// More than one command is the per-collection fallback, or a repair read after a miscounted result.
	sent := target.commands()
	if len(sent) != 1 || sent[0].name != "bulkWrite" {
		names := make([]string, len(sent))
		for i, c := range sent {
			names[i] = c.name
		}
		t.Fatalf("the target received %v, want one bulkWrite", names)
	}
	if applier.bulk.unsupported.Load() {
		t.Error("a target that took the bulkWrite was marked as lacking it")
	}
	bulk := sent[0]
	if ordered, ok := bulk.body.Lookup("ordered").BooleanOK(); !ok || !ordered {
		t.Errorf("ordered = %v, want true: a unique value handed between documents applies out of order", bulk.body.Lookup("ordered"))
	}
	var namespaces []string
	for _, info := range bulk.sequences["nsInfo"] {
		namespaces = append(namespaces, info.Lookup("ns").StringValue())
	}
	var ops []string
	for _, op := range bulk.sequences["ops"] {
		ops = append(ops, fmt.Sprintf("%d:%s", op.Lookup("update").Int32(),
			op.Lookup("filter", "_id").StringValue()))
	}
	if fmt.Sprint(namespaces) != "[shop.orders_archive shop.payments]" || fmt.Sprint(ops) != "[0:a 1:b]" {
		t.Errorf("sent nsInfo %v and ops %v, want the run in its order under the mapped names",
			namespaces, ops)
	}
}

func TestAWriteFailureOnTheOneRequestPathIsReturned(t *testing.T) {
	target, client := newBulkWriteTarget(t, bulkWriteReply(0, 0, bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "idx", Value: int32(0)},
		{Key: "code", Value: int32(11000)},
		{Key: "errmsg", Value: "E11000 duplicate key error collection: shop.orders_archive index: email_1"},
	}))
	applier := oneRequestApplier(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run := []*domain.Event{deltaTo("orders", "a"), writeEvent("payments", "b")}
	_, err := applier.Apply(ctx, [][]*domain.Event{run}, domain.Position{})
	var refused mongo.ClientBulkWriteException
	if !errors.As(err, &refused) || refused.WriteErrors[0].Code != 11000 {
		t.Fatalf("Apply = %v, want the duplicate key the target reported", err)
	}
	// Marked unsupported, every later batch takes the slower path and this one is retried through it.
	if applier.bulk.unsupported.Load() {
		t.Error("a refused write was taken to mean the target has no bulkWrite")
	}
	if sent := target.commands(); len(sent) != 1 {
		t.Errorf("the target received %d commands, want only the bulkWrite that failed", len(sent))
	}
}

func TestARunThatCannotBeSentWholeIsNotSentAtAll(t *testing.T) {
	for _, c := range []struct {
		name  string
		event *domain.Event
	}{
		{"a change still awaiting its document", &domain.Event{
			NS:      domain.Namespace{DB: "shop", Object: "orders"},
			Op:      domain.OpUpdate,
			Payload: &fullDocumentRead{filter: bson.M{"_id": "c"}, reason: reasonMissing},
		}},
		{"a write this does not produce", &domain.Event{
			NS:      domain.Namespace{DB: "shop", Object: "orders"},
			Op:      domain.OpUpdate,
			Payload: mongo.NewUpdateManyModel().SetFilter(bson.M{}).SetUpdate(bson.M{"$set": bson.M{"x": 1}}),
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			target, client := newBulkWriteTarget(t)
			applier := oneRequestApplier(client)

			_, _, err := applier.writeRunAsOne(context.Background(),
				[]*domain.Event{writeEvent("payments", "b"), c.event})
			// Sending the rest records a position past a change the target never received.
			if !domain.IsUnrecoverable(err) || len(target.commands()) != 0 {
				t.Errorf("writeRunAsOne = %v with %d commands sent, want it refused before any",
					err, len(target.commands()))
			}
		})
	}
}

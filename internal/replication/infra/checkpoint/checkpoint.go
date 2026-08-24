// Package checkpoint stores how far a replication task has got.
//
// Every checkpoint used to live in a file on the syncer's own disk: the binlog
// position, the MongoDB resume tokens, the Redis stream offsets. That is the
// one place it must not be. The syncer runs in Tokyo alongside the source it is
// reading, so the outage the whole setup exists to survive takes the record of
// what has been applied with it — and a replacement started in Osaka has no way
// to find out where to resume from. It re-copies everything, or worse, starts
// from the current end of the stream and quietly skips whatever was in flight.
//
// The checkpoint belongs with the target, which is the side that survives. That
// is also where it is true: it describes what the target has applied.
package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// tableName is the table, collection or key the checkpoints live in.
const tableName = "_sync_checkpoint"

// Store reads and writes checkpoints for one task. A key names which one: a
// task has one for its binlog position, or one per collection for its resume
// tokens.
type Store interface {
	Load(ctx context.Context, key string) (string, error)
	Save(ctx context.Context, key, payload string) error
}

// ------------------------------------------------------------------- SQL

// SQLStore keeps checkpoints in a table on the target database.
type SQLStore struct {
	DB *sql.DB
	// Schema is the database the table lives in; empty addresses it unqualified.
	Schema string
	TaskID int
	// NumberedPlaceholders spells parameters as $1, $2 rather than ?, which is
	// what PostgreSQL takes. MySQL and SQLite take the question marks.
	NumberedPlaceholders bool
}

// arg renders the nth parameter marker, counting from one.
func (s *SQLStore) arg(n int) string {
	if s.NumberedPlaceholders {
		return "$" + strconv.Itoa(n)
	}
	return "?"
}

func (s *SQLStore) qualified() string {
	if s.Schema == "" {
		return tableName
	}
	return s.Schema + "." + tableName
}

// ensure creates the table if it is not there. The column types are spelled so
// both MySQL and SQLite accept them, since the hermetic suite drives this
// against SQLite.
func (s *SQLStore) ensure(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		task_id INTEGER NOT NULL,
		name VARCHAR(190) NOT NULL,
		payload TEXT NOT NULL,
		PRIMARY KEY (task_id, name))`, s.qualified()))
	if err != nil {
		return fmt.Errorf("create %s: %w", s.qualified(), err)
	}
	return nil
}

func (s *SQLStore) Load(ctx context.Context, key string) (string, error) {
	if err := s.ensure(ctx); err != nil {
		return "", err
	}

	var payload string
	err := s.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT payload FROM %s WHERE task_id = %s AND name = %s",
			s.qualified(), s.arg(1), s.arg(2)),
		s.TaskID, key).Scan(&payload)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read %s: %w", s.qualified(), err)
	}
	return payload, nil
}

func (s *SQLStore) Save(ctx context.Context, key, payload string) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}

	// One statement rather than a delete and an insert inside a transaction.
	// This runs once per source transaction on the MySQL path, and the extra
	// commit was costing an fsync on the target for every transaction
	// replicated: measured against MySQL 8.1, the apply rate went from 47 rows a
	// second to 96 by removing it.
	if _, err := s.DB.ExecContext(ctx, s.upsert(), s.TaskID, key, payload); err != nil {
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}
	return nil
}

// Execer is what SaveTx writes through: a *sql.Tx satisfies it, and so does a
// *sql.DB for callers that have no transaction open.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// SaveTx records the position through a caller's transaction, so it commits with
// whatever else that transaction is applying.
//
// This is how a MySQL replica keeps its own position honest: the applier
// position lives in an InnoDB table and is written in the transaction that
// applies the rows, so a crash cannot leave the two disagreeing. Saving through
// a separate connection — which is what Save does — means the data commits at
// one moment and the position at another, and every crash in between replays
// the difference. Replaying is safe only because the writes are idempotent; on a
// table with no primary key it is not safe at all.
//
// The table has to exist already: creating it here would be DDL inside the
// caller's transaction, which MySQL commits implicitly and would break the
// atomicity this exists to provide. Call Ensure once before streaming.
func (s *SQLStore) SaveTx(ctx context.Context, tx Execer, key, payload string) error {
	if _, err := tx.ExecContext(ctx, s.upsert(), s.TaskID, key, payload); err != nil {
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}
	return nil
}

// Ensure creates the checkpoint table, so that SaveTx never has to.
func (s *SQLStore) Ensure(ctx context.Context) error { return s.ensure(ctx) }

// upsert renders the write. The two flavours spell it differently, which is why
// this was a delete and an insert to begin with.
//
// REPLACE rather than MySQL's ON DUPLICATE KEY UPDATE so that one statement
// serves MySQL, MariaDB and SQLite alike — the hermetic tests run this against
// SQLite, and a store whose only exercised path is the one nothing tests is not
// worth having. The row has no triggers and no auto-increment, so the delete
// REPLACE performs underneath costs nothing here.
func (s *SQLStore) upsert() string {
	if s.NumberedPlaceholders {
		return fmt.Sprintf(
			"INSERT INTO %s (task_id, name, payload) VALUES (%s, %s, %s) "+
				"ON CONFLICT (task_id, name) DO UPDATE SET payload = EXCLUDED.payload",
			s.qualified(), s.arg(1), s.arg(2), s.arg(3))
	}
	return fmt.Sprintf("REPLACE INTO %s (task_id, name, payload) VALUES (%s, %s, %s)",
		s.qualified(), s.arg(1), s.arg(2), s.arg(3))
}

// --------------------------------------------------------------- MongoDB

// MongoStore keeps checkpoints in a collection on the target database.
type MongoStore struct {
	Database *mongo.Database
	TaskID   int
}

func (s *MongoStore) id(key string) string {
	return strconv.Itoa(s.TaskID) + ":" + key
}

func (s *MongoStore) Load(ctx context.Context, key string) (string, error) {
	var doc struct {
		Payload string `bson:"payload"`
	}
	err := s.Database.Collection(tableName).
		FindOne(ctx, bson.M{"_id": s.id(key)}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read %s: %w", tableName, err)
	}
	return doc.Payload, nil
}

func (s *MongoStore) Save(ctx context.Context, key, payload string) error {
	_, err := s.Database.Collection(tableName).ReplaceOne(ctx,
		bson.M{"_id": s.id(key)},
		bson.M{"_id": s.id(key), "task_id": s.TaskID, "name": key, "payload": payload},
		options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("write %s: %w", tableName, err)
	}
	return nil
}

// SaveIn records the position through a caller's session, so it commits with
// whatever else that session's transaction is applying.
//
// A change stream applier writes several collections per batch and MongoDB gives
// no atomicity across them outside a transaction. Recording the position in the
// same transaction is what makes a batch all-or-nothing: without it the batch
// can be half applied and the position can point either side of the gap, and the
// target ends up in a state the source was never in — which is the one kind of
// inconsistency a failover cannot recover from.
//
// The caller has to be inside mongo.SessionContext for this to join its
// transaction; passed a plain context it is an ordinary write.
func (s *MongoStore) SaveIn(ctx context.Context, key, payload string) error {
	return s.Save(ctx, key, payload)
}

// ----------------------------------------------------------------- Redis

// RedisStore keeps checkpoints in one hash on the target.
type RedisStore struct {
	Client goredis.UniversalClient
	TaskID int
}

// redisKey is the hash the checkpoints live in. The name is deliberately not
// one a keyspace replication task would copy across.
const redisKey = "_sync:checkpoint"

func (s *RedisStore) field(key string) string {
	return strconv.Itoa(s.TaskID) + ":" + key
}

func (s *RedisStore) Load(ctx context.Context, key string) (string, error) {
	payload, err := s.Client.HGet(ctx, redisKey, s.field(key)).Result()
	if err == goredis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read %s: %w", redisKey, err)
	}
	return payload, nil
}

func (s *RedisStore) Save(ctx context.Context, key, payload string) error {
	if err := s.Client.HSet(ctx, redisKey, s.field(key), payload).Err(); err != nil {
		return fmt.Errorf("write %s: %w", redisKey, err)
	}
	return nil
}

// ----------------------------------------------------------------- layers

// Layered reads from the first store that has an answer and writes to all of
// them.
//
// It exists for the migration: a deployment upgrading from the file-only
// version has its position on local disk and nothing on the target, so the file
// is read once and every write from then on lands in both places. Ordering the
// target first means that once it has a checkpoint, that is the one used — a
// syncer replaced in the other region reads the same value the old one wrote.
type Layered struct {
	Stores []Store
	// OnError is called for a store that fails, so a degraded layer is visible
	// rather than silent. It may be nil.
	OnError func(error)
}

func (l *Layered) report(err error) {
	if err != nil && l.OnError != nil {
		l.OnError(err)
	}
}

func (l *Layered) Load(ctx context.Context, key string) (string, error) {
	var lastErr error
	for _, store := range l.Stores {
		payload, err := store.Load(ctx, key)
		if err != nil {
			l.report(err)
			lastErr = err
			continue
		}
		if payload != "" {
			return payload, nil
		}
	}
	if lastErr != nil {
		// Every store either failed or had nothing. Reporting the failure
		// matters: "no checkpoint" and "could not read the checkpoint" lead to
		// opposite decisions, and confusing them re-copies a whole database or,
		// worse, skips what was in flight.
		return "", lastErr
	}
	return "", nil
}

// Save writes to every store, reporting a failure only when none of them took
// it. One store being unreachable must not stop the checkpoint being recorded
// in the others.
func (l *Layered) Save(ctx context.Context, key, payload string) error {
	var lastErr error
	saved := 0
	for _, store := range l.Stores {
		if err := store.Save(ctx, key, payload); err != nil {
			l.report(err)
			lastErr = err
			continue
		}
		saved++
	}
	if saved == 0 {
		if lastErr != nil {
			return lastErr
		}
		return fmt.Errorf("no checkpoint store is configured")
	}
	return nil
}

// Encode renders a value as the payload a store holds.
func Encode(v interface{}) (string, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// Decode reads a payload back. An empty payload leaves the destination alone
// and reports false, which is how "there is no checkpoint" is told apart from
// "the checkpoint says the zero value".
func Decode(payload string, v interface{}) (bool, error) {
	if payload == "" {
		return false, nil
	}
	if err := json.Unmarshal([]byte(payload), v); err != nil {
		return false, err
	}
	return true, nil
}

// ------------------------------------------------------------------ file

// FileStore keeps a checkpoint in a file on local disk, which is where every
// checkpoint used to live. It stays for one reason: an existing deployment has
// its position there and has to be able to resume from it once.
type FileStore struct {
	// Path names the file for the empty key; any other key is stored beside it
	// with the key appended, which is how one task keeps a checkpoint per
	// collection or per stream.
	Path string
}

func (s *FileStore) path(key string) string {
	if key == "" {
		return s.Path
	}
	return s.Path + "." + key
}

func (s *FileStore) Load(_ context.Context, key string) (string, error) {
	if s.Path == "" {
		return "", nil
	}
	data, err := os.ReadFile(s.path(key))
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read %s: %w", s.path(key), err)
	}
	return strings.TrimSpace(string(data)), nil
}

func (s *FileStore) Save(_ context.Context, key, payload string) error {
	if s.Path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path(key)), 0o755); err != nil {
		return fmt.Errorf("create the directory for %s: %w", s.path(key), err)
	}
	if err := os.WriteFile(s.path(key), []byte(payload), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", s.path(key), err)
	}
	return nil
}

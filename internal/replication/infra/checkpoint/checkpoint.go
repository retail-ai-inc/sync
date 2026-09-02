// Package checkpoint stores how far a replication task has got. It belongs with
// the target, which is the side that survives the outage and the side it is
// true about: on the syncer's own disk the outage takes it away, and a
// replacement in Osaka cannot know where to resume.
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
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const tableName = "_sync_checkpoint"

// Store reads and writes checkpoints for one task; a key names which one — a
// binlog position, or one per collection for resume tokens.
type Store interface {
	Load(ctx context.Context, key string) (string, error)
	Save(ctx context.Context, key, payload string) error
}

type SQLStore struct {
	DB *sql.DB
	// Schema is the database the table lives in; empty addresses it unqualified.
	Schema string
	TaskID int
	// NumberedPlaceholders spells parameters as $1, $2 for PostgreSQL; MySQL and
	// SQLite take question marks.
	NumberedPlaceholders bool
}

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

// ensure creates the table if absent, with column types both MySQL and SQLite
// accept, since the hermetic suite drives this against SQLite.
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

	// One statement rather than a delete and an insert in a transaction: this runs
	// per source transaction, and the extra commit cost an fsync each time —
	// measured on MySQL 8.1, 47 rows a second became 96 without it.
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

// SaveTx records the position through the caller's transaction so it commits
// with the data, the way a MySQL replica keeps its own position honest; a
// separate connection lets a crash land between the two. The table must exist
// already — creating it here would be DDL inside that transaction, which MySQL
// commits implicitly, so call Ensure first.
func (s *SQLStore) SaveTx(ctx context.Context, tx Execer, key, payload string) error {
	if _, err := tx.ExecContext(ctx, s.upsert(), s.TaskID, key, payload); err != nil {
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}
	return nil
}

func (s *SQLStore) Ensure(ctx context.Context) error { return s.ensure(ctx) }

// upsert uses REPLACE rather than ON DUPLICATE KEY UPDATE so one statement
// serves MySQL, MariaDB and SQLite alike; the hermetic tests run against
// SQLite.
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
// that transaction: a change stream applier writes several collections per
// batch, and MongoDB gives no atomicity across them otherwise.
func (s *MongoStore) SaveIn(ctx context.Context, key, payload string) error {
	return s.Save(ctx, key, payload)
}

type RedisStore struct {
	Client goredis.UniversalClient
	TaskID int
}

// redisKey is the hash the checkpoints live in, named so a keyspace replication
// task would not copy it across.
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

// Layered reads from the first store with an answer and writes to all of them,
// for the migration: a deployment upgrading from the file-only version has its
// position on disk and nothing on the target.
type Layered struct {
	Stores []Store
	// OnError is called for a store that fails, so a degraded layer is visible
	// rather than silent. May be nil.
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
		// Every store failed or had nothing. Reporting the failure matters: "no
		// checkpoint" and "could not read it" lead to opposite decisions, and
		// confusing them re-copies a database or skips what was in flight.
		return "", lastErr
	}
	return "", nil
}

// Save writes to every store, failing only when none took it: one unreachable
// store must not stop the others recording.
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

func Encode(v interface{}) (string, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// Decode leaves the destination alone on an empty payload and reports false,
// which is how "no checkpoint" differs from "the checkpoint says the zero
// value".
func Decode(payload string, v interface{}) (bool, error) {
	if payload == "" {
		return false, nil
	}
	if err := json.Unmarshal([]byte(payload), v); err != nil {
		return false, err
	}
	return true, nil
}

// FileStore keeps a checkpoint on local disk, where every checkpoint used to
// live. It stays so an existing deployment can resume from its position once.
type FileStore struct {
	// Path names the file for the empty key; any other key is stored beside it
	// with the key appended, which is how one task keeps a checkpoint per
	// collection.
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

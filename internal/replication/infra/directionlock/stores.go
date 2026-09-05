package directionlock

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// tableName is the table, collection or key the claims live in. It is on the
// replicated database itself rather than in the syncer's own state, because the
// question it answers — "who is writing here?" — has to survive the syncer
// being replaced, moved to another region, or run twice by mistake.
const tableName = "_sync_direction_lock"

// ErrClaimHeld reports that another process holds a live claim on the task, so
// this one did not take it. It is what makes the claim mutually exclusive
// rather than last-writer-wins.
var ErrClaimHeld = errors.New("the direction claim is held by another process")

type SQLStore struct {
	DB *sql.DB
	// Schema is the database the table lives in. It may be empty, in which case
	// the table is addressed unqualified.
	Schema string
	// Address describes the endpoint without credentials.
	Address string
	// NumberedPlaceholders spells parameters as $1, $2 rather than ?, which is
	// what PostgreSQL takes. MySQL and SQLite take the question marks.
	NumberedPlaceholders bool

	// mu guards made, which records that the table has been created in this
	// process.
	mu   sync.Mutex
	made bool
}

func (s *SQLStore) arg(n int) string {
	if s.NumberedPlaceholders {
		return "$" + strconv.Itoa(n)
	}
	return "?"
}

func (s *SQLStore) Endpoint() string { return s.Address }

// qualified renders the table name the way the rest of the syncer addresses
// tables on this database.
func (s *SQLStore) qualified() string {
	if s.Schema == "" {
		return tableName
	}
	return s.Schema + "." + tableName
}

// ensure creates the table if it is not there. The column types are spelled so
// both MySQL and SQLite accept them, since the hermetic suite drives this
// against SQLite.
// ensure creates the table if absent.
//
// Once per process, not once per claim. MySQL writes a CREATE TABLE to the
// binary log whether or not the table was there to create, and a claim is
// refreshed on both endpoints every HeartbeatInterval -- so this put a DDL
// statement into the source's binary log every minute, for ever, from a tool
// that is otherwise a reader there. Any task replicating that source then read
// its own lock table's DDL back and counted it as a schema change it had
// refused to carry, which is what the refusal metric was showing.
//
// The checkpoint store had the same defect and the same fix; this is its
// sibling.
func (s *SQLStore) ensure(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.made {
		return nil
	}

	if err := s.create(ctx); err != nil {
		return err
	}
	s.made = true
	return nil
}

func (s *SQLStore) create(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		task_id INTEGER PRIMARY KEY,
		role TEXT NOT NULL,
		peer TEXT NOT NULL,
		owner TEXT NOT NULL,
		updated_at TEXT NOT NULL)`, s.qualified()))
	if err != nil {
		return fmt.Errorf("create %s: %w", s.qualified(), err)
	}
	return nil
}

func (s *SQLStore) Claims(ctx context.Context) ([]Claim, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}

	rows, err := s.DB.QueryContext(ctx,
		fmt.Sprintf("SELECT task_id, role, peer, owner, updated_at FROM %s", s.qualified()))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", s.qualified(), err)
	}
	defer rows.Close()

	var claims []Claim
	for rows.Next() {
		var c Claim
		var role, updated string
		if err := rows.Scan(&c.TaskID, &role, &c.Peer, &c.Owner, &updated); err != nil {
			return nil, fmt.Errorf("read %s: %w", s.qualified(), err)
		}
		c.Role = Role(role)
		c.UpdatedAt, err = time.Parse(time.RFC3339, updated)
		if err != nil {
			// A timestamp that cannot be read is treated as long past, so a
			// corrupt row cannot hold an endpoint hostage.
			c.UpdatedAt = time.Time{}
		}
		claims = append(claims, c)
	}
	return claims, rows.Err()
}

func (s *SQLStore) Put(ctx context.Context, c Claim) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Only this owner's claim, or one nobody has refreshed for long enough to
	// count as abandoned, may be cleared. Another process's live claim is left
	// where it is, and the insert below then writes nothing.
	stale := c.UpdatedAt.Add(-ConcurrentAfter).Format(time.RFC3339)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE task_id = %s AND (owner = %s OR updated_at < %s)",
		s.qualified(), s.arg(1), s.arg(2), s.arg(3)),
		c.TaskID, c.Owner, stale); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}

	// Conditional on the row still being absent, in one statement, so two
	// processes claiming the same task at the same moment cannot both succeed.
	// It used to be an unconditional delete and insert: both read no claim, both
	// wrote one, and the second overwrote the first -- leaving two writers on
	// one target, which is the single thing this lock exists to prevent. Their
	// heartbeats then overwrote each other for as long as both ran.
	result, err := tx.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (task_id, role, peer, owner, updated_at) "+
			"SELECT %s, %s, %s, %s, %s WHERE NOT EXISTS "+
			"(SELECT 1 FROM %s WHERE task_id = %s)",
		s.qualified(), s.arg(1), s.arg(2), s.arg(3), s.arg(4), s.arg(5),
		s.qualified(), s.arg(6)),
		c.TaskID, string(c.Role), c.Peer, c.Owner,
		c.UpdatedAt.Format(time.RFC3339), c.TaskID)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}

	written, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}
	if written == 0 {
		_ = tx.Rollback()
		// Whoever holds it is reported by the caller's own read; this says only
		// that the claim was not taken.
		return fmt.Errorf("%w: task %d on %s", ErrClaimHeld, c.TaskID, s.Endpoint())
	}
	return tx.Commit()
}

func (s *SQLStore) Remove(ctx context.Context, taskID int) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE task_id = %s", s.qualified(), s.arg(1)), taskID)
	if err != nil {
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}
	return nil
}

type MongoStore struct {
	Database *mongo.Database
	Address  string
}

func (s *MongoStore) Endpoint() string { return s.Address }

func (s *MongoStore) Claims(ctx context.Context) ([]Claim, error) {
	cursor, err := s.Database.Collection(tableName).Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", tableName, err)
	}
	defer cursor.Close(ctx)

	var claims []Claim
	for cursor.Next(ctx) {
		var doc struct {
			TaskID    int    `bson:"_id"`
			Role      string `bson:"role"`
			Peer      string `bson:"peer"`
			Owner     string `bson:"owner"`
			UpdatedAt string `bson:"updated_at"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("read %s: %w", tableName, err)
		}
		claim := Claim{TaskID: doc.TaskID, Role: Role(doc.Role), Peer: doc.Peer, Owner: doc.Owner}
		if at, err := time.Parse(time.RFC3339, doc.UpdatedAt); err == nil {
			claim.UpdatedAt = at
		}
		claims = append(claims, claim)
	}
	return claims, cursor.Err()
}

// Put takes the claim, or reports that somebody else holds it.
//
// Conditional, in one operation. It used to be an unconditional upsert: two
// processes starting together both read no claim and both wrote one, and the
// second overwrote the first -- leaving two writers on one target, which is the
// single thing this lock exists to prevent.
//
// updated_unix is written beside updated_at because the string form cannot be
// compared: RFC3339 with a fractional second sorts before one without.
func (s *MongoStore) Put(ctx context.Context, c Claim) error {
	stale := c.UpdatedAt.Add(-ConcurrentAfter).Unix()
	document := bson.M{
		"_id":          c.TaskID,
		"role":         string(c.Role),
		"peer":         c.Peer,
		"owner":        c.Owner,
		"updated_at":   c.UpdatedAt.Format(time.RFC3339),
		"updated_unix": c.UpdatedAt.Unix(),
	}

	// Mine, or nobody's for long enough to count as abandoned. A live claim held
	// by somebody else matches nothing, so the upsert tries to insert a document
	// whose _id is already taken and the server refuses it -- which is the answer
	// rather than an error to retry.
	_, err := s.Database.Collection(tableName).UpdateOne(ctx,
		bson.M{"_id": c.TaskID, "$or": []bson.M{
			{"owner": c.Owner},
			{"updated_unix": bson.M{"$lt": stale}},
			{"updated_unix": bson.M{"$exists": false}},
		}},
		bson.M{"$set": document},
		options.UpdateOne().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("%w: task %d on %s", ErrClaimHeld, c.TaskID, s.Endpoint())
	}
	if err != nil {
		return fmt.Errorf("write %s: %w", tableName, err)
	}
	return nil
}

func (s *MongoStore) Remove(ctx context.Context, taskID int) error {
	_, err := s.Database.Collection(tableName).DeleteOne(ctx, bson.M{"_id": taskID})
	if err != nil {
		return fmt.Errorf("write %s: %w", tableName, err)
	}
	return nil
}

type RedisStore struct {
	Client  goredis.UniversalClient
	Address string
}

func (s *RedisStore) Endpoint() string { return s.Address }

// RedisKey is the hash the claims live in. A keyspace replication task has to
// skip it in both directions -- the first copy and the stream -- because
// copying a claim tells the target it is a source. It is exported so that the
// skip names this rather than a string of its own: the two drifting apart is
// how the claim gets copied.
const RedisKey = "_sync:direction_lock"

func (s *RedisStore) Claims(ctx context.Context) ([]Claim, error) {
	fields, err := s.Client.HGetAll(ctx, RedisKey).Result()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", RedisKey, err)
	}

	var claims []Claim
	for _, raw := range fields {
		var c Claim
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			// A field that cannot be read is ignored rather than holding the
			// endpoint hostage.
			continue
		}
		claims = append(claims, c)
	}
	return claims, nil
}

// claimScript takes the claim only when nobody else holds a live one.
//
// A script because the read and the write have to be one step: two processes
// starting together both read no claim and both wrote one, and the second
// overwrote the first -- two writers on one target, which is the single thing
// this lock exists to prevent.
//
// The time is kept in a field of its own, as seconds, because the claim's own
// updated_at cannot be compared as a string: RFC3339 with a fractional second
// sorts before one without.
var claimScript = goredis.NewScript(`
local held = redis.call('HGET', KEYS[1], ARGV[1])
if held then
  local ok, claim = pcall(cjson.decode, held)
  local at = tonumber(redis.call('HGET', KEYS[1], ARGV[1] .. ':at')) or 0
  if ok and claim.owner ~= ARGV[2] and at >= tonumber(ARGV[3]) then
    return 0
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4], ARGV[1] .. ':at', ARGV[5])
return 1
`)

func (s *RedisStore) Put(ctx context.Context, c Claim) error {
	encoded, err := json.Marshal(c)
	if err != nil {
		return err
	}

	field := strconv.Itoa(c.TaskID)
	taken, err := claimScript.Run(ctx, s.Client, []string{RedisKey},
		field, c.Owner, c.UpdatedAt.Add(-ConcurrentAfter).Unix(),
		string(encoded), c.UpdatedAt.Unix()).Int()
	if err != nil {
		return fmt.Errorf("write %s: %w", RedisKey, err)
	}
	if taken == 0 {
		return fmt.Errorf("%w: task %d on %s", ErrClaimHeld, c.TaskID, s.Endpoint())
	}
	return nil
}

func (s *RedisStore) Remove(ctx context.Context, taskID int) error {
	field := strconv.Itoa(taskID)
	if err := s.Client.HDel(ctx, RedisKey, field, field+":at").Err(); err != nil {
		return fmt.Errorf("write %s: %w", RedisKey, err)
	}
	return nil
}

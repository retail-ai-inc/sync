package directionlock

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// tableName is the table, collection or key the claims live in. It is on the
// replicated database itself rather than in the syncer's own state, because the
// question it answers — "who is writing here?" — has to survive the syncer
// being replaced, moved to another region, or run twice by mistake.
const tableName = "_sync_direction_lock"

// ------------------------------------------------------------------- SQL

// SQLStore keeps the claims in a table on the database itself.
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
}

// arg renders the nth parameter marker, counting from one.
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
func (s *SQLStore) ensure(ctx context.Context) error {
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
	// Delete and insert rather than an upsert, because the two flavours spell
	// an upsert differently and this is one row.
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE task_id = %s", s.qualified(), s.arg(1)),
		c.TaskID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (task_id, role, peer, owner, updated_at) VALUES (%s, %s, %s, %s, %s)",
		s.qualified(), s.arg(1), s.arg(2), s.arg(3), s.arg(4), s.arg(5)),
		c.TaskID, string(c.Role), c.Peer, c.Owner, c.UpdatedAt.Format(time.RFC3339)); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("write %s: %w", s.qualified(), err)
	}
	return tx.Commit()
}

// --------------------------------------------------------------- MongoDB

// MongoStore keeps the claims in a collection on the database itself.
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

func (s *MongoStore) Put(ctx context.Context, c Claim) error {
	_, err := s.Database.Collection(tableName).ReplaceOne(ctx,
		bson.M{"_id": c.TaskID},
		bson.M{
			"_id":        c.TaskID,
			"role":       string(c.Role),
			"peer":       c.Peer,
			"owner":      c.Owner,
			"updated_at": c.UpdatedAt.Format(time.RFC3339),
		},
		options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("write %s: %w", tableName, err)
	}
	return nil
}

// ----------------------------------------------------------------- Redis

// RedisStore keeps the claims in one hash, keyed by task.
type RedisStore struct {
	Client  goredis.UniversalClient
	Address string
}

func (s *RedisStore) Endpoint() string { return s.Address }

// redisKey is the hash the claims live in. The name is deliberately not one a
// keyspace replication task would copy across, since copying a claim would tell
// the target it is a source.
const redisKey = "_sync:direction_lock"

func (s *RedisStore) Claims(ctx context.Context) ([]Claim, error) {
	fields, err := s.Client.HGetAll(ctx, redisKey).Result()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", redisKey, err)
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

func (s *RedisStore) Put(ctx context.Context, c Claim) error {
	encoded, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err := s.Client.HSet(ctx, redisKey, strconv.Itoa(c.TaskID), string(encoded)).Err(); err != nil {
		return fmt.Errorf("write %s: %w", redisKey, err)
	}
	return nil
}

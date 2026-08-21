package domain

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Config is a sync task's configuration as stored in the config_json column.
//
// It had three declarations before this one: an anonymous `extra` struct the
// list endpoint read into, and two anonymous `cfgJSONStruct` values the create
// and update endpoints marshalled. All three carried exactly these fourteen
// fields with exactly these tags, so this type replaces them without changing a
// byte of the stored document.
type Config struct {
	Type                   string                   `json:"type"`
	TaskName               string                   `json:"taskName"`
	Status                 string                   `json:"status"`
	SourceConn             map[string]string        `json:"sourceConn"`
	TargetConn             map[string]string        `json:"targetConn"`
	Mappings               []map[string]interface{} `json:"mappings"`
	PgReplicationSlot      string                   `json:"pg_replication_slot"`
	PgPlugin               string                   `json:"pg_plugin"`
	PgPositionPath         string                   `json:"pg_position_path"`
	PgPublicationNames     string                   `json:"pg_publication_names"`
	MysqlPositionPath      string                   `json:"mysql_position_path"`
	MongodbResumeTokenPath string                   `json:"mongodb_resume_token_path"`
	RedisPositionPath      string                   `json:"redis_position_path"`
	SecurityEnabled        bool                     `json:"securityEnabled"`
}

// Request is the body the create and update endpoints accept. It is Config with
// the engine named sourceType rather than type, which is what the front end
// sends.
type Request struct {
	TaskName               string                   `json:"taskName"`
	SourceType             string                   `json:"sourceType"`
	Status                 string                   `json:"status"`
	SourceConn             map[string]string        `json:"sourceConn"`
	TargetConn             map[string]string        `json:"targetConn"`
	Mappings               []map[string]interface{} `json:"mappings"`
	PgReplicationSlot      string                   `json:"pg_replication_slot"`
	PgPlugin               string                   `json:"pg_plugin"`
	PgPositionPath         string                   `json:"pg_position_path"`
	PgPublicationNames     string                   `json:"pg_publication_names"`
	MysqlPositionPath      string                   `json:"mysql_position_path"`
	MongodbResumeTokenPath string                   `json:"mongodb_resume_token_path"`
	RedisPositionPath      string                   `json:"redis_position_path"`
	SecurityEnabled        bool                     `json:"securityEnabled"`
}

// The two statuses a task can be in.
const (
	StatusRunning = "Running"
	StatusStopped = "Stopped"
)

// Normalise fills in the defaults the create and update endpoints apply to a
// request before storing it, and reports the enable column that goes with the
// resulting status.
func (req *Request) Normalise() (enable int) {
	if req.TaskName == "" {
		req.TaskName = "Sync Task"
	}
	if req.Status == "" {
		req.Status = StatusStopped
	}
	if strings.EqualFold(req.Status, StatusRunning) {
		return 1
	}
	return 0
}

// ConfigFrom builds the stored configuration from a request.
//
// Every field comes from the request, so an omitted field is stored as its zero
// value and whatever was there before is lost — an update replaces the
// configuration rather than merging into it.
func ConfigFrom(req Request) Config {
	return Config{
		Type:                   req.SourceType,
		TaskName:               req.TaskName,
		Status:                 req.Status,
		SourceConn:             req.SourceConn,
		TargetConn:             req.TargetConn,
		Mappings:               req.Mappings,
		PgReplicationSlot:      req.PgReplicationSlot,
		PgPlugin:               req.PgPlugin,
		PgPositionPath:         req.PgPositionPath,
		PgPublicationNames:     req.PgPublicationNames,
		MysqlPositionPath:      req.MysqlPositionPath,
		MongodbResumeTokenPath: req.MongodbResumeTokenPath,
		RedisPositionPath:      req.RedisPositionPath,
		SecurityEnabled:        req.SecurityEnabled,
	}
}

// SyncTask is one replication task, as the sync_tasks table holds it.
//
// The timestamps stay the strings the columns store: the endpoints hand them to
// the JST converter verbatim, and parsing them here would change what an
// unparseable value does.
type SyncTask struct {
	id             int
	enable         int
	lastUpdateTime string
	lastRunTime    string
	configJSON     string
}

// NewSyncTask builds a task from a stored row.
func NewSyncTask(id, enable int, lastUpdate, lastRun, configJSON string) SyncTask {
	return SyncTask{
		id:             id,
		enable:         enable,
		lastUpdateTime: lastUpdate,
		lastRunTime:    lastRun,
		configJSON:     configJSON,
	}
}

func (t SyncTask) ID() int                { return t.id }
func (t SyncTask) IsEnabled() bool        { return t.enable != 0 }
func (t SyncTask) LastUpdateTime() string { return t.lastUpdateTime }
func (t SyncTask) LastRunTime() string    { return t.lastRunTime }
func (t SyncTask) ConfigJSON() string     { return t.configJSON }

// Config parses the stored configuration. A document that will not parse yields
// the zero value together with the error; the list endpoint logs it and serves
// the task with empty connections rather than failing the request.
func (t SyncTask) Config() (Config, error) {
	var c Config
	if t.configJSON == "" {
		return c, nil
	}
	err := json.Unmarshal([]byte(t.configJSON), &c)
	return c, err
}

// Status reports the task's status. The enable column decides, but a status
// recorded inside the configuration overrides it.
func (t SyncTask) Status(c Config) string {
	status := StatusStopped
	if t.enable == 1 {
		status = StatusRunning
	}
	if c.Status != "" {
		status = c.Status
	}
	return status
}

// DisplayName reports the task's name, substituting a generated one when the
// configuration carries none.
func (t SyncTask) DisplayName(c Config) string {
	if c.TaskName == "" {
		return fmt.Sprintf("Sync Task %d", t.id)
	}
	return c.TaskName
}

// IsMongoDB reports whether a configuration names the MongoDB engine. The
// comparison is case-insensitive, which the monitor does and the syncer
// dispatch does not (T-053).
func IsMongoDB(c Config) bool { return strings.EqualFold(c.Type, "mongodb") }

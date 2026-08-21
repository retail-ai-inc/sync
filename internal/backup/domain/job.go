package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// Config is a backup job's configuration as stored in the config_json column.
//
// It had six declarations before this one: an anonymous struct in the list
// handler, two more inside the create handler, two inside the update handler,
// and a named ExecutorBackupConfig in the executor. All five in the handlers
// carried exactly these twelve fields with exactly these tags, so this type
// replaces them without changing a byte of the JSON that goes into or comes out
// of the column. The executor keeps its own shape for now: it reads a different
// projection of the same document, and reconciling the two is aggregate work
// (#59).
type Config struct {
	Name               string                 `json:"name"`
	SourceType         string                 `json:"sourceType"`
	Database           map[string]interface{} `json:"database"`
	Destination        map[string]interface{} `json:"destination"`
	Schedule           string                 `json:"schedule"`
	Format             string                 `json:"format"`
	BackupType         string                 `json:"backupType"`
	Query              map[string]interface{} `json:"query"`
	Status             string                 `json:"status"`
	CompressionType    string                 `json:"compressionType"`
	TableSelectionMode string                 `json:"tableSelectionMode"`
	RegexPattern       string                 `json:"regexPattern"`
}

// Request is the body the create and update endpoints accept. It is Config
// without the status, which the server decides rather than the caller.
type Request struct {
	Name               string                 `json:"name"`
	SourceType         string                 `json:"sourceType"`
	Database           map[string]interface{} `json:"database"`
	Destination        map[string]interface{} `json:"destination"`
	Schedule           string                 `json:"schedule"`
	Format             string                 `json:"format"`
	BackupType         string                 `json:"backupType"`
	Query              map[string]interface{} `json:"query"`
	CompressionType    string                 `json:"compressionType"`
	TableSelectionMode string                 `json:"tableSelectionMode"`
	RegexPattern       string                 `json:"regexPattern"`
}

// ConfigFrom builds the stored configuration from a request and a status.
//
// Every field comes from the request, so a request that omits a field stores
// the zero value for it and the previously stored value is lost — an update is
// a replacement, not a merge (T-111). Making it a merge is a behaviour change
// and belongs to #59.
func ConfigFrom(req Request, status string) Config {
	return Config{
		Name:               req.Name,
		SourceType:         req.SourceType,
		Database:           req.Database,
		Destination:        req.Destination,
		Schedule:           req.Schedule,
		Format:             req.Format,
		BackupType:         req.BackupType,
		Query:              req.Query,
		Status:             status,
		CompressionType:    req.CompressionType,
		TableSelectionMode: req.TableSelectionMode,
		RegexPattern:       req.RegexPattern,
	}
}

// The two statuses the enable column maps onto.
const (
	StatusEnabled  = "enabled"
	StatusDisabled = "disabled"
)

// BackupJob is one scheduled export, as the backup_tasks table holds it.
//
// The timestamps are the strings the columns store rather than time.Time: the
// column is a DATETIME the endpoints hand to the JST converter verbatim, and
// parsing them here would change what an unparseable value does.
type BackupJob struct {
	id             int
	enable         int
	lastUpdateTime string
	lastBackupTime string
	nextBackupTime string
	configJSON     string
}

// NewBackupJob builds a job from a stored row.
func NewBackupJob(id, enable int, lastUpdate, lastBackup, nextBackup, configJSON string) BackupJob {
	return BackupJob{
		id:             id,
		enable:         enable,
		lastUpdateTime: lastUpdate,
		lastBackupTime: lastBackup,
		nextBackupTime: nextBackup,
		configJSON:     configJSON,
	}
}

func (j BackupJob) ID() int                   { return j.id }
func (j BackupJob) Enable() int               { return j.enable }
func (j BackupJob) LastUpdateTime() string    { return j.lastUpdateTime }
func (j BackupJob) LastBackupTime() string    { return j.lastBackupTime }
func (j BackupJob) NextBackupTimeRaw() string { return j.nextBackupTime }
func (j BackupJob) ConfigJSON() string        { return j.configJSON }

// Config parses the stored configuration. A document that will not parse
// yields a zero Config and an error; the list endpoint logs the error and
// carries on with the zero value, which is why a corrupt row is served as a
// job with no name and no schedule rather than as a failure.
func (j BackupJob) Config() (Config, error) {
	var c Config
	if j.configJSON == "" {
		return c, nil
	}
	err := json.Unmarshal([]byte(j.configJSON), &c)
	return c, err
}

// Status reports the job's status. It is derived from the enable column, but a
// status recorded inside the configuration overrides it — the two can disagree,
// and when they do the configuration wins.
func (j BackupJob) Status(c Config) string {
	status := StatusDisabled
	if j.enable == 1 {
		status = StatusEnabled
	}
	if c.Status != "" {
		status = c.Status
	}
	return status
}

// DisplayName reports the job's name, substituting a generated one when the
// configuration carries none.
func (j BackupJob) DisplayName(c Config) string {
	if c.Name == "" {
		return fmt.Sprintf("Backup Task %d", j.id)
	}
	return c.Name
}

// Run is one execution of a job, tracked in memory while it happens.
type Run struct {
	TaskID      string     `json:"taskId"`
	BackupID    int        `json:"backupId"`
	Status      string     `json:"status"` // pending, running, completed, failed
	Message     string     `json:"message"`
	CreatedAt   time.Time  `json:"createdAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	Error       string     `json:"error,omitempty"`
}

// The statuses a run passes through.
const (
	RunPending   = "pending"
	RunRunning   = "running"
	RunCompleted = "completed"
	RunFailed    = "failed"
)

// IsTerminal reports whether a run status means the run is over.
func IsTerminal(status string) bool {
	return status == RunCompleted || status == RunFailed
}

// Advance moves a run to a new status.
//
// A nil error leaves any previously recorded error in place, so a run that
// fails and is then recovered keeps its stale error text (T-113). Preserved as
// it stands.
func (r *Run) Advance(status, message string, err error) {
	r.Status = status
	r.Message = message
	if err != nil {
		r.Error = err.Error()
	}
	if IsTerminal(status) {
		now := time.Now()
		r.CompletedAt = &now
	}
}

// DeriveUpdateStatus reports the status an update keeps for a job. A status
// recorded in the stored configuration wins; otherwise the enable column
// decides.
//
// The assertion on the stored value is unchecked, so a configuration whose
// status is not a string panics and the request dies with a 500 and no body
// (T-112). Preserved as it stands.
func DeriveUpdateStatus(oldConfig map[string]interface{}, enable int) string {
	status := StatusDisabled
	if val, ok := oldConfig["status"]; ok {
		status = val.(string)
	} else if enable == 1 {
		status = StatusEnabled
	}
	return status
}

// DeriveUpdateName reports the name an update keeps when the request omits one:
// the stored name, or a generated one when there is none.
//
// The assertion on the stored value is unchecked, so a configuration whose name
// is not a string panics. Preserved as it stands.
func DeriveUpdateName(oldConfig map[string]interface{}, id string) string {
	if name, ok := oldConfig["name"]; ok {
		return name.(string)
	}
	return fmt.Sprintf("Backup Task %s", id)
}

// ParseStoredConfig decodes a stored configuration document into a map. A
// document that will not parse yields an empty map rather than an error, which
// is what the update path has always done.
func ParseStoredConfig(configJSON string) map[string]interface{} {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &data); err != nil {
		return make(map[string]interface{})
	}
	return data
}

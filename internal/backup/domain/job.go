package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Config is a backup job's configuration as stored in the config_json column.
// It had six declarations before this one: an anonymous struct in the list
// handler, two more inside the create handler, two inside the update handler,
// and a named ExecutorBackupConfig in the executor.
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

// MissingFieldsError names what a request left out. The endpoints answer with
// it rather than storing a job stripped of everything the caller did not
// mention.
type MissingFieldsError struct{ Fields []string }

func (e *MissingFieldsError) Error() string {
	return "the request leaves out " + strings.Join(e.Fields, ", ") +
		"; an update replaces the whole configuration, so every field has to be sent"
}

// Complete reports the fields a request needs to describe a job, and what it
// is missing. An update replaces the stored configuration, so a client that
// means to change only the schedule and sends only the schedule used to have
// its database connection, its destination, its format and its compression
// emptied — with the job then failing at its next run, or backing up nothing
// at all.
func (r Request) Complete() error {
	var missing []string

	if strings.TrimSpace(r.SourceType) == "" {
		missing = append(missing, "sourceType")
	}
	if len(r.Database) == 0 {
		missing = append(missing, "database")
	}
	if len(r.Destination) == 0 {
		missing = append(missing, "destination")
	}
	if strings.TrimSpace(r.Schedule) == "" {
		missing = append(missing, "schedule")
	}

	if len(missing) > 0 {
		return &MissingFieldsError{Fields: missing}
	}
	return nil
}

// ConfigFrom builds the stored configuration from a request and a status.
//
// Every field comes from the request: an update replaces the configuration
// rather than merging into it, which is what PUT means. What is not acceptable
// is doing that silently — see Complete, which is what stops a request that
// left fields out from quietly emptying them.
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
	lastRun        RunOutcome
}

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

// RunOutcome is how a job's last run went, as it was last written to the
// control database. An unrun job has all three empty.
type RunOutcome struct {
	At      string
	Status  string
	Message string
}

// SetLastRun attaches the stored outcome of the last run. The store fills it in
// after building the job; nothing else writes it.
func (j *BackupJob) SetLastRun(outcome RunOutcome) { j.lastRun = outcome }

// LastRun reports how the last attempted run went. That is not the same as
// LastBackupTime, which moves only when a run succeeds: a job whose last run
// failed keeps the older successful timestamp and reports the failure here.
func (j BackupJob) LastRun() RunOutcome { return j.lastRun }

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

func IsTerminal(status string) bool {
	return status == RunCompleted || status == RunFailed
}

// Advance moves a run to a new status.
//
// The error goes with the status. A nil error used to leave whatever was there
// before, so a run that failed and was then retried successfully came back as
// "completed" carrying the text of the failure — which reads as a backup that
// both worked and did not.
func (r *Run) Advance(status, message string, err error) {
	r.Status = status
	r.Message = message
	if err != nil {
		r.Error = err.Error()
	} else {
		r.Error = ""
	}
	if IsTerminal(status) {
		now := time.Now()
		r.CompletedAt = &now
	}
}

// DeriveUpdateStatus reports the status an update keeps for a job. A status
// recorded in the stored configuration wins; otherwise the enable column
// decides.
func DeriveUpdateStatus(oldConfig map[string]interface{}, enable int) string {
	// The test is on the value, not on the key. A stored `"status": ""` used to
	// be carried through as an empty status, which the list endpoint then read as
	// absent and answered from the enable column instead — so one job had two
	// statuses depending on which endpoint was asked. The assertion was
	// unchecked too, so a status stored as a number panicked.
	if val, ok := oldConfig["status"].(string); ok && val != "" {
		return val
	}
	if enable == 1 {
		return StatusEnabled
	}
	return StatusDisabled
}

// DeriveUpdateName reports the name an update keeps when the request omits one:
// the stored name, or a generated one when there is none.
//
// The assertion used to be unchecked, so a stored name that was not a string —
// a number, say — panicked. The router installs no recovery, so that panic
// reached net/http and cut the connection rather than answering.
func DeriveUpdateName(oldConfig map[string]interface{}, id string) string {
	if name, ok := oldConfig["name"].(string); ok && name != "" {
		return name
	}
	return fmt.Sprintf("Backup Task %s", id)
}

// ParseStoredConfig decodes a stored configuration document into a map. A
// document that will not parse yields an empty map rather than an error, which
// is what the update path has always done.
func ParseStoredConfig(configJSON string) map[string]interface{} {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &data); err != nil || data == nil {
		return make(map[string]interface{})
	}
	return data
}

// RedactedPassword is the marker a masked password comes back as. It has to
// match what the HTTP layer sends out; see httpx.WithoutPassword.
const RedactedPassword = "********"

// CarryStoredPasswords replaces a redacted password in an update with the one
// already stored.
//
// The list endpoint masks passwords on the way out and the edit form sends the
// whole configuration back, so without this an edit that did not touch the
// password would save the mask as the password — and the next run would
// authenticate with "********". Masking a field the caller round-trips is only
// safe with this on the other side.
func CarryStoredPasswords(req Request, stored map[string]interface{}) Request {
	req.Database = carryPassword(req.Database, nested(stored, "database"))
	req.Destination = carryPassword(req.Destination, nested(stored, "destination"))
	return req
}

// nested reads one object out of a stored configuration document.
func nested(stored map[string]interface{}, key string) map[string]interface{} {
	inner, _ := stored[key].(map[string]interface{})
	return inner
}

func carryPassword(incoming, stored map[string]interface{}) map[string]interface{} {
	if incoming == nil {
		return incoming
	}
	text, ok := incoming["password"].(string)
	if !ok || text != RedactedPassword {
		return incoming
	}
	kept, ok := stored["password"].(string)
	if !ok {
		// Nothing stored to carry over, so the mask is not a password either.
		delete(incoming, "password")
		return incoming
	}
	incoming["password"] = kept
	return incoming
}

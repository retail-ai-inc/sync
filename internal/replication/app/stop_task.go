package app

import (
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra"
)

// CreateTask stores a new task and reports the request as normalised, so the
// endpoint can echo back exactly what was stored.
func CreateTask(req domain.Request) (id int64, stored domain.Request, now string, enable int, err error) {
	enable = req.Normalise()
	now = httpx.TimeNowStr()

	newID, err := infra.InsertTask(enable, now, domain.ConfigFrom(req))
	if err != nil {
		return 0, req, now, enable, err
	}
	return newID, req, now, enable, nil
}

// UpdateTask replaces a task's configuration and reports the request as
// normalised.
//
// A name or a status the request omits is taken from the stored task, not from
// the defaults a *new* task gets. Applying the create-time defaults meant an
// update that only meant to change the mappings renamed the task to "Sync Task"
// and stopped it — the backup side has always read its old name and status back,
// and the two endpoints disagreed about what omitting a field means.
func UpdateTask(id string, req domain.Request) (stored domain.Request, err error) {
	if req.TaskName == "" || req.Status == "" {
		if existing, readErr := infra.ReadTaskConfig(id); readErr == nil {
			if req.TaskName == "" {
				req.TaskName = existing.TaskName
			}
			if req.Status == "" {
				req.Status = existing.Status
			}
		}
	}

	enable := req.Normalise()
	return req, infra.UpdateTask(id, enable, httpx.TimeNowStr(), domain.ConfigFrom(req))
}

// DeleteTask removes a task.
func DeleteTask(id string) error { return infra.DeleteTask(id) }

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
func UpdateTask(id string, req domain.Request) (stored domain.Request, err error) {
	enable := req.Normalise()
	return req, infra.UpdateTask(id, enable, httpx.TimeNowStr(), domain.ConfigFrom(req))
}

// DeleteTask removes a task.
func DeleteTask(id string) error { return infra.DeleteTask(id) }

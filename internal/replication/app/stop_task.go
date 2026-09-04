package app

import (
	"fmt"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra"
	"log"
	"strconv"
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
// normalised. A name or a status the request omits is taken from the stored
// task, not from the defaults a *new* task gets.
func UpdateTask(id string, req domain.Request) (stored domain.Request, err error) {
	if existing, readErr := infra.ReadTaskConfig(id); readErr == nil {
		if req.TaskName == "" {
			req.TaskName = existing.TaskName
		}
		if req.Status == "" {
			req.Status = existing.Status
		}
		// An edit that did not touch the password sends back the mask the list
		// endpoint handed out; saving that would leave the task authenticating
		// with "********".
		req = domain.CarryStoredPasswords(req, existing)
	}

	enable := req.Normalise()
	return req, infra.UpdateTask(id, enable, httpx.TimeNowStr(), domain.ConfigFrom(req))
}

// PurgeCheckpoints removes what a deleted task left on its target. It is set
// at start-up, where the engines are already known: this package does not
// dispatch on engine and should not start.
//
// Nil means nothing is removed, which is what the tests and any caller that
// has not wired it get.
var PurgeCheckpoints func(cfg config.SyncConfig) error

// DeleteTask removes a task and then what it left behind.
//
// The row goes first. A position removed before the row would, if the delete
// then failed, leave a live task with no position and so a full re-copy on its
// next start -- worse than the orphan rows this is here to clean up. Failing to
// clean up is reported and not fatal for the same reason: a target that cannot
// be reached must not make a task undeletable.
func DeleteTask(id string) error {
	// Read before deleting: the clean-up needs the target's connection, and
	// after the row is gone there is nowhere to read it from.
	var stored config.SyncConfig
	readErr := fmt.Errorf("no task id")
	if n, convErr := strconv.Atoi(id); convErr == nil {
		stored, readErr = config.LoadSyncTask(n)
	}

	if err := infra.DeleteTask(id); err != nil {
		return err
	}

	if PurgeCheckpoints == nil || readErr != nil {
		return nil
	}
	if err := PurgeCheckpoints(stored); err != nil {
		log.Printf("[WARN] task %s is deleted, but its positions are still on the "+
			"target and a new task given the same id would resume from them: %v", id, err)
	}
	return nil
}

// StoredEndpointPassword reports the password a saved task holds for one of its
// endpoints, "source" or "target".
//
// The list endpoint masks passwords, so an edit form is filled with the mask
// rather than a credential. Saving an untouched field keeps what is stored;
// this is the same rule for the connection probe, which the edit form runs on
// open to list the source's tables. Without it, opening a task to look at it
// asks the operator to retype a password.
//
// It reports false rather than an error for every failure: the caller's next
// move is the same in each case — refuse the probe and say the mask could not
// be resolved.
func StoredEndpointPassword(taskID, role string) (string, bool) {
	config, err := infra.ReadTaskConfig(taskID)
	if err != nil {
		return "", false
	}

	connection := config.SourceConn
	if role == "target" {
		connection = config.TargetConn
	}
	password := connection["password"]
	return password, password != ""
}

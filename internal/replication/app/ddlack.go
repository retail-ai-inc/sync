package app

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/infra/ddlack"
)

// Letting a task past one schema change it refused.
//
// The refusal is the safe half and it already worked: a statement that would
// destroy replicated data stops the task instead of being carried into the
// disaster-recovery copy. What did not exist was a way back. A task halted on a
// DROP COLUMN in Tokyo stayed halted; the only ways on were to copy the task
// again -- hours, and not an option for a payment database -- or to move the
// stored position past the statement by hand, which skips every transaction
// beside it on every table.

// AcknowledgeDDL records that one statement may be passed over the next time
// the task reaches it. It never means "apply it": whatever the target needed,
// the operator does there, deliberately.
func AcknowledgeDDL(ctx context.Context, id, statement, by string) (ddlack.Acknowledgement, error) {
	number, err := taskNumber(id)
	if err != nil {
		return ddlack.Acknowledgement{}, err
	}
	if strings.TrimSpace(statement) == "" {
		return ddlack.Acknowledgement{}, fmt.Errorf("say which statement is acknowledged")
	}
	return ddlack.Add(ctx, number, statement, by)
}

// DDLAcknowledgements reports what a task is allowed to pass over, and what it
// already has. "What may this task skip" is a question a review has to be able
// to ask without reading the log.
func DDLAcknowledgements(ctx context.Context, id string) ([]ddlack.Acknowledgement, error) {
	number, err := taskNumber(id)
	if err != nil {
		return nil, err
	}
	return ddlack.List(ctx, number)
}

// RevokeDDLAcknowledgement withdraws one that has not been used.
func RevokeDDLAcknowledgement(ctx context.Context, id string, ackID int64) error {
	number, err := taskNumber(id)
	if err != nil {
		return err
	}
	return ddlack.Revoke(ctx, number, ackID)
}

// taskNumber reads the id and checks there is such a task, so an acknowledgement
// cannot be recorded against a task that does not exist -- where it would sit
// unused and unnoticed while the task everybody meant stayed blocked.
func taskNumber(id string) (int, error) {
	number, err := strconv.Atoi(id)
	if err != nil {
		return 0, fmt.Errorf("%q is not a task id", id)
	}
	if _, err := config.LoadSyncTask(number); err != nil {
		return 0, err
	}
	return number, nil
}

package mongodb

import (
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// A batch is replayed after an unclean stop — that is the whole basis of
// resuming from a position deliberately behind the truth.
func TestAReplayedSchemaChangeIsNotAFailure(t *testing.T) {
	for _, c := range []struct {
		code int
		name string
	}{
		{48, "NamespaceExists — the collection is already there"},
		{85, "IndexOptionsConflict — the index exists with these options"},
		{86, "IndexKeySpecsConflict — the index exists on these keys"},
		{68, "IndexAlreadyExists"},
		{27, "IndexNotFound — a replayed drop finds nothing to drop"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := mongo.CommandError{Code: int32(c.code), Message: c.name}
			if !alreadyInPlace(err) {
				t.Errorf("code %d was treated as a real failure, which stops the task "+
					"for ever on a change that has already been made", c.code)
			}
		})
	}
}

// TestARealSchemaFailureIsStillAFailure. The other half of the line: an error
// that is not "already done" has to stop the task, because the target's shape
// then differs from the source's and the rows that follow land wrong.
func TestARealSchemaFailureIsStillAFailure(t *testing.T) {
	for _, c := range []struct {
		code int
		name string
	}{
		{13, "Unauthorized"},
		{26, "NamespaceNotFound"},
		{11000, "DuplicateKey"},
		{50, "MaxTimeMSExpired"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := mongo.CommandError{Code: int32(c.code), Message: c.name}
			if alreadyInPlace(err) {
				t.Errorf("code %d was treated as already done, so a real failure "+
					"would be swallowed", c.code)
			}
		})
	}
}

// TestSomethingThatIsNotAServerErrorIsAFailure — a network error carries no
// code, and guessing that it means "already done" would skip the change.
func TestSomethingThatIsNotAServerErrorIsAFailure(t *testing.T) {
	for _, err := range []error{
		errors.New("connection refused"),
		fmt.Errorf("wrapped: %w", errors.New("i/o timeout")),
		nil,
	} {
		if alreadyInPlace(err) {
			t.Errorf("%v was treated as already done", err)
		}
	}
}

// TestAWrappedServerErrorIsStillRecognised, because the change is applied
// through helpers that add context to what they return.
func TestAWrappedServerErrorIsStillRecognised(t *testing.T) {
	inner := mongo.CommandError{Code: 48, Message: "NamespaceExists"}
	wrapped := fmt.Errorf("create the collection: %w", inner)

	if !alreadyInPlace(wrapped) {
		t.Error("a wrapped NamespaceExists was not recognised, so adding context " +
			"to an error would block the task")
	}
}

// TestAMalformedURIIsNotRetried. Waiting does not make a connection string
// parse, and retrying one for ever leaves a task that looks alive and replicates
// nothing.
func TestAMalformedURIIsNotRetried(t *testing.T) {
	inner := errors.New("error parsing uri: scheme must be mongodb")
	err := permanentURI{inner}

	if !err.Permanent() {
		t.Error("a malformed URI was not marked permanent")
	}
	if !errors.Is(err, inner) {
		t.Error("the reason was lost, so nothing downstream can say what was wrong")
	}
}

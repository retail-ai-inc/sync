package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Schema changes reaching the target, which used not to happen at all.
//
// Indexes were copied once, by the initial snapshot, and never again. An index
// added at the source afterwards therefore never existed on the target — and an
// index is usually added because a query became too slow, so the moment the
// target most needed it was exactly the moment it did not have it. A target that
// answers correctly but too slowly to serve is as much an outage as one that has
// lost rows, and it is harder to predict.
//
// MongoDB 6.0 and later report these changes on a change stream opened with
// showExpandedEvents. What arrives is a description of what happened rather than
// a statement to run, so each one is turned back into a command here.

// schemaChange is what a DDL event is carried as through the pipeline.
type schemaChange struct {
	// Kind is the change stream's operationType.
	Kind string
	// Collection is the source collection, before mapping.
	Collection string
	// Command is what to run on the target, with the collection name still to be
	// substituted for the mapped one.
	Command bson.D
	// Describe is what to write in the log.
	Describe string
}

// ddlDecision is what to do with one schema change.
type ddlDecision int

const (
	// ddlApply rewrites it for the target and runs it.
	ddlApply ddlDecision = iota
	// ddlSkip leaves it alone: it is not something this replicates.
	ddlSkip
	// ddlStop refuses to replicate it, because doing so would destroy
	// replicated data or leave the target in a shape nobody chose.
	ddlStop
)

// planSchemaChange decides what to do with one expanded change stream event.
//
// The policy mirrors the MySQL side: additive changes travel, destructive ones
// stop the task so somebody decides. A DROP that arrives unattended removes data
// on the disaster-recovery copy at the moment the copy is the only thing left of
// it, and a mistaken drop at the source is one of the reasons the copy exists.
func planSchemaChange(raw bson.Raw, collection string) (schemaChange, ddlDecision, string) {
	kind, _ := raw.Lookup("operationType").StringValueOK()
	description := raw.Lookup("operationDescription")

	switch kind {
	case "createIndexes":
		specs, ok := indexSpecs(description)
		if !ok {
			return schemaChange{}, ddlSkip, "carries no index specification"
		}
		return schemaChange{
			Kind:       kind,
			Collection: collection,
			Command:    bson.D{{Key: "createIndexes", Value: collection}, {Key: "indexes", Value: specs}},
			Describe:   fmt.Sprintf("create %d index(es) on %s", len(specs), collection),
		}, ddlApply, ""

	case "dropIndexes":
		specs, ok := indexSpecs(description)
		if !ok {
			return schemaChange{}, ddlSkip, "carries no index specification"
		}
		names := indexNames(specs)
		if len(names) == 0 {
			return schemaChange{}, ddlSkip, "names no index"
		}
		// Dropping an index destroys no data, and leaving a stale one behind
		// means the two schemas differ in a way nothing reports.
		return schemaChange{
			Kind:       kind,
			Collection: collection,
			Command:    bson.D{{Key: "dropIndexes", Value: collection}, {Key: "index", Value: names[0]}},
			Describe:   fmt.Sprintf("drop index %s on %s", names[0], collection),
		}, ddlApply, ""

	case "create":
		command := bson.D{{Key: "create", Value: collection}}
		for _, element := range describeElements(description) {
			switch element.Key() {
			case "idIndex":
				// The target's _id index is made by the create itself.
				continue
			}
			command = append(command, bson.E{Key: element.Key(), Value: element.Value()})
		}
		return schemaChange{
			Kind:       kind,
			Collection: collection,
			Command:    command,
			Describe:   fmt.Sprintf("create collection %s", collection),
		}, ddlApply, ""

	case "modify":
		command := bson.D{{Key: "collMod", Value: collection}}
		for _, element := range describeElements(description) {
			command = append(command, bson.E{Key: element.Key(), Value: element.Value()})
		}
		if len(command) == 1 {
			return schemaChange{}, ddlSkip, "describes no modification"
		}
		return schemaChange{
			Kind:       kind,
			Collection: collection,
			Command:    command,
			Describe:   fmt.Sprintf("modify collection %s", collection),
		}, ddlApply, ""

	case "drop":
		return schemaChange{}, ddlStop,
			fmt.Sprintf("drops %s, which holds replicated data", collection)

	case "dropDatabase":
		return schemaChange{}, ddlStop, "drops the whole database"

	case "rename":
		// The target's name comes from the task's mapping, so a rename at the
		// source either contradicts the mapping or makes it stale. Either way
		// somebody has to decide which name the target should carry.
		return schemaChange{}, ddlStop,
			fmt.Sprintf("renames %s, and the target's name comes from the task's mapping", collection)

	case "shardCollection", "reshardCollection", "refineCollectionShardKey":
		// The target's sharding is set up with the target, deliberately, and has
		// to match the source's for the data to land where it should. Copying
		// the change would reshard the disaster-recovery copy unattended.
		return schemaChange{}, ddlSkip,
			fmt.Sprintf("changes the sharding of %s, which belongs to whoever built the target", collection)
	}

	return schemaChange{}, ddlSkip, fmt.Sprintf("is a %q event", kind)
}

// indexSpecs reads the index specifications out of an operationDescription.
func indexSpecs(description bson.RawValue) ([]bson.Raw, bool) {
	if description.Type == 0 {
		return nil, false
	}
	doc, ok := description.DocumentOK()
	if !ok {
		return nil, false
	}
	array, err := doc.LookupErr("indexes")
	if err != nil {
		return nil, false
	}
	values, err := array.Array().Values()
	if err != nil {
		return nil, false
	}
	var specs []bson.Raw
	for _, value := range values {
		if spec, ok := value.DocumentOK(); ok {
			specs = append(specs, spec)
		}
	}
	return specs, len(specs) > 0
}

// indexNames reads the names out of index specifications.
func indexNames(specs []bson.Raw) []string {
	var names []string
	for _, spec := range specs {
		if name, ok := spec.Lookup("name").StringValueOK(); ok && name != "" {
			names = append(names, name)
		}
	}
	return names
}

// describeElements reads an operationDescription as its elements, or nothing
// when it is absent.
func describeElements(description bson.RawValue) []bson.RawElement {
	if description.Type == 0 {
		return nil
	}
	doc, ok := description.DocumentOK()
	if !ok {
		return nil
	}
	elements, err := doc.Elements()
	if err != nil {
		return nil
	}
	return elements
}

// applySchemaChange runs one schema change on the target.
//
// It runs outside a transaction because MongoDB's catalogue is not
// transactional: a DDL inside one is refused. That is why a schema change gets a
// batch of its own — a batch holding both a DDL and rows could not be applied
// atomically, and applying the halves separately is the torn batch this design
// exists to prevent.
func (a *Applier) applySchemaChange(ctx context.Context, event *domain.Event) error {
	change, ok := event.Payload.(schemaChange)
	if !ok {
		return domain.Unrecoverable(
			"a schema event for %s carries a %T rather than a schema change",
			event.NS, event.Payload)
	}

	target := a.targetFor(change.Collection)
	command := make(bson.D, len(change.Command))
	copy(command, change.Command)
	// The first element names the collection the command acts on, under whatever
	// name the task maps it to.
	command[0].Value = target

	database := a.Client.Database(a.TargetDatabase)
	err := database.RunCommand(ctx, command).Err()
	if err == nil {
		if a.Logger != nil {
			a.Logger.Infof("[MongoDB][DDL] Applied: %s", change.Describe)
		}
		return nil
	}

	// A change already in place is not a failure. Replaying a batch is expected
	// — that is what makes an interrupted batch recoverable — and an index that
	// exists, or a collection that does, means the replay has caught up.
	if alreadyInPlace(err) {
		if a.Logger != nil {
			a.Logger.Infof("[MongoDB][DDL] Already in place: %s", change.Describe)
		}
		return nil
	}
	return fmt.Errorf("apply %s: %w", change.Describe, err)
}

// alreadyInPlace reports whether a schema change failed because it had been made
// already, which is what a replayed batch looks like.
func alreadyInPlace(err error) bool {
	var serverErr mongo.ServerError
	if !errors.As(err, &serverErr) {
		return false
	}
	// NamespaceExists, IndexOptionsConflict, IndexKeySpecsConflict,
	// IndexAlreadyExists, IndexNotFound — the last because a dropIndexes replayed
	// after it succeeded finds nothing to drop.
	for _, code := range []int{48, 85, 86, 68, 27} {
		if serverErr.HasErrorCode(code) {
			return true
		}
	}
	return false
}

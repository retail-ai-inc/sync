package app

import (
	"errors"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
)

// MaxPageSize bounds what one request may ask for.
//
// There was no bound: ?pageSize=1000000 was accepted and the whole table was
// read through a connection pool that holds exactly one connection — which every
// other part of the process is waiting on, replication checkpoints included.
const MaxPageSize = 200

// ErrBadPage means the paging parameters are not a page.
var ErrBadPage = errors.New("current must be at least 1 and pageSize between 1 and 200")

// ListUsers returns one page of the user directory together with the total
// number of users. Sensitive and internal columns are stripped.
//
// The paging is done by the database. It used to read every row and slice the
// result in memory, and an out-of-range parameter was silently replaced with a
// default rather than reported.
func ListUsers(current, pageSize int) (page []map[string]interface{}, total int, err error) {
	if current < 1 || pageSize < 1 || pageSize > MaxPageSize {
		return nil, 0, ErrBadPage
	}

	users, total, err := infra.PageOfUsers((current-1)*pageSize, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return domain.PublicUsers(users), total, nil
}

// AccessRejection names a request the access change refuses outright. The
// handler answers each with a 200 and success:false, so they are not errors.
type AccessRejection string

const (
	RejectInvalidAccess   AccessRejection = "Invalid permission type, must be admin or guest"
	RejectInvalidStatus   AccessRejection = "Invalid status, must be active or inactive"
	RejectNoSuchUser      AccessRejection = "User does not exist"
	RejectNothingToUpdate AccessRejection = "No fields to update"
	RejectLastAdmin       AccessRejection = "The last administrator cannot be removed"
)

// ChangeUserAccess applies an access level and a status to a user. It returns
// the user's profile on success; a non-empty rejection when the request is
// refused with a 200; or an error when the store failed.
func ChangeUserAccess(userID, access, status string) (map[string]interface{}, AccessRejection, error) {
	if !domain.IsValidAccess(access) {
		return nil, RejectInvalidAccess, nil
	}
	if !domain.IsValidStatus(status) {
		return nil, RejectInvalidStatus, nil
	}

	user, err := infra.UpdateUserAccessAndStatus(userID, access, status)
	switch {
	case err == infra.ErrNoSuchUser:
		return nil, RejectNoSuchUser, nil
	case err == infra.ErrNothingToUpdate:
		return nil, RejectNothingToUpdate, nil
	case err != nil:
		return nil, "", err
	}
	return user, "", nil
}

// RemoveUser deletes a user. A RejectNoSuchUser rejection means the userId was
// not in the table; RejectLastAdmin means removing them would leave nobody who
// can grant the level again; an error means the store failed.
func RemoveUser(userID string) (AccessRejection, error) {
	err := infra.DeleteUser(userID)
	switch {
	case err == infra.ErrNoSuchUser:
		return RejectNoSuchUser, nil
	case err == infra.ErrLastAdmin:
		return RejectLastAdmin, nil
	case err != nil:
		return "", err
	}
	return "", nil
}

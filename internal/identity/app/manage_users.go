package app

import (
	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
)

// ListUsers returns one page of the user directory together with the total
// number of users. Sensitive and internal columns are stripped.
func ListUsers(current, pageSize int) (page []map[string]interface{}, total int, err error) {
	users, err := infra.GetAllUsers()
	if err != nil {
		return nil, 0, err
	}
	return domain.PageOfUsers(users, current, pageSize), len(users), nil
}

// AccessRejection names a request the access change refuses outright. The
// handler answers each with a 200 and success:false, so they are not errors.
type AccessRejection string

const (
	RejectInvalidAccess   AccessRejection = "Invalid permission type, must be admin or guest"
	RejectInvalidStatus   AccessRejection = "Invalid status, must be active or inactive"
	RejectNoSuchUser      AccessRejection = "User does not exist"
	RejectNothingToUpdate AccessRejection = "No fields to update"
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
// not in the table; an error means the store failed.
func RemoveUser(userID string) (AccessRejection, error) {
	err := infra.DeleteUser(userID)
	switch {
	case err == infra.ErrNoSuchUser:
		return RejectNoSuchUser, nil
	case err != nil:
		return "", err
	}
	return "", nil
}

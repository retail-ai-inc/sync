package sqlite

// The fallback for SYNC_DB_PATH used to be derived from runtime.Caller, which
// is the path of this source file on the machine that compiled the binary. In a
// container built anywhere else that directory does not exist; OpenSQLiteDB
// created it, created an empty database inside it, and the process started with
// no sync tasks and nothing to say so. A deployment that forgot to set
// SYNC_DB_PATH looked exactly like a deployment with nothing configured.
//
// There is no build-time path any more. Unset means ./sync.db relative to the
// working directory, and OpenSQLiteDB says out loud when it has had to create a
// control database that was not already there.

package sqlite

// The fallback for SYNC_DB_PATH used to be derived from runtime.Caller, which
// is the path of this source file on the machine that compiled the binary. In
// a container built anywhere else that directory does not exist; OpenSQLiteDB
// created it, created an empty database inside it, and the process started
// with no sync tasks and nothing to say so.

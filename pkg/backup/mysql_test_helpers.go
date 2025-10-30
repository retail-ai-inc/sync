package backup

// TestParseMySQLConnectionURL is a test helper that exposes parseMySQLConnectionURL for testing
func TestParseMySQLConnectionURL(url string) (host, port, username, password string) {
	return parseMySQLConnectionURL(url)
}

// TestBuildMySQLConnectionString is a test helper that exposes buildMySQLConnectionString for testing
func TestBuildMySQLConnectionString(url, username, password string) (host, port, user, pass string) {
	return buildMySQLConnectionString(url, username, password)
}

// TestMaskMySQLPassword is a test helper that exposes maskMySQLPassword for testing
func (e *BackupExecutor) TestMaskMySQLPassword(args []string) string {
	return e.maskMySQLPassword(args)
}

// TestBuildMySQLSelectQuery is a test helper that exposes buildMySQLSelectQuery for testing
func (e *BackupExecutor) TestBuildMySQLSelectQuery(table string, config ExecutorBackupConfig) string {
	return e.buildMySQLSelectQuery(table, config)
}

// TestConvertTimeRangeQueryForMySQL is a test helper that exposes convertTimeRangeQueryForMySQL for testing
func (e *BackupExecutor) TestConvertTimeRangeQueryForMySQL(query map[string]interface{}) string {
	return e.convertTimeRangeQueryForMySQL(query)
}

// TestExtractTablePrefix is a test helper that exposes extractTablePrefix for testing
func (e *BackupExecutor) TestExtractTablePrefix(tableName string) string {
	return e.extractTablePrefix(tableName)
}


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

// TestGroupTablesByPrefix is a test helper that exposes groupTablesByPrefix for testing
func (e *BackupExecutor) TestGroupTablesByPrefix(tables []string) map[string][]string {
	return e.groupTablesByPrefix(tables)
}

// TestExtractTimeRange is a test helper that exposes extractTimeRange for testing
func (e *BackupExecutor) TestExtractTimeRange(query map[string]interface{}) *TimeRange {
	return e.extractTimeRange(query)
}

// TestIsTableRelevantForTimeRange is a test helper that exposes isTableRelevantForTimeRange for testing
func (e *BackupExecutor) TestIsTableRelevantForTimeRange(tableName string, timeRange *TimeRange) bool {
	return e.isTableRelevantForTimeRange(tableName, timeRange)
}

// TestExtractTableTimePattern is a test helper that exposes extractTableTimePattern for testing
func (e *BackupExecutor) TestExtractTableTimePattern(tableName string) *TimeRange {
	return e.extractTableTimePattern(tableName)
}

// TestFilterRelevantTables is a test helper that exposes filterRelevantTables for testing
func (e *BackupExecutor) TestFilterRelevantTables(tables []string, queryConditions map[string]map[string]interface{}, groupName string) []string {
	return e.filterRelevantTables(tables, queryConditions, groupName)
}

// TestBuildMongoDBConnectionString is a test helper that exposes buildMongoDBConnectionString for testing
func TestBuildMongoDBConnectionString(url, username, password string) string {
	return buildMongoDBConnectionString(url, username, password)
}

// TestMaskSensitiveArgs is a test helper that exposes maskSensitiveArgs for testing
func (e *BackupExecutor) TestMaskSensitiveArgs(args []string) string {
	return e.maskSensitiveArgs(args)
}

// TestConvertTimeRangeQuery is a test helper that exposes convertTimeRangeQuery for testing
func (e *BackupExecutor) TestConvertTimeRangeQuery(query map[string]interface{}) map[string]interface{} {
	return e.convertTimeRangeQuery(query)
}

// TestCleanQueryStringValues is a test helper that exposes cleanQueryStringValues for testing
func TestCleanQueryStringValues(queryObj map[string]interface{}) map[string]interface{} {
	return cleanQueryStringValues(queryObj)
}

// TestParseYearMonth is a test helper that exposes parseYearMonth for testing
func TestParseYearMonth(yyyymm string) (int, int, error) {
	year, month, err := parseYearMonth(yyyymm)
	return year, int(month), err
}

// TestParseYear is a test helper that exposes parseYear for testing
func TestParseYear(yyyy string) (int, error) {
	return parseYear(yyyy)
}

// TestProcessFileNamePattern is a test helper that exposes processFileNamePattern for testing
func TestProcessFileNamePattern(pattern, tableName string) string {
	return processFileNamePattern(pattern, tableName)
}

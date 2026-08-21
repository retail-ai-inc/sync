package domain

// Checkpoints are the four places a task records how far it has replicated,
// one per engine. They were carried as four unrelated strings on every
// configuration struct; grouping them names what they have in common.
//
// Only the one belonging to the task's engine is ever used. Nothing validates
// that the path is writable, or that it is not shared with another task.
type Checkpoints struct {
	Postgres string
	MySQL    string
	MongoDB  string
	Redis    string
}

// CheckpointsOf reads the four paths out of a configuration.
func CheckpointsOf(c Config) Checkpoints {
	return Checkpoints{
		Postgres: c.PgPositionPath,
		MySQL:    c.MysqlPositionPath,
		MongoDB:  c.MongodbResumeTokenPath,
		Redis:    c.RedisPositionPath,
	}
}

// For reports the checkpoint path an engine uses, empty when the engine has
// none configured or is not one of the four.
func (cp Checkpoints) For(engine string) string {
	switch lower(engine) {
	case "postgresql":
		return cp.Postgres
	case "mysql", "mariadb":
		return cp.MySQL
	case "mongodb":
		return cp.MongoDB
	case "redis":
		return cp.Redis
	}
	return ""
}

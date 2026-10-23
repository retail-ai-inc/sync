package state

type StateStore interface {
	Save(key string, value []byte) error
	Load(key string) ([]byte, error)
}

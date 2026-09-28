package infra

// Stages a store operation can fail at. The handlers have always answered with
// a different message per stage, so the store reports which one it reached
// rather than collapsing everything into one error.
const (
	StageConnect = "connect"
	StageBegin   = "begin"
	StageCheck   = "check"
	StageQuery   = "query"
	StageUpdate  = "update"
	StageDelete  = "delete"
	StageCommit  = "commit"
)

type Fault struct {
	Stage string
	Err   error
}

func (f *Fault) Error() string { return f.Stage + ": " + f.Err.Error() }

func (f *Fault) Unwrap() error { return f.Err }

func faultAt(stage string, err error) *Fault { return &Fault{Stage: stage, Err: err} }

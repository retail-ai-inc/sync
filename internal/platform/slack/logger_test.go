package slack

import (
	"io"

	"github.com/sirupsen/logrus"
)

// quietLogger returns a logger that discards output, so tests do not flood the
// test log.
func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

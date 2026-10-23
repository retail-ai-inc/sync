package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/sirupsen/logrus"
)

// A recorder for the warnings these paths exist to produce. The warning is the
// whole behaviour -- nothing else changes -- so an assertion on the metric or
// the return value would prove nothing.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (r *recordingLogger) logger() *logrus.Logger {
	log := logrus.New()
	log.SetOutput(r)
	log.SetLevel(logrus.DebugLevel)
	return log
}

func (r *recordingLogger) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, string(p))
	return len(p), nil
}

func (r *recordingLogger) saw(substring string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, line := range r.lines {
		if strings.Contains(line, substring) {
			return true
		}
	}
	return false
}

// TestAFillingSnapshotQueueIsWarnedAbout covers the warning that names the
// consequence: if the queue fills, the stream stops being read and the pinned
// point can age out of the source's log, which costs the whole copy. The
// threshold is eight tenths.
func TestAFillingSnapshotQueueIsWarnedAbout(t *testing.T) {
	log := &recordingLogger{}
	r := &Runner{Opts: Options{
		ReportInterval: time.Millisecond,
		Logger:         log.logger(),
		Engine:         "Test",
	}}

	queue := make(chan *domain.Event, 10)
	for i := 0; i < 8; i++ {
		queue <- &domain.Event{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := r.watchQueuePressure(ctx, queue)

	deadline := time.Now().Add(2 * time.Second)
	for !log.saw("hold 8 of the queue's 10 places") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop()

	if !log.saw("hold 8 of the queue's 10 places") {
		t.Error("a queue at eight tenths was not warned about")
	}
}

// TestTheQueueWarningIsSaidOnce: it is emitted from a ticker, so repeating it
// would fill the log with the same line for as long as the copy runs.
func TestTheQueueWarningIsSaidOnce(t *testing.T) {
	log := &recordingLogger{}
	r := &Runner{Opts: Options{
		ReportInterval: time.Millisecond,
		Logger:         log.logger(),
		Engine:         "Test",
	}}

	queue := make(chan *domain.Event, 10)
	for i := 0; i < 9; i++ {
		queue <- &domain.Event{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := r.watchQueuePressure(ctx, queue)
	time.Sleep(100 * time.Millisecond)
	stop()

	log.mu.Lock()
	defer log.mu.Unlock()
	count := 0
	for _, line := range log.lines {
		if strings.Contains(line, "places") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("the warning was said %d times over a hundred ticks, want once", count)
	}
}

func TestAQuietQueueIsNotWarnedAbout(t *testing.T) {
	log := &recordingLogger{}
	r := &Runner{Opts: Options{
		ReportInterval: time.Millisecond,
		Logger:         log.logger(),
		Engine:         "Test",
	}}

	queue := make(chan *domain.Event, 10)
	queue <- &domain.Event{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := r.watchQueuePressure(ctx, queue)
	time.Sleep(50 * time.Millisecond)
	stop()

	if log.saw("places") {
		t.Error("a queue at one tenth was warned about")
	}
}

// TestStoppingTheWatchTwiceIsSafe: the stop function is returned to a caller
// that may defer it and also call it on an early return.
func TestStoppingTheWatchTwiceIsSafe(t *testing.T) {
	r := &Runner{Opts: Options{ReportInterval: time.Hour, Logger: quietLogger()}}
	stop := r.watchQueuePressure(context.Background(), make(chan *domain.Event, 1))
	stop()
	stop()
}

// TestASourceThatWillNotSayIsAskedOnce covers the refusal path. A guess would
// be worse than nothing here: the headroom is only read when somebody is
// deciding restart against re-copy, so a made-up number decides it wrongly.
func TestASourceThatWillNotSayIsAskedOnce(t *testing.T) {
	log := &recordingLogger{}
	reader := &windowedReader{fakeReader: &fakeReader{},
		err: errors.New("the source refused")}
	labels := metrics.Labels{"task": t.Name()}
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	r := &Runner{
		Reader: reader,
		Opts:   Options{Labels: labels, Logger: log.logger(), Engine: "Test"},
	}

	now := time.Now()
	r.reportRetention(context.Background(), now, 5, true)
	r.reportRetention(context.Background(), now.Add(time.Hour), 5, true)
	r.reportRetention(context.Background(), now.Add(2*time.Hour), 5, true)

	if reader.calls != 1 {
		t.Errorf("the source was asked %d times after refusing once, want 1", reader.calls)
	}
	if !log.saw("did not say how far its log reaches") {
		t.Error("nothing said why no headroom is published")
	}
	for _, sample := range metrics.Default.Snapshot(metrics.RetentionWindowSeconds) {
		if sample.Labels["task"] == t.Name() {
			t.Error("a window was published for a source that refused to say")
		}
	}
}

// TestASourceThatIsNotReadyYetIsAskedAgain covers the other half: some sources
// must be measured twice before they can answer, so ErrWindowNotYet is not a
// refusal and must not latch.
func TestASourceThatIsNotReadyYetIsAskedAgain(t *testing.T) {
	reader := &windowedReader{fakeReader: &fakeReader{}, err: domain.ErrWindowNotYet}
	labels := metrics.Labels{"task": t.Name()}
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	r := &Runner{
		Reader: reader,
		Opts:   Options{Labels: labels, Logger: quietLogger(), Engine: "Test"},
	}

	now := time.Now()
	r.reportRetention(context.Background(), now, 5, true)
	r.reportRetention(context.Background(), now.Add(time.Hour), 5, true)

	if reader.calls != 2 {
		t.Errorf("the source was asked %d times, want 2 -- not-yet must not latch",
			reader.calls)
	}
}

// TestAWindowOfZeroIsTreatedAsNoAnswer: a zero-length window would publish a
// headroom equal to minus the lag, which reads as an emergency on every task.
func TestAWindowOfZeroIsTreatedAsNoAnswer(t *testing.T) {
	reader := &windowedReader{fakeReader: &fakeReader{}, window: 0}
	labels := metrics.Labels{"task": t.Name()}
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	r := &Runner{
		Reader: reader,
		Opts:   Options{Labels: labels, Logger: quietLogger(), Engine: "Test"},
	}

	now := time.Now()
	r.reportRetention(context.Background(), now, 5, true)
	r.reportRetention(context.Background(), now.Add(time.Hour), 5, true)

	if reader.calls != 1 {
		t.Errorf("the source was asked %d times after answering zero, want 1", reader.calls)
	}
	for _, sample := range metrics.Default.Snapshot(metrics.RetentionWindowSeconds) {
		if sample.Labels["task"] == t.Name() {
			t.Error("a window of zero was published")
		}
	}
}

package main

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/app"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	replicationapp "github.com/retail-ai-inc/sync/internal/replication/app"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/sirupsen/logrus"
)

const (
	// configReloadInterval is how often the stored configuration is re-read, and
	// how often a task that stopped by itself is considered for restart.
	configReloadInterval = 10 * time.Second
	// drainTimeout is how long a task is given to finish what it was applying
	// after being asked to stop.
	drainTimeout = 30 * time.Second
	// restartBackoff is the wait before restarting a task that stopped on its own,
	// doubling each time it stops again.
	restartBackoff = 10 * time.Second
	// maxRestartBackoff caps that wait: a source down for an hour should be
	// retried every few minutes, not once more at day's end.
	maxRestartBackoff = 5 * time.Minute
)

type runningTask struct {
	// fingerprint is the configuration the task was started from. One that no
	// longer matches the stored configuration is restarted; an unchanged one is
	// left alone.
	fingerprint string
	cancel      context.CancelFunc
	done        chan struct{}
	// err is why the task stopped, written before done is closed and read only
	// after, so the close orders the two.
	err error

	// attempts counts consecutive stops, and nextAttempt is when to try again.
	attempts    int
	nextAttempt time.Time
	// blocked means the task stopped for a reason retrying cannot fix, so it stays
	// stopped until somebody changes something.
	blocked bool
}

func (t *runningTask) exited() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

func fingerprint(sc config.SyncConfig) string {
	encoded, err := json.Marshal(sc)
	if err != nil {
		// An unencodable configuration counts as changed, restarting the task rather
		// than leaving it on something unknown.
		return time.Now().String()
	}
	return string(encoded)
}

// globalFingerprint renders the settings belonging to the process rather than
// one task. Comparing only the task list meant a changed monitor interval or
// webhook took effect on the next restart and not before.
func globalFingerprint(cfg *config.Config) string {
	encoded, err := json.Marshal(struct {
		Monitoring bool
		Interval   time.Duration
		Webhook    string
		Channel    string
		LogLevel   string
	}{
		cfg.EnableTableRowCountMonitoring, cfg.MonitorInterval,
		cfg.SlackWebhookURL, cfg.SlackChannel, cfg.LogLevel,
	})
	if err != nil {
		return time.Now().String()
	}
	return string(encoded)
}

// supervisor keeps the running syncers in step with the stored configuration.
// One context for all of them meant any change cancelled every syncer, so
// editing one task's tables stopped replication for every other task, payments
// included.
type supervisor struct {
	log *logrus.Logger
	// global is where syncers read process-wide settings from, currently the Slack
	// credentials the MongoDB syncer alerts through.
	global  *config.Config
	running map[int]*runningTask

	monitorFingerprint string
	monitorCancel      context.CancelFunc

	// build resolves a task's configuration to the function that runs it, a field
	// so a test can substitute a stub for syncers that need databases.
	build func(config.SyncConfig, *config.Config, *logrus.Logger) func(context.Context) error
}

func newSupervisor(log *logrus.Logger) *supervisor {
	return &supervisor{log: log, running: map[int]*runningTask{}, build: syncerFor}
}

func (s *supervisor) apply(ctx context.Context, cfg *config.Config) {
	s.global = cfg

	desired := map[int]config.SyncConfig{}
	for _, sc := range cfg.SyncConfigs {
		if sc.Enable {
			desired[sc.ID] = sc
		}
	}

	for id, task := range s.running {
		wanted, keep := desired[id]
		if keep && fingerprint(wanted) == task.fingerprint {
			continue
		}
		if keep {
			s.log.Infof("Task %d has changed, restarting it", id)
		} else {
			s.log.Infof("Task %d is no longer enabled, stopping it", id)
		}
		s.stop(id)
	}

	for id, sc := range desired {
		existing, already := s.running[id]
		switch {
		case !already:
			s.start(ctx, sc)
		case existing.exited():
			s.reconsider(ctx, sc, existing)
		}
	}

	s.applyMonitoring(ctx, cfg)
}

// reconsider decides what to do about a task that stopped by itself. Nothing
// used to: its goroutine returned, its fingerprint was unchanged, and it was
// never looked at again.
func (s *supervisor) reconsider(ctx context.Context, sc config.SyncConfig, task *runningTask) {
	if task.blocked {
		return
	}

	if domain.IsUnrecoverable(task.err) {
		// Restarting would fail identically for as long as anybody let it, and the
		// looping would bury the one thing somebody needs to be told.
		task.blocked = true
		metrics.SetTaskBlocked(taskLabels(sc), true)
		s.log.Errorf("Task %d has stopped and will not be restarted: %v. "+
			"Replication for it has halted until this is dealt with.", sc.ID, task.err)
		return
	}

	if time.Now().Before(task.nextAttempt) {
		return
	}

	attempts := task.attempts
	wait := restartBackoff << attempts
	if wait > maxRestartBackoff || wait <= 0 {
		wait = maxRestartBackoff
	}

	s.log.Warnf("Task %d stopped (%v); restarting it, attempt %d",
		sc.ID, task.err, attempts+1)
	metrics.CountRestart(taskLabels(sc))

	s.start(ctx, sc)
	if restarted, ok := s.running[sc.ID]; ok {
		restarted.attempts = attempts + 1
		restarted.nextAttempt = time.Now().Add(wait)
	}
}

// taskLabels identify a task in the supervisor's metrics — deliberately the
// subset every engine agrees on, so a dashboard can sum across them.
func taskLabels(sc config.SyncConfig) metrics.Labels {
	return metrics.Labels{
		"task":   strconv.Itoa(sc.ID),
		"engine": sc.Type,
	}
}

func (s *supervisor) start(parentCtx context.Context, sc config.SyncConfig) {
	syncer := s.build(sc, s.global, s.log)
	if syncer == nil {
		s.log.Errorf("Unknown sync type: %s", sc.Type)
		return
	}

	ctx, cancel := context.WithCancel(parentCtx)
	done := make(chan struct{})
	task := &runningTask{fingerprint: fingerprint(sc), cancel: cancel, done: done}
	s.running[sc.ID] = task

	go func() {
		defer close(done)
		// A panic here used to end the process, and with it every other task: four
		// replication links stopped because one of them dereferenced something.
		// It becomes this task's error, which reconsider restarts with backoff --
		// and the pipeline resumes from its stored position, so the restart begins
		// from a point the target agrees with.
		task.err = domain.Guard(func() error { return syncer(ctx) })
	}()
	metrics.SetTaskBlocked(taskLabels(sc), false)
	s.log.Infof("Task %d (%s) started", sc.ID, sc.Type)
}

func (s *supervisor) stop(id int) {
	task, ok := s.running[id]
	if !ok {
		return
	}
	delete(s.running, id)

	task.cancel()
	select {
	case <-task.done:
	case <-time.After(drainTimeout):
		s.log.Warnf("Task %d did not finish within %v of being asked to stop",
			id, drainTimeout)
	}
}

// stopAll asks every task to stop and waits for them together, so shutdown
// takes one drain timeout rather than one per task.
func (s *supervisor) stopAll() {
	for _, task := range s.running {
		task.cancel()
	}

	deadline := time.After(drainTimeout)
	for id, task := range s.running {
		select {
		case <-task.done:
		case <-deadline:
			s.log.Warnf("Task %d did not finish within %v of being asked to stop; "+
				"whatever it had buffered is lost", id, drainTimeout)
		}
	}
	s.running = map[int]*runningTask{}

	if s.monitorCancel != nil {
		s.monitorCancel()
		s.monitorCancel = nil
	}
	// Cancelling only asks. Every monitor writes to the control database, so
	// returning early leaves goroutines opening SQLite after the process believes
	// it stopped.
	app.WaitForWatchers()
}

// applyMonitoring brings the process-wide watchers into line. Lag alerting runs
// whatever the row-count switch says: how far behind the copy is, is not an
// optional statistic.
func (s *supervisor) applyMonitoring(ctx context.Context, cfg *config.Config) {
	wanted := globalFingerprint(cfg)
	if wanted == s.monitorFingerprint && s.monitorCancel != nil {
		return
	}
	s.monitorFingerprint = wanted

	if s.monitorCancel != nil {
		s.monitorCancel()
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	s.monitorCancel = cancel

	app.StartLagAlerting(monitorCtx, cfg, s.log)
	app.StartConsistencyChecks(monitorCtx, cfg, s.log)
	// Trimming runs whether or not row-count monitoring is on: rows written before
	// it was turned off do not remove themselves.
	app.StartMonitoringRetention(monitorCtx, s.log)
	if cfg.EnableTableRowCountMonitoring {
		app.StartRowCountMonitoring(monitorCtx, cfg, s.log, cfg.MonitorInterval)
	}
}

// syncerFor reports the Start function for a task's engine, or nil when this
// build does not replicate it. The comparison folds case, as the monitoring
// side already did.
func syncerFor(sc config.SyncConfig, global *config.Config, log *logrus.Logger) func(context.Context) error {
	switch strings.ToLower(strings.TrimSpace(sc.Type)) {
	case "mongodb":
		return func(ctx context.Context) error {
			return replicationapp.NewMongoDBSyncer(sc, global, log).Start(ctx)
		}
	case "mysql", "mariadb":
		return func(ctx context.Context) error {
			return replicationapp.NewMySQLSyncer(sc, log).Start(ctx)
		}
	case "postgresql":
		return func(ctx context.Context) error {
			return replicationapp.NewPostgreSQLSyncer(sc, log).Start(ctx)
		}
	case "redis":
		return func(ctx context.Context) error {
			return replicationapp.NewRedisSyncer(sc, log).Start(ctx)
		}
	}
	return nil
}

// runSyncTasks keeps the running syncers in step with the stored configuration
// until its context is cancelled.
func runSyncTasks(parentCtx context.Context, log *logrus.Logger, cfg *config.Config) {
	s := newSupervisor(log)
	s.apply(parentCtx, cfg)

	ticker := time.NewTicker(configReloadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-parentCtx.Done():
			s.stopAll()
			return
		case <-ticker.C:
			newConfig, err := config.NewConfig()
			if err != nil {
				// One unreadable read is no reason to stop replicating; the tasks go on
				// with the configuration they have.
				log.Errorf("Could not re-read the configuration, keeping the "+
					"running tasks as they are: %v", err)
				continue
			}
			s.apply(parentCtx, newConfig)
		}
	}
}

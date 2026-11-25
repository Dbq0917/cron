package cron

import (
	"sync/atomic"
	"time"
)

// EntryStats is an immutable, point-in-time snapshot of the observability
// information collected for a single cron entry.
type EntryStats struct {
	// Next is the next scheduled activation time, or the zero time if the
	// entry is not scheduled.
	Next time.Time

	// Prev is the most recent scheduled activation time, or the zero time if
	// the entry has never been activated.
	Prev time.Time

	// Activations is the number of times the scheduler dispatched this entry.
	// Every due activation is counted exactly once, whether it ran, was
	// skipped or was late.
	Activations uint64

	// Runs is the number of activations whose job body actually executed to
	// completion.
	Runs uint64

	// Skips is the number of activations that did not run because an outer
	// wrapper (e.g. SkipIfStillRunning) suppressed the execution while a
	// previous invocation was still in progress.
	Skips uint64

	// Misses is the number of scheduled activations that fired late because
	// the scheduler was blocked or asleep at their scheduled time. A missed
	// activation is distinct from a skipped one: skipping is a job-level
	// decision, missing is a timing problem.
	Misses uint64

	// Running is the number of invocations currently in progress.
	Running int64

	// LastStart is the time the most recent invocation started.
	LastStart time.Time

	// LastFinish is the time the most recent invocation finished.
	LastFinish time.Time

	// LastDuration is the wall-clock duration of the most recent completed run.
	LastDuration time.Duration

	// TotalDuration is the cumulative wall-clock time spent running the job.
	TotalDuration time.Duration
}

// entryStats collects the live observability data for an entry.
//
// Every field touched on the hot path is updated with atomic operations, so
// recording an activation or a run never takes a lock. interval and hasPrev
// are only read and written by the single scheduler goroutine and therefore
// need no synchronization.
type entryStats struct {
	// The 64-bit words are declared first so they remain 64-bit aligned even
	// on 32-bit platforms, as required by the atomic operations below.
	lastStart   int64
	lastFinish  int64
	running     int64
	lastDur     int64
	totalDur    int64
	activations uint64
	runs        uint64
	misses      uint64

	location *time.Location
	interval time.Duration
	hasPrev  bool
}

func newEntryStats(location *time.Location) *entryStats {
	return &entryStats{location: location}
}

// observe is invoked by the scheduler goroutine whenever an entry reaches a
// scheduled time. scheduled is the time the entry was due, now is the actual
// wake time and nextScheduled is the following activation time. It records the
// activation, derives the schedule interval and counts late (missed) runs.
func (s *entryStats) observe(scheduled, now, nextScheduled time.Time) {
	atomic.AddUint64(&s.activations, 1)

	if s.hasPrev && s.interval > 0 {
		if late := now.Sub(scheduled); late >= s.interval {
			atomic.AddUint64(&s.misses, uint64(late/s.interval))
		}
	}

	if !scheduled.IsZero() && !nextScheduled.IsZero() {
		if interval := nextScheduled.Sub(scheduled); interval > 0 {
			s.interval = interval
			s.hasPrev = true
		}
	}
}

func (s *entryStats) recordStart(now time.Time) {
	atomic.AddInt64(&s.running, 1)
	atomic.StoreInt64(&s.lastStart, now.UnixNano())
}

func (s *entryStats) recordFinish(start time.Time) {
	finish := time.Now()
	atomic.AddInt64(&s.running, -1)
	atomic.AddUint64(&s.runs, 1)
	atomic.StoreInt64(&s.lastFinish, finish.UnixNano())
	duration := finish.Sub(start)
	atomic.StoreInt64(&s.lastDur, int64(duration))
	atomic.AddInt64(&s.totalDur, int64(duration))
}

func (s *entryStats) snapshot(next, prev time.Time) EntryStats {
	runs := atomic.LoadUint64(&s.runs)
	activations := atomic.LoadUint64(&s.activations)
	running := atomic.LoadInt64(&s.running)

	// An activation that is still in progress is not a skip. The value is
	// clamped because a freshly dispatched goroutine may not have incremented
	// Running yet when the snapshot is taken.
	skips := int64(activations) - int64(runs) - running
	if skips < 0 {
		skips = 0
	}

	return EntryStats{
		Next:          next,
		Prev:          prev,
		Activations:   activations,
		Runs:          runs,
		Skips:         uint64(skips),
		Misses:        atomic.LoadUint64(&s.misses),
		Running:       running,
		LastStart:     nanoToTime(atomic.LoadInt64(&s.lastStart), s.location),
		LastFinish:    nanoToTime(atomic.LoadInt64(&s.lastFinish), s.location),
		LastDuration:  time.Duration(atomic.LoadInt64(&s.lastDur)),
		TotalDuration: time.Duration(atomic.LoadInt64(&s.totalDur)),
	}
}

func nanoToTime(nano int64, location *time.Location) time.Time {
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano).In(location)
}

// observedJob wraps the user job so the entry's run counters and durations are
// recorded around every execution. It is installed as the innermost wrapper so
// that an outer wrapper which suppresses an execution (e.g.
// SkipIfStillRunning) keeps Runs from being incremented and the activation is
// reported as a skip instead.
type observedJob struct {
	job   Job
	stats *entryStats
}

func (j observedJob) Run() {
	start := time.Now()
	j.stats.recordStart(start)
	defer j.stats.recordFinish(start)
	j.job.Run()
}

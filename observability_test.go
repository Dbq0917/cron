package cron

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSchedule returns activation times from a preloaded slice so tests can
// drive the scheduler deterministically.
type fakeSchedule struct {
	mu    sync.Mutex
	times []time.Time
}

func (s *fakeSchedule) Next(after time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.times) > 0 && !s.times[0].After(after) {
		s.times = s.times[1:]
	}
	if len(s.times) == 0 {
		return time.Time{}
	}
	next := s.times[0]
	s.times = s.times[1:]
	return next
}

func TestEntryStatsActivationsRunsAndDuration(t *testing.T) {
	loc := time.UTC
	stats := newEntryStats(loc)

	if got := stats.snapshot(time.Time{}, time.Time{}); got.Activations != 0 || got.Runs != 0 {
		t.Fatalf("expected empty stats, got %+v", got)
	}

	base := time.Unix(1000, 0).In(loc)
	interval := time.Second

	for i := 0; i < 3; i++ {
		scheduled := base.Add(time.Duration(i) * interval)
		next := scheduled.Add(interval)
		stats.observe(scheduled, scheduled, next)

		start := scheduled
		stats.recordStart(start)
		if running := stats.snapshot(next, scheduled).Running; running != 1 {
			t.Fatalf("expected Running=1 during run, got %d", running)
		}
		stats.recordFinish(start)
	}

	snap := stats.snapshot(base.Add(3*interval), base.Add(2*interval))
	if snap.Activations != 3 || snap.Runs != 3 {
		t.Errorf("expected 3 activations and 3 runs, got activations=%d runs=%d",
			snap.Activations, snap.Runs)
	}
	if snap.Skips != 0 || snap.Misses != 0 {
		t.Errorf("expected no skips and no misses, got skips=%d misses=%d",
			snap.Skips, snap.Misses)
	}
	if snap.Running != 0 {
		t.Errorf("expected Running=0 after completion, got %d", snap.Running)
	}
	if snap.LastDuration <= 0 || snap.TotalDuration <= snap.LastDuration {
		t.Errorf("expected positive durations, got last=%v total=%v",
			snap.LastDuration, snap.TotalDuration)
	}
	if !snap.LastStart.Equal(base.Add(2 * interval)) {
		t.Errorf("unexpected LastStart: %v", snap.LastStart)
	}
	if snap.LastFinish.Before(snap.LastStart) {
		t.Errorf("LastFinish %v before LastStart %v", snap.LastFinish, snap.LastStart)
	}
	if snap.Next != base.Add(3*interval) || !snap.Prev.Equal(base.Add(2*interval)) {
		t.Errorf("unexpected Next/Prev: next=%v prev=%v", snap.Next, snap.Prev)
	}
}

func TestEntryStatsMissesLateActivation(t *testing.T) {
	loc := time.UTC
	stats := newEntryStats(loc)
	base := time.Unix(1000, 0).In(loc)
	interval := time.Second

	// First activation establishes the interval and must never count as a miss.
	stats.observe(base, base, base.Add(interval))
	stats.recordStart(base)
	stats.recordFinish(base)

	// Second activation fires three intervals late, so two runs were missed.
	late := base.Add(3 * interval)
	stats.observe(base.Add(interval), late, late.Add(interval))
	stats.recordStart(late)
	stats.recordFinish(late)

	snap := stats.snapshot(time.Time{}, time.Time{})
	if snap.Activations != 2 {
		t.Errorf("expected 2 activations, got %d", snap.Activations)
	}
	if snap.Misses != 2 {
		t.Errorf("expected 2 missed runs, got %d", snap.Misses)
	}
	if snap.Skips != 0 {
		t.Errorf("expected no skips, got %d", snap.Skips)
	}
}

func TestEntryStatsSkipsSuppressedExecution(t *testing.T) {
	stats := newEntryStats(time.UTC)
	base := time.Unix(1000, 0)

	// Three activations: the first and third execute, the second is suppressed
	// by an outer wrapper and therefore never reaches recordStart.
	stats.observe(base, base, base.Add(time.Second))
	stats.recordStart(base)
	stats.recordFinish(base)

	skipped := base.Add(time.Second)
	stats.observe(skipped, skipped, skipped.Add(time.Second))

	third := base.Add(2 * time.Second)
	stats.observe(third, third, third.Add(time.Second))
	stats.recordStart(third)
	stats.recordFinish(third)

	snap := stats.snapshot(time.Time{}, time.Time{})
	if snap.Activations != 3 || snap.Runs != 2 {
		t.Fatalf("expected 3 activations and 2 runs, got activations=%d runs=%d",
			snap.Activations, snap.Runs)
	}
	if snap.Skips != 1 {
		t.Errorf("expected 1 skip, got %d", snap.Skips)
	}
	if snap.Misses != 0 {
		t.Errorf("expected no misses, got %d", snap.Misses)
	}
}

// Drive a real Cron for several rounds and verify the exported stats line up
// with what the job actually did.
func TestCronStatsAcrossRuns(t *testing.T) {
	var runs int64
	cron := newWithSeconds()
	id, err := cron.AddFunc("* * * * * ?", func() {
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&runs, 1)
	})
	if err != nil {
		t.Fatal(err)
	}

	cron.Start()
	defer cron.Stop()

	deadline := time.After(3 * OneSecond)
	for atomic.LoadInt64(&runs) < 2 {
		select {
		case <-deadline:
			t.Fatalf("job only ran %d times", atomic.LoadInt64(&runs))
		case <-time.After(10 * time.Millisecond):
		}
	}

	entry := cron.Entry(id)
	snap := entry.Stats()

	if got := atomic.LoadInt64(&runs); int64(snap.Runs) != got {
		t.Errorf("Runs=%d does not match actual executions=%d", snap.Runs, got)
	}
	if snap.Activations != snap.Runs {
		t.Errorf("expected activations==runs, got %d vs %d",
			snap.Activations, snap.Runs)
	}
	if snap.Skips != 0 || snap.Misses != 0 {
		t.Errorf("expected no skips/misses, got skips=%d misses=%d",
			snap.Skips, snap.Misses)
	}
	if snap.LastDuration < 10*time.Millisecond {
		t.Errorf("expected LastDuration >= 10ms, got %v", snap.LastDuration)
	}
	if snap.TotalDuration < snap.LastDuration {
		t.Errorf("total %v shorter than last duration %v",
			snap.TotalDuration, snap.LastDuration)
	}
	if snap.LastStart.IsZero() || snap.LastFinish.Before(snap.LastStart) {
		t.Errorf("unexpected start/finish: %v -> %v", snap.LastStart, snap.LastFinish)
	}
	if snap.Next.IsZero() || !snap.Next.After(time.Now()) {
		t.Errorf("expected Next to be a readable future time, got %v", snap.Next)
	}
	if snap.Prev.IsZero() {
		t.Errorf("expected a readable Prev time")
	}

	// Cron-level helpers must agree with the per-entry snapshot.
	all := cron.Stats()
	if len(all) != 1 || all[0].Runs != snap.Runs {
		t.Errorf("Cron.Stats mismatch: %+v", all)
	}
	if got := cron.EntryStats(id); got.Runs != snap.Runs {
		t.Errorf("EntryStats mismatch: %+v", got)
	}
}

// A job wrapped with SkipIfStillRunning reports suppressed activations as
// skips, distinct from misses.
func TestCronStatsSkippedByWrapper(t *testing.T) {
	release := make(chan struct{})
	firstStarted := make(chan struct{})
	var first int32
	var completed int64

	cron := New(WithParser(secondParser),
		WithChain(SkipIfStillRunning(DiscardLogger)))
	id := cron.Schedule(Every(time.Second), FuncJob(func() {
		if atomic.CompareAndSwapInt32(&first, 0, 1) {
			close(firstStarted)
			<-release // Hold the first invocation so later ones are skipped.
		}
		atomic.AddInt64(&completed, 1)
	}))

	cron.Start()
	defer func() {
		close(release)
		cron.Stop()
	}()

	<-firstStarted

	// While the first invocation is blocked, subsequent activations must be
	// skipped rather than counted as runs.
	deadline := time.After(3 * OneSecond)
	for cron.EntryStats(id).Skips == 0 {
		select {
		case <-deadline:
			t.Fatalf("expected at least one skip, stats=%+v", cron.EntryStats(id))
		case <-time.After(10 * time.Millisecond):
		}
	}

	snap := cron.EntryStats(id)
	if snap.Runs != uint64(atomic.LoadInt64(&completed)) {
		t.Errorf("Runs=%d does not match completed invocations=%d",
			snap.Runs, atomic.LoadInt64(&completed))
	}
	if snap.Running != 1 {
		t.Errorf("expected 1 running invocation, got %d", snap.Running)
	}
	if snap.Activations != snap.Skips+snap.Runs+uint64(snap.Running) {
		t.Errorf("activations %d != skips %d + runs %d + running %d",
			snap.Activations, snap.Skips, snap.Runs, snap.Running)
	}
	if snap.Misses != 0 {
		t.Errorf("skips must not be reported as misses, got misses=%d", snap.Misses)
	}
}

// A deterministic schedule that activates late must be reported as a miss.
func TestCronStatsMissedActivation(t *testing.T) {
	loc := time.UTC
	now := time.Now().In(loc)

	// Two activations one second apart, but the second is delivered three
	// seconds after the first due time.
	schedule := &fakeSchedule{times: []time.Time{
		now,
		now.Add(time.Second),
	}}

	stats := newEntryStats(loc)
	entry := &Entry{
		ID:       1,
		Schedule: schedule,
		stats:    stats,
	}

	// First tick on time.
	firstDue := entry.Schedule.Next(now.Add(-time.Second))
	nextDue := entry.Schedule.Next(firstDue)
	stats.observe(firstDue, firstDue, nextDue)
	stats.recordStart(firstDue)
	stats.recordFinish(firstDue)

	// Scheduler wakes three intervals late for the second tick.
	lateWake := firstDue.Add(3 * time.Second)
	stats.observe(nextDue, lateWake, time.Time{})
	stats.recordStart(lateWake)
	stats.recordFinish(lateWake)

	snap := entry.Stats()
	if snap.Activations != 2 {
		t.Errorf("expected 2 activations, got %d", snap.Activations)
	}
	if snap.Misses != 2 {
		t.Errorf("expected 2 misses for a 3-interval-late wake, got %d", snap.Misses)
	}
	if snap.Skips != 0 {
		t.Errorf("expected no skips, got %d", snap.Skips)
	}
}

func TestEntryStatsZeroValueWhenNoStats(t *testing.T) {
	entry := Entry{ID: 1, Next: time.Now(), Prev: time.Now()}
	snap := entry.Stats()
	if snap.Activations != 0 || snap.Runs != 0 || snap.Running != 0 {
		t.Errorf("expected zero-value stats, got %+v", snap)
	}
	if snap.Next.IsZero() || snap.Prev.IsZero() {
		t.Errorf("zero-value stats should still carry Next/Prev")
	}
}

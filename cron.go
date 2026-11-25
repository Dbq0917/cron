package cron

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	entryByID map[EntryID]*Entry
	chain     Chain
	stop      chan struct{}
	add       chan *Entry
	remove    chan EntryID
	snapshot  chan chan []Entry
	running   bool
	logger    Logger
	runningMu sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job

	heapIndex int
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int { return len(s) }
func (s byTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
	s[i].heapIndex = i
	s[j].heapIndex = j
}
func (s byTime) Less(i, j int) bool {
	return entryLess(s[i], s[j])
}

func entryLess(left, right *Entry) bool {
	// A zero time is not schedulable and sorts after every concrete time.
	// Ties use ID so equal-time entries keep insertion order.
	leftZero := left.Next.IsZero()
	rightZero := right.Next.IsZero()
	if leftZero || rightZero {
		if leftZero && rightZero {
			return left.ID < right.ID
		}
		return !leftZero
	}
	if left.Next.Equal(right.Next) {
		return left.ID < right.ID
	}
	return left.Next.Before(right.Next)
}

func (s *byTime) Push(x interface{}) {
	entry := x.(*Entry)
	entry.heapIndex = len(*s)
	*s = append(*s, entry)
}

func (s *byTime) Pop() interface{} {
	old := *s
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	*s = old[:n-1]
	entry.heapIndex = -1
	return entry
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//   Time Zone
//     Description: The time zone in which schedules are interpreted
//     Default:     time.Local
//
//   Parser
//     Description: Parser converts cron spec strings into cron.Schedules.
//     Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//   Chain
//     Description: Wrap submitted jobs to customize behavior.
//     Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		entryByID: make(map[EntryID]*Entry),
		chain:     NewChain(),
		add:       make(chan *Entry),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
		heapIndex:  -1,
	}
	if !c.running {
		c.addEntry(entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}
	queue := (*byTime)(&c.entries)
	heap.Init(queue)

	var timer *time.Timer
	for {
		now = c.now()

		var delay time.Duration
		if queue.Len() == 0 || (*queue)[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			delay = 100000 * time.Hour
		} else {
			delay = (*queue)[0].Next.Sub(now)
		}
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}

		select {
		case wake := <-timer.C:
			now = wake.In(c.location)
			c.logger.Info("wake", "now", now)

			var due []*Entry
			// Run every entry whose next time was less than now.
			for queue.Len() > 0 {
				entry := (*queue)[0]
				if entry.Next.After(now) || entry.Next.IsZero() {
					break
				}
				entry = heap.Pop(queue).(*Entry)
				due = append(due, entry)

				c.startJob(entry.WrappedJob)
				entry.Prev = entry.Next
				entry.Next = entry.Schedule.Next(now)
				c.logger.Info("run", "now", now, "entry", entry.ID, "next", entry.Next)
			}
			for _, entry := range due {
				heap.Push(queue, entry)
			}

		case newEntry := <-c.add:
			stopTimer(timer)
			now = c.now()
			newEntry.Next = newEntry.Schedule.Next(now)
			c.addEntry(newEntry)
			c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

		case replyChan := <-c.snapshot:
			replyChan <- c.entrySnapshot()
			stopTimer(timer)

		case <-c.stop:
			timer.Stop()
			c.logger.Info("stop")
			return

		case id := <-c.remove:
			stopTimer(timer)
			c.removeEntry(id)
			c.logger.Info("removed", "entry", id)
		}
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		<-timer.C
	}
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		j.Run()
	}()
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c.jobWaiter.Wait()
		cancel()
	}()
	return ctx
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	sort.Slice(entries, func(i, j int) bool {
		return entryLess(&entries[i], &entries[j])
	})
	return entries
}

func (c *Cron) addEntry(entry *Entry) {
	if c.entryByID == nil {
		c.entryByID = make(map[EntryID]*Entry)
	}
	c.entryByID[entry.ID] = entry
	heap.Push((*byTime)(&c.entries), entry)
}

func (c *Cron) removeEntry(id EntryID) {
	if c.entryByID == nil {
		var entries []*Entry
		for _, candidate := range c.entries {
			if candidate.ID != id {
				entries = append(entries, candidate)
			}
		}
		c.entries = entries
		return
	}

	entry := c.entryByID[id]
	if entry == nil {
		return
	}
	delete(c.entryByID, id)
	if entry.heapIndex < 0 || entry.heapIndex >= len(c.entries) || c.entries[entry.heapIndex] != entry {
		entry.heapIndex = -1
		for i, candidate := range c.entries {
			if candidate == entry {
				entry.heapIndex = i
				break
			}
		}
	}
	if entry.heapIndex < 0 {
		return
	}
	heap.Remove((*byTime)(&c.entries), entry.heapIndex)
}

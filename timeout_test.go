package cron

import (
	"context"
	"sync"
	"testing"
	"time"
)

type blockingContextJob struct {
	mu       sync.Mutex
	starts   int
	started  chan int
	canceled chan error
}

func (j *blockingContextJob) Run(ctx context.Context) {
	j.mu.Lock()
	j.starts++
	run := j.starts
	j.mu.Unlock()

	j.started <- run
	<-ctx.Done()
	j.canceled <- ctx.Err()
}

func (j *blockingContextJob) Starts() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.starts
}

func TestJobTimeoutReleasesDelayIfStillRunning(t *testing.T) {
	job := &blockingContextJob{
		started:  make(chan int, 4),
		canceled: make(chan error, 4),
	}
	cron := New(
		WithJobTimeout(15*time.Millisecond),
		WithChain(DelayIfStillRunning(DiscardLogger)),
	)
	cron.Schedule(Every(20*time.Millisecond), job)
	cron.Start()

	for i := 0; i < 2; i++ {
		select {
		case <-job.started:
		case <-time.After(time.Second):
			t.Fatalf("job did not start run %d", i+1)
		}
		select {
		case err := <-job.canceled:
			if err != context.DeadlineExceeded {
				t.Fatalf("run %d context error = %v, want deadline exceeded", i+1, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("run %d did not observe its timeout", i+1)
		}
	}

	stopCtx := cron.Stop()
	select {
	case <-stopCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after timed out jobs exited")
	}
}

func TestJobTimeoutReleasesSkipIfStillRunning(t *testing.T) {
	job := &blockingContextJob{
		started:  make(chan int, 4),
		canceled: make(chan error, 4),
	}
	cron := New(
		WithJobTimeout(15*time.Millisecond),
		WithChain(SkipIfStillRunning(DiscardLogger)),
	)
	cron.Schedule(Every(20*time.Millisecond), job)
	cron.Start()

	for i := 0; i < 2; i++ {
		select {
		case <-job.started:
		case <-time.After(time.Second):
			t.Fatalf("job did not start run %d after timeout", i+1)
		}
		select {
		case <-job.canceled:
		case <-time.After(time.Second):
			t.Fatalf("run %d did not observe its timeout", i+1)
		}
	}

	stopCtx := cron.Stop()
	select {
	case <-stopCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after timed out jobs exited")
	}
}

func TestJobTimeoutStopWaitsForActualExit(t *testing.T) {
	started := make(chan struct{}, 1)
	exited := make(chan struct{})
	job := FuncJobContext(func(ctx context.Context) {
		started <- struct{}{}
		<-ctx.Done()
		time.Sleep(25 * time.Millisecond)
		close(exited)
	})

	cron := New(WithJobTimeout(10 * time.Millisecond))
	cron.Schedule(Every(time.Millisecond), job)
	cron.Start()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}

	stopCtx := cron.Stop()
	select {
	case <-stopCtx.Done():
		select {
		case <-exited:
		default:
			t.Fatal("Stop returned before the job actually exited")
		}
	case <-exited:
	}

	select {
	case <-stopCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after the job exited")
	}
}

func TestTimeoutWrapperCancelsContext(t *testing.T) {
	canceled := make(chan error, 1)
	job := NewChain(Timeout(10 * time.Millisecond)).Then(
		FuncJobContext(func(ctx context.Context) {
			<-ctx.Done()
			canceled <- ctx.Err()
		}),
	)

	job.Run()
	select {
	case err := <-canceled:
		if err != context.DeadlineExceeded {
			t.Fatalf("context error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wrapped job did not observe its timeout")
	}
}

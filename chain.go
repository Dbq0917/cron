package cron

import (
	"context"
	"fmt"
	"runtime"
	"time"
)

// JobWrapper decorates the given Job with some behavior.
type JobWrapper func(Job) Job

// Chain is a sequence of JobWrappers that decorates submitted jobs with
// cross-cutting behaviors like logging or synchronization.
type Chain struct {
	wrappers []JobWrapper
}

// NewChain returns a Chain consisting of the given JobWrappers.
func NewChain(c ...JobWrapper) Chain {
	return Chain{c}
}

// Then decorates the given job with all JobWrappers in the chain.
//
// This:
//     NewChain(m1, m2, m3).Then(job)
// is equivalent to:
//     m1(m2(m3(job)))
func (c Chain) Then(job interface{}) Job {
	j, err := asJob(job)
	if err != nil {
		panic(err)
	}
	for i := range c.wrappers {
		j = c.wrappers[len(c.wrappers)-i-1](j)
	}
	return j
}

// Recover panics in wrapped jobs and log them with the provided logger.
func Recover(logger Logger) JobWrapper {
	return func(j Job) Job {
		return contextFuncJob(func(ctx context.Context) error {
			defer func() {
				if r := recover(); r != nil {
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					err, ok := r.(error)
					if !ok {
						err = fmt.Errorf("%v", r)
					}
					logger.Error(err, "panic", "stack", "...\n"+string(buf))
				}
			}()
			return runJob(ctx, j)
		})
	}
}

// DelayIfStillRunning serializes jobs, delaying subsequent runs until the
// previous one is complete. Jobs running after a delay of more than a minute
// have the delay logged at Info.
func DelayIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		ch := make(chan struct{}, 1)
		return contextFuncJob(func(ctx context.Context) error {
			start := time.Now()
			select {
			case ch <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			defer func() { <-ch }()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if dur := time.Since(start); dur > time.Minute {
				logger.Info("delay", "duration", dur)
			}
			return runJob(ctx, j)
		})
	}
}

// SkipIfStillRunning skips an invocation of the Job if a previous invocation is
// still running. It logs skips to the given logger at Info level.
func SkipIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		var ch = make(chan struct{}, 1)
		ch <- struct{}{}
		return contextFuncJob(func(ctx context.Context) error {
			select {
			case v := <-ch:
				defer func() { ch <- v }()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return runJob(ctx, j)
			default:
				logger.Info("skip")
			}
			return nil
		})
	}
}

// Timeout limits each invocation of the wrapped job to the given duration.
// When the timeout expires, the job's context is canceled. The wrapper still
// waits for the job to return, so serialization wrappers and Stop observe a
// real completion rather than an abandoned goroutine.
func Timeout(timeout time.Duration, loggers ...Logger) JobWrapper {
	var logger Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	return func(j Job) Job {
		return contextFuncJob(func(ctx context.Context) error {
			if timeout <= 0 {
				return runJob(ctx, j)
			}
			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			err := runJob(timeoutCtx, j)
			if logger != nil && timeoutCtx.Err() == context.DeadlineExceeded {
				logger.Error(timeoutCtx.Err(), "timeout")
			}
			return err
		})
	}
}

// JobTimeout is an alias for Timeout.
func JobTimeout(timeout time.Duration, loggers ...Logger) JobWrapper {
	return Timeout(timeout, loggers...)
}

// TimeoutJob is an alias for Timeout.
func TimeoutJob(timeout time.Duration, loggers ...Logger) JobWrapper {
	return Timeout(timeout, loggers...)
}

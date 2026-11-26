package cron

import "time"

// ConstantDelaySchedule represents a simple recurring duty cycle, e.g. "Every 5 minutes".
// It supports sub-second delays at their original precision.
type ConstantDelaySchedule struct {
	Delay time.Duration
}

// Every returns a crontab Schedule that activates once every duration.
// The delay is kept at its original precision, including sub-second delays.
func Every(duration time.Duration) ConstantDelaySchedule {
	if duration < time.Nanosecond {
		duration = time.Nanosecond
	}
	return ConstantDelaySchedule{
		Delay: duration,
	}
}

// Next returns the next time this should be run.
// This rounds up so that the next activation time falls on a multiple of the
// delay, keeping activations aligned to the schedule's own precision.
func (schedule ConstantDelaySchedule) Next(t time.Time) time.Time {
	return t.Add(schedule.Delay - time.Duration(t.Nanosecond())%schedule.Delay)
}

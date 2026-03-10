package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/job"
)

// ErrSimulatedQueueOutage is returned by OutageQueue when simulating an outage.
var ErrSimulatedQueueOutage = errors.New("simulated queue outage")

var _ Queue = (*OutageQueue)(nil)

// OutageQueue decorates a Queue and can simulate temporary queue/network outages.
// During an outage, all Queue operations return ErrSimulatedQueueOutage.
type OutageQueue struct {
	inner     Queue
	untilNano atomic.Int64 // Unix nanoseconds
}

func NewOutageQueue(inner Queue) *OutageQueue {
	return &OutageQueue{inner: inner}
}

// StartOutage simulates an outage for the specified duration.
func (q *OutageQueue) StartOutage(d time.Duration) {
	until := time.Now().Add(d)

	for {
		current := q.untilNano.Load()
		if until.UnixNano() <= current {
			return
		}

		if q.untilNano.CompareAndSwap(current, until.UnixNano()) {
			return
		}
	}
}

// Err returns ErrSimulatedQueueOutage if the queue
// is currently simulating an outage.
func (q *OutageQueue) Err() error {
	if q.isOutage() {
		return ErrSimulatedQueueOutage
	}

	return nil
}

func (q *OutageQueue) Enqueue(ctx context.Context, j job.Job) error {
	if err := q.Err(); err != nil {
		return err
	}

	return q.inner.Enqueue(ctx, j)
}

func (q *OutageQueue) Dequeue(ctx context.Context, workerID string) (*job.Job, error) {
	if err := q.Err(); err != nil {
		return nil, err
	}

	return q.inner.Dequeue(ctx, workerID)
}

func (q *OutageQueue) ExtendLease(
	ctx context.Context,
	jobId int64,
	lockToken string,
) (time.Time, error) {
	if err := q.Err(); err != nil {
		return time.Time{}, err
	}

	return q.inner.ExtendLease(ctx, jobId, lockToken)
}

func (q *OutageQueue) LeaseTime() time.Duration {
	return q.inner.LeaseTime()
}

func (q *OutageQueue) ScheduleRetry(
	ctx context.Context,
	jobId int64,
	lockToken string,
	delay time.Duration,
	lastError string,
) error {
	if err := q.Err(); err != nil {
		return err
	}

	return q.inner.ScheduleRetry(ctx, jobId, lockToken, delay, lastError)
}

func (q *OutageQueue) Complete(ctx context.Context, jobId int64, lockToken string) error {
	if err := q.Err(); err != nil {
		return err
	}

	return q.inner.Complete(ctx, jobId, lockToken)
}

func (q *OutageQueue) Fail(
	ctx context.Context,
	jobId int64,
	lockToken string,
	lastError string,
) error {
	if err := q.Err(); err != nil {
		return err
	}

	return q.inner.Fail(ctx, jobId, lockToken, lastError)
}

func (q *OutageQueue) NextDue(ctx context.Context) (time.Time, error) {
	if err := q.Err(); err != nil {
		return time.Time{}, err
	}

	return q.inner.NextDue(ctx)
}

// isOutage checks if an outage is currently active.
func (q *OutageQueue) isOutage() bool {
	return q.untilNano.Load() > time.Now().UnixNano()
}

package queue

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/job"
)

// CounterQueue decorates a Queue and counts dequeue attempts, including empty results.
type CounterQueue struct {
	inner Queue

	dequeueCalls  atomic.Uint32
	dequeueEmpty  atomic.Uint32
	dequeueErrors atomic.Uint32
}

func NewCounterQueue(inner Queue) *CounterQueue {
	return &CounterQueue{inner: inner}
}

// DequeueCounts returns the total number of Dequeue calls,
// how many returned no job, and how many resulted in an error.
func (q *CounterQueue) DequeueCounts() (calls, empty, errors uint) {
	return uint(q.dequeueCalls.Load()), uint(q.dequeueEmpty.Load()), uint(q.dequeueErrors.Load())
}

func (q *CounterQueue) Enqueue(ctx context.Context, job job.Job) error {
	return q.inner.Enqueue(ctx, job)
}

// Dequeue increments the dequeue call counters and delegates to the inner queue.
func (q *CounterQueue) Dequeue(ctx context.Context, workerID string) (*job.Job, error) {
	q.dequeueCalls.Add(1)

	j, err := q.inner.Dequeue(ctx, workerID)
	if err != nil {
		q.dequeueErrors.Add(1)

		return nil, err
	}

	if j == nil {
		q.dequeueEmpty.Add(1)
	}

	return j, nil
}

func (q *CounterQueue) ExtendLease(
	ctx context.Context,
	jobId int64,
	lockToken string,
) (time.Time, error) {
	return q.inner.ExtendLease(ctx, jobId, lockToken)
}

func (q *CounterQueue) LeaseTime() time.Duration {
	return q.inner.LeaseTime()
}

func (q *CounterQueue) ScheduleRetry(
	ctx context.Context,
	jobId int64,
	lockToken string,
	delay time.Duration,
	lastError string,
) error {
	return q.inner.ScheduleRetry(ctx, jobId, lockToken, delay, lastError)
}

func (q *CounterQueue) Complete(ctx context.Context, jobId int64, lockToken string) error {
	return q.inner.Complete(ctx, jobId, lockToken)
}

func (q *CounterQueue) Fail(
	ctx context.Context,
	jobId int64,
	lockToken string,
	lastError string,
) error {
	return q.inner.Fail(ctx, jobId, lockToken, lastError)
}

func (q *CounterQueue) NextDue(ctx context.Context) (time.Time, error) {
	return q.inner.NextDue(ctx)
}

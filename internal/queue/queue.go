package queue

import (
	"context"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/job"
)

// Queue defines the interface for a job queue.
type Queue interface {
	// Enqueue adds a new job to the queue.
	Enqueue(ctx context.Context, job job.Job) error
	// Dequeue retrieves the next available job from the queue.
	Dequeue(ctx context.Context, workerID string) (*job.Job, error)
	// ScheduleRetry schedules a job for retry after a delay.
	ScheduleRetry(
		ctx context.Context,
		jobID int64,
		lockToken string,
		delay time.Duration,
		lastError string,
	) error
	// Complete marks a job as completed.
	Complete(ctx context.Context, jobID int64, lockToken string) error
	// Fail marks a job as failed with the provided error message.
	Fail(ctx context.Context, jobID int64, lockToken string, lastError string) error
	// ExtendLease extends the processing lease for a job.
	ExtendLease(ctx context.Context, jobID int64, lockToken string) (time.Time, error)
	// LeaseTime returns the default processing lease duration for this queue.
	LeaseTime() time.Duration
	// NextDue returns the next time a job is due to be processed.
	NextDue(ctx context.Context) (time.Time, error)
}

// DeadLetterQueue is an optional interface for queues that support
// a DLQ for permantently failed jobs.
type DeadLetterQueue interface {
	// DLQCount returns the number of jobs currently in the dead-letter queue.
	DLQCount(ctx context.Context) (int64, error)
	// DLQList returns a list of entries in the dead-letter queue for monitoring purposes.
	DLQList(ctx context.Context, limit, offset int) ([]DeadLetterQueueEntry, error)
}

// DeadLetterQueueEntry describes a job that was moved to the dead-letter queue.
type DeadLetterQueueEntry struct {
	JobID    int64     // ID of the original job.
	FailedAt time.Time // When the job was moved to the DLQ.
	Reason   string    // Human-readable reason for dead-lettering.
}

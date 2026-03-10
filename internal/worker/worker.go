package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/backoff"
	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
)

// WorkerState represents the current state of a worker for metrics tracking.
type WorkerState string

const (
	StateIdle       WorkerState = "idle"       // Worker is idle, waiting for next poll
	StateFetching   WorkerState = "fetching"   // Worker is attempting to dequeue a job
	StateProcessing WorkerState = "processing" // Worker is actively processing a job
)

const (
	defaultBackoffBaseDelay     = 2 * time.Second
	defaultMaxAttempts          = 5
	defaultInterval             = 5 * time.Second
	defaultTimeout              = 30 * time.Second
	defaultFinalizeTimeout      = 10 * time.Second
	defaultHeartbeatMinInterval = 250 * time.Millisecond
	defaultLeaseDivisor         = 2
)

// WorkerEvent represents an event related to worker activity.
type WorkerEvent struct {
	At         time.Time `json:"time"`
	WorkerName string    `json:"worker"`
	JobID      *int64    `json:"job,omitempty"`
	Event      string    `json:"event"`
	Error      string    `json:"error,omitempty"`
}

type OnWorkerEventFunc func(WorkerEvent)

// Worker defines the interface for a job processing worker.
type Worker interface {
	GetID() uint
	GetName() string
	GetQueue() queue.Queue
	GetState() WorkerState
	SetState(state WorkerState)
	OnEvent(event string, job *job.Job, err error)
	Start()
	Stop()
	Process(j *job.Job) error
}

// AttemptRecord captures data about a single job processing attempt.
type AttemptRecord struct {
	JobID       int64
	WorkerName  string
	Attempt     uint
	AttemptedAt time.Time
	Success     bool
	RetryDelay  time.Duration // delay scheduled for next retry (0 if success or final failure)
}

// OnAttemptFunc is called after each job processing attempt.
type OnAttemptFunc func(AttemptRecord)

// WorkerOptions holds configuration options for a Worker.
type WorkerOptions struct {
	// Registry holds the job handlers that can be executed by this worker.
	Registry job.HandlerRegistry
	// RetryStrategy determines how retry delays are calculated.
	Backoff backoff.Backoff
	// MaxAttempts is the maximum number of attempts before a job is considered failed.
	MaxAttempts uint
	// Interval is the time to wait between polling attempts when idle.
	Interval time.Duration
	// Timeout is the maximum allowed processing time for a job before it's considered failed.
	Timeout time.Duration
	// FinalizeTimeout is the maximum time allowed for finalizing a job before giving up.
	FinalizeTimeout time.Duration
	// OnAttempt is called after each job processing attempt with details about the attempt.
	OnAttempt OnAttemptFunc
	// OnWorkerEvent is called for various worker events such as state changes, errors, etc.
	OnWorkerEvent OnWorkerEventFunc
}

// NewDefaultWorkerOptions creates WorkerOptions with defaults and applies any modifiers.
func NewDefaultWorkerOptions(opts ...func(*WorkerOptions)) *WorkerOptions {
	reg := job.NewDefaultHandlerRegistry()
	reg.Register(job.TypeNoop, job.NoopHandler)

	o := &WorkerOptions{
		Backoff:         backoff.NewConstant(defaultBackoffBaseDelay),
		MaxAttempts:     defaultMaxAttempts,
		Interval:        defaultInterval,
		Registry:        reg,
		Timeout:         defaultTimeout,
		FinalizeTimeout: defaultFinalizeTimeout,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

func WithBackoff(b backoff.Backoff) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.Backoff = b
	}
}

func WithMaxAttempts(a uint) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.MaxAttempts = a
	}
}

func WithInterval(d time.Duration) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.Interval = d
	}
}

func WithRegistry(hr job.HandlerRegistry) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.Registry = hr
	}
}

func WithTimeout(d time.Duration) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.Timeout = d
	}
}

func WithFinalizeTimeout(d time.Duration) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.FinalizeTimeout = d
	}
}

func WithOnAttempt(fn OnAttemptFunc) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.OnAttempt = fn
	}
}

func WithOnWorkerEvent(fn OnWorkerEventFunc) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.OnWorkerEvent = fn
	}
}

// WorkerEventLogger writes worker events to a log file.
type WorkerEventLogger struct {
	mu       sync.Mutex
	file     *os.File
	encoder  *json.Encoder
	filepath string
}

// NewWorkerEventLogger creates a new logger that writes to the specified file.
func NewWorkerEventLogger(filepath string) (*WorkerEventLogger, error) {
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open worker log file: %w", err)
	}

	return &WorkerEventLogger{
		file:     file,
		encoder:  json.NewEncoder(file),
		filepath: filepath,
	}, nil
}

// Log writes a worker event to the log file.
func (l *WorkerEventLogger) Log(event WorkerEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.encoder.Encode(event); err != nil {
		fmt.Fprintf(os.Stderr, "failed to log worker event: %v\n", err)
	}
}

// Close closes the log file.
func (l *WorkerEventLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		return l.file.Close()
	}

	return nil
}

// processJobs continuously fetches and processes jobs from the queue until the queue is empty.
func processJobs(ctx context.Context, w Worker) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Worker attempts to fetch a job.
		// Can block based on locking behavior (FOR UPDATE vs SKIP LOCKED).
		w.SetState(StateFetching)

		job, err := w.GetQueue().Dequeue(ctx, w.GetName())
		if err != nil || job == nil {
			if err != nil {
				if errors.Is(err, queue.ErrSimulatedQueueOutage) {
					w.OnEvent("outage_error", job, err)
				} else {
					w.OnEvent("dequeue_error", job, err)
				}
			}

			w.SetState(StateIdle)

			return
		}

		// Worker successfully got a job and is now processing it
		w.SetState(StateProcessing)

		stopHeartbeat := make(chan struct{})
		startLeaseHeartbeat(ctx, w.GetQueue(), job, stopHeartbeat)

		err = w.Process(job)

		close(stopHeartbeat)

		// If processing failed due to a queue error (e.g. outage), stop for now.
		// Job retry/failure decisions are handled inside Worker.Process.
		if err != nil {
			w.SetState(StateIdle)

			return
		}
	}
}

// recordAttempt emits an AttemptRecord to the OnAttempt callback if one is set.
func (o *WorkerOptions) recordAttempt(
	workerName string,
	j *job.Job,
	attemptedAt time.Time,
	success bool,
	retryDelay time.Duration,
) {
	if o.OnAttempt == nil {
		return
	}

	o.OnAttempt(AttemptRecord{
		JobID:       j.ID,
		WorkerName:  workerName,
		Attempt:     j.Attempts,
		AttemptedAt: attemptedAt,
		Success:     success,
		RetryDelay:  retryDelay,
	})
}

// handleJobError handles a failed job execution: it either stops the worker,
// schedules a retry, or marks the job as permanently failed.
// It is shared between PollingWorker and ListeningWorker.
func handleJobError(
	workerCtx context.Context,
	w Worker,
	q queue.Queue,
	opts *WorkerOptions,
	finalizeCtx context.Context,
	j *job.Job,
	jobErr error,
	attemptedAt time.Time,
) error {
	// If the worker itself is shutting down, stop processing.
	if (errors.Is(jobErr, context.Canceled) || errors.Is(jobErr, context.DeadlineExceeded)) &&
		workerCtx.Err() != nil {
		return jobErr
	}

	if j.Attempts >= opts.MaxAttempts {
		opts.recordAttempt(w.GetName(), j, attemptedAt, false, 0)
		w.OnEvent("job_failed_final", j, jobErr)

		return q.Fail(finalizeCtx, j.ID, j.LockToken, jobErr.Error())
	}

	delay := opts.Backoff.CalculateDelay(j.Attempts)

	opts.recordAttempt(w.GetName(), j, attemptedAt, false, delay)

	w.OnEvent("job_retry_scheduled", j, jobErr)

	if err := q.ScheduleRetry(finalizeCtx, j.ID, j.LockToken, delay, jobErr.Error()); err != nil {
		w.OnEvent("retry_schedule_error", j, err)

		return err
	}

	return nil
}

// startLeaseHeartbeat starts a goroutine that periodically extends the lease on the job being processed.
func startLeaseHeartbeat(ctx context.Context, q queue.Queue, job *job.Job, stop <-chan struct{}) {
	if q == nil || job == nil {
		return
	}

	lease := q.LeaseTime()
	if lease <= 0 {
		return
	}

	if job.LockToken == "" {
		return
	}

	interval := max(lease/defaultLeaseDivisor, defaultHeartbeatMinInterval)

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				_, err := q.ExtendLease(ctx, job.ID, job.LockToken)
				// If the queue is temporarily unavailable, keep trying.
				if err != nil {
					if errors.Is(err, queue.ErrSimulatedQueueOutage) {
						continue
					}
					// If the lock was lost (e.g. reclaimed by another worker), stop heartbeating.
					return
				}
			}
		}
	}()
}

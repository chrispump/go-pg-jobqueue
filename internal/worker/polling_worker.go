package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
)

// Polling is a Worker implementation that periodically polls the queue
// for new jobs at a specified interval.
type Polling struct {
	id    uint
	label string

	ctx    context.Context
	cancel context.CancelFunc

	queue queue.Queue
	opts  *WorkerOptions
	state atomic.Value // stores WorkerState
}

// NewPollingWorker creates a new instance of Polling worker.
func NewPollingWorker(
	ctx context.Context,
	label string,
	id uint,
	queue queue.Queue,
	opts ...func(*WorkerOptions),
) *Polling {
	ctx, cancel := context.WithCancel(ctx)
	w := &Polling{
		ctx:    ctx,
		cancel: cancel,
		label:  label,
		id:     id,
		queue:  queue,
		opts:   NewDefaultWorkerOptions(opts...),
	}

	// Initialize state as idle
	w.SetState(StateIdle)

	return w
}

func (w *Polling) GetID() uint {
	return w.id
}

func (w *Polling) GetName() string {
	return fmt.Sprintf("%s-%d", w.label, w.id)
}

func (w *Polling) Start() {
	// Set worker to idle on start
	w.SetState(StateIdle)

	// Process immediately on start, no initial wait
	processJobs(w.ctx, w)

	for {
		select {
		// On each tick, attempt to dequeue and process a job
		case <-time.After(w.opts.Interval):
			w.SetState(StateIdle) // Set to idle before attempting to fetch
			processJobs(w.ctx, w)
		// On context cancellation, stop the worker
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Polling) Process(job *job.Job) error {
	typ := job.Payload.Opts.Type
	attemptedAt := time.Now()

	handler, ok := w.opts.Registry.GetHandler(typ)
	if !ok {
		errMsg := fmt.Sprintf("no handler registered for job type: %s", typ)
		w.OnEvent("handler_not_found", job, errors.New(errMsg))

		finalizeCtx, cancelFinalize := w.finalizeContext()
		defer cancelFinalize()

		return w.queue.Fail(finalizeCtx, job.ID, job.LockToken, errMsg)
	}

	// Limit job processing time if timeout is set
	workCtx, cancelWork := w.workContext()
	defer cancelWork()

	err := handler(workCtx, job)

	finalizeCtx, cancelFinalize := w.finalizeContext()
	defer cancelFinalize()

	if err != nil {
		return handleJobError(w.ctx, w, w.queue, w.opts, finalizeCtx, job, err, attemptedAt)
	}

	w.opts.recordAttempt(w.GetName(), job, attemptedAt, true, 0)

	return w.queue.Complete(finalizeCtx, job.ID, job.LockToken)
}

func (w *Polling) Stop() { w.cancel() }

// GetQueue implements Worker.
func (w *Polling) GetQueue() queue.Queue {
	return w.queue
}

// GetState implements Worker.
func (w *Polling) GetState() WorkerState {
	val := w.state.Load()
	if val == nil {
		return StateIdle
	}

	return val.(WorkerState)
}

// SetState implements Worker.
func (w *Polling) SetState(state WorkerState) {
	w.state.Store(state)
}

// OnEvent implements Worker.
func (w *Polling) OnEvent(event string, job *job.Job, err error) {
	jobId := (*int64)(nil)
	if job != nil {
		jobId = &job.ID
	}

	if w.opts.OnWorkerEvent != nil {
		w.opts.OnWorkerEvent(WorkerEvent{
			At:         time.Now(),
			WorkerName: w.GetName(),
			JobID:      jobId,
			Event:      event,
			Error:      err.Error(),
		})
	}
}

func (w *Polling) workContext() (context.Context, context.CancelFunc) {
	if w.opts.Timeout <= 0 {
		return w.ctx, func() {}
	}

	return context.WithTimeout(w.ctx, w.opts.Timeout)
}

func (w *Polling) finalizeContext() (context.Context, context.CancelFunc) {
	if w.opts.FinalizeTimeout <= 0 {
		return context.Background(), func() {}
	}

	return context.WithTimeout(context.Background(), w.opts.FinalizeTimeout)
}

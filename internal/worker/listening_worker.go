package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// idleCount tracks the number of idle lottery-enabled workers.
var idleCount atomic.Int32

const (
	defaultReconnectDelay = 500 * time.Millisecond
	defaultListenTimeout  = 250 * time.Millisecond
)

// Listening is a worker that uses PostgreSQL LISTEN/NOTIFY to receive
// notifications when new jobs are enqueued.
type Listening struct {
	id    uint
	label string

	ctx    context.Context
	cancel context.CancelFunc
	state  atomic.Value

	queue queue.Queue
	pool  *pgxpool.Pool
	opts  *WorkerOptions

	notifyChannel    string
	notifyMaxWinners uint

	reconnectDelay time.Duration
	timeout        time.Duration

	lotteryScheduling bool
	isCountedAsIdle   atomic.Bool
}

// NewListeningWorker creates a new Listening worker.
func NewListeningWorker(
	ctx context.Context,
	label string,
	id uint,
	queue queue.Queue,
	pool *pgxpool.Pool,
	opts ...func(*WorkerOptions),
) *Listening {
	w := &Listening{
		label:            label,
		id:               id,
		notifyMaxWinners: 1,
		notifyChannel:    "job_enqueued",
		queue:            queue,
		pool:             pool,
		reconnectDelay:   defaultReconnectDelay,
		timeout:          defaultListenTimeout,
		opts:             NewDefaultWorkerOptions(opts...),
	}

	w.ctx, w.cancel = context.WithCancel(ctx)
	w.SetState(StateIdle)

	return w
}

// UseLotteryScheduling enables lottery-based notification handling
// to reduce thundering herd effects.
func (w *Listening) UseLotteryScheduling() {
	w.lotteryScheduling = true
}

// SetMaxNotifyWinners sets the number of workers that should respond
// to each notification in lottery mode.
func (w *Listening) SetMaxNotifyWinners(count uint) {
	w.notifyMaxWinners = count
}

// GetID returns the worker's numeric identifier.
func (w *Listening) GetID() uint {
	return w.id
}

// GetName returns the worker's display name.
func (w *Listening) GetName() string {
	return fmt.Sprintf("%s-%d", w.label, w.id)
}

// Start begins the worker's listen loop. It drains any backlog,
// then listens for NOTIFY events and reconnects on failure.
func (w *Listening) Start() {
	w.SetState(StateIdle)

	if w.lotteryScheduling {
		defer func() {
			if w.isCountedAsIdle.CompareAndSwap(true, false) {
				idleCount.Add(-1)
			}
		}()
	}

	processJobs(w.ctx, w)

	for {
		conn, err := w.pool.Acquire(w.ctx)
		if err != nil {
			w.OnEvent("db_connection_error", nil, err)

			if !w.sleepOrStop() {
				return
			}

			continue
		}

		err = w.listen(conn)
		conn.Release()

		if err == nil {
			return
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			w.ctx.Err() != nil {
			w.OnEvent("worker_stopped", nil, err)

			return
		}

		if !w.sleepOrStop() {
			w.OnEvent("reconnect_aborted", nil, w.ctx.Err())

			return
		}
	}
}

// Process executes the handler for a job and finalizes it as completed,
// retried, or failed.
func (w *Listening) Process(job *job.Job) error {
	typ := job.Payload.Opts.Type
	attemptedAt := time.Now()

	handler, ok := w.opts.Registry.GetHandler(typ)
	if !ok {
		errMsg := fmt.Sprintf("no handler registered for job type: %s", typ)
		w.OnEvent("handler_not_found", job, errors.New(errMsg))

		finalizeCtx, cancel := w.finalizeContext()
		defer cancel()

		return w.queue.Fail(finalizeCtx, job.ID, job.LockToken, errMsg)
	}

	finalizeCtx, cancelFinalize := w.finalizeContext()
	defer cancelFinalize()

	workCtx, cancelWork := w.workContext()
	defer cancelWork()

	err := handler(workCtx, job)
	if err != nil {
		return handleJobError(w.ctx, w, w.queue, w.opts, finalizeCtx, job, err, attemptedAt)
	}

	return w.queue.Complete(finalizeCtx, job.ID, job.LockToken)
}

// Stop cancels the worker's context, triggering shutdown.
func (w *Listening) Stop() { w.cancel() }

// GetQueue returns the queue this worker pulls jobs from.
func (w *Listening) GetQueue() queue.Queue {
	return w.queue
}

// GetState returns the worker's current state.
func (w *Listening) GetState() WorkerState {
	val := w.state.Load()
	if val == nil {
		return StateIdle
	}

	return val.(WorkerState)
}

// SetState updates the worker's state and adjusts the idle counter.
func (w *Listening) SetState(state WorkerState) {
	w.state.Store(state)
	w.updateIdleCount(state)
}

// OnEvent emits a worker event to the configured event handler.
func (w *Listening) OnEvent(event string, job *job.Job, err error) {
	jobId := (*int64)(nil)
	if job != nil {
		jobId = &job.ID
	}

	if w.opts.OnWorkerEvent != nil {
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}

		w.opts.OnWorkerEvent(WorkerEvent{
			At:         time.Now(),
			WorkerName: w.GetName(),
			JobID:      jobId,
			Event:      event,
			Error:      errMsg,
		})
	}
}

// isWinner returns true with probability maxWinners/idles.
func (w *Listening) isWinner(idles int32) bool {
	if idles <= 0 {
		return false
	}

	return rand.Float64() < min(1.0, float64(w.notifyMaxWinners)/float64(idles))
}

// updateIdleCount adjusts the global idle counter based on state transitions.
// Only applies to workers with lottery scheduling enabled.
func (w *Listening) updateIdleCount(state WorkerState) {
	if !w.lotteryScheduling {
		return
	}

	if state == StateIdle {
		if w.isCountedAsIdle.CompareAndSwap(false, true) {
			idleCount.Add(1)
		}

		return
	}

	if w.isCountedAsIdle.CompareAndSwap(true, false) {
		idleCount.Add(-1)
	}
}

// listen subscribes to the notification channel and runs the main event loop.
func (w *Listening) listen(conn *pgxpool.Conn) error {
	if _, err := conn.Exec(w.ctx, "LISTEN "+w.notifyChannel); err != nil {
		return err
	}

	notifyCh, errCh := startNotificationListener(w.ctx, conn)

	var (
		timer     *time.Timer
		nextDue   time.Time
		nextDueCh <-chan time.Time
	)

	cancelWakeup := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			timer, nextDueCh, nextDue = nil, nil, time.Time{}
		}
	}

	scheduleWakeup := func(due time.Time) {
		if due.IsZero() || (!nextDue.IsZero() && !due.Before(nextDue)) {
			return
		}

		cancelWakeup()

		delay := max(time.Until(due), 0)
		timer = time.NewTimer(delay)
		nextDueCh, nextDue = timer.C, due
	}

	retrySchedule := func() {
		cancelWakeup()
		scheduleWakeup(time.Now().Add(w.reconnectDelay))
	}

	// Schedule initial wakeup for pending future jobs (non-lottery only).
	if !w.lotteryScheduling {
		if due, err := w.queue.NextDue(w.ctx); err == nil {
			scheduleWakeup(due)
		} else {
			scheduleWakeup(time.Now().Add(w.reconnectDelay))
		}
	}

	for {
		select {
		case <-w.ctx.Done():
			cancelWakeup()

			return w.ctx.Err()

		case err, ok := <-errCh:
			cancelWakeup()

			if !ok {
				return nil
			}

			return err

		case <-nextDueCh:
			cancelWakeup()
			w.processAndReschedule(scheduleWakeup, retrySchedule)

		case n, ok := <-notifyCh:
			if !ok {
				cancelWakeup()

				return nil
			}

			due, ok := parseNotification(n.Payload)
			if !ok || !due.After(time.Now()) {
				// Job is due now or payload unparseable: process immediately.
				w.processAndReschedule(scheduleWakeup, retrySchedule)
			} else {
				// Job is due in the future: schedule a wakeup timer.
				w.handleFutureNotification(due, scheduleWakeup, retrySchedule)
			}
		}
	}
}

// processAndReschedule drains the queue and schedules a timer for the next
// future job. In lottery mode, only winners attempt processing.
func (w *Listening) processAndReschedule(scheduleWakeup func(time.Time), retrySchedule func()) {
	var wonLottery bool

	if w.lotteryScheduling {
		if w.GetState() == StateIdle {
			idles := idleCount.Load()
			if idles > 0 && w.isWinner(idles) {
				processJobs(w.ctx, w)

				wonLottery = true
			}
		}
	} else {
		processJobs(w.ctx, w)

		wonLottery = true
	}

	if !wonLottery {
		return
	}

	due, err := w.queue.NextDue(w.ctx)
	if err != nil {
		retrySchedule()

		return
	}

	if !due.IsZero() && due.After(time.Now()) {
		scheduleWakeup(due)
	}
}

// handleFutureNotification schedules a wakeup timer for a job due in the future.
// In lottery mode, only winning workers schedule the wakeup.
func (w *Listening) handleFutureNotification(
	due time.Time,
	scheduleWakeup func(time.Time),
	retrySchedule func(),
) {
	var shouldSchedule bool

	if w.lotteryScheduling {
		if w.GetState() == StateIdle {
			idles := idleCount.Load()
			if idles > 0 {
				shouldSchedule = w.isWinner(idles)
			}
		}
	} else {
		shouldSchedule = true
	}

	if shouldSchedule {
		scheduleWakeup(due)

		next, err := w.queue.NextDue(w.ctx)
		if err != nil {
			retrySchedule()

			return
		}

		scheduleWakeup(next)
	}
}

// workContext returns a context bounded by the job processing timeout.
func (w *Listening) workContext() (context.Context, context.CancelFunc) {
	if w.opts.Timeout <= 0 {
		return w.ctx, func() {}
	}

	return context.WithTimeout(w.ctx, w.opts.Timeout)
}

// finalizeContext returns a context bounded by the finalization timeout.
// Uses context.Background so finalization can complete after worker shutdown.
func (w *Listening) finalizeContext() (context.Context, context.CancelFunc) {
	if w.opts.FinalizeTimeout <= 0 {
		return context.Background(), func() {}
	}

	return context.WithTimeout(context.Background(), w.opts.FinalizeTimeout)
}

// sleepOrStop waits for the reconnect delay or returns false if the
// worker context is cancelled.
func (w *Listening) sleepOrStop() bool {
	select {
	case <-time.After(w.reconnectDelay):
		return true
	case <-w.ctx.Done():
		return false
	}
}

// startNotificationListener starts a goroutine that receives NOTIFY messages from
// PostgreSQL and forwards them onto notifyCh. Errors are sent on errCh.
func startNotificationListener(
	ctx context.Context,
	conn *pgxpool.Conn,
) (<-chan *pgconn.Notification, <-chan error) {
	notifyCh := make(chan *pgconn.Notification, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(notifyCh)

		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				errCh <- err

				return
			}

			select {
			case notifyCh <- n:
			case <-ctx.Done():
				errCh <- ctx.Err()

				return
			default:
			}
		}
	}()

	return notifyCh, errCh
}

// parseNotification parses a NOTIFY payload as a Unix millisecond timestamp.
func parseNotification(payload string) (time.Time, bool) {
	ms, err := strconv.ParseInt(payload, 10, 64)
	if err != nil {
		return time.Time{}, false
	}

	return time.UnixMilli(ms), true
}

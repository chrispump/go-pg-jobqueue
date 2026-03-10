package worker_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/backoff"
	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
	"github.com/chrispump/go-pg-jobqueue/internal/worker"
)

// mockQueue implements queue.Queue for unit tests.
type mockQueue struct {
	mu                  sync.Mutex
	completeCalled      bool
	failCalled          bool
	scheduleRetryCalled bool
	failErr             error
	completeErr         error
	scheduleRetryErr    error
	lastError           string
}

func (q *mockQueue) Enqueue(_ context.Context, _ job.Job) error { return nil }

func (q *mockQueue) Dequeue(_ context.Context, _ string) (*job.Job, error) { return nil, nil }

func (q *mockQueue) ScheduleRetry(
	_ context.Context,
	_ int64,
	_ string,
	_ time.Duration,
	errMsg string,
) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.scheduleRetryCalled = true
	q.lastError = errMsg

	return q.scheduleRetryErr
}

func (q *mockQueue) Complete(_ context.Context, _ int64, _ string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.completeCalled = true

	return q.completeErr
}

func (q *mockQueue) Fail(_ context.Context, _ int64, _ string, errMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.failCalled = true
	q.lastError = errMsg

	return q.failErr
}

func (q *mockQueue) ExtendLease(_ context.Context, _ int64, _ string) (time.Time, error) {
	return time.Time{}, nil
}

func (q *mockQueue) LeaseTime() time.Duration                     { return 0 }
func (q *mockQueue) NextDue(_ context.Context) (time.Time, error) { return time.Time{}, nil }

// mockWorker implements worker.Worker for unit tests.
type mockWorker struct {
	mu     sync.Mutex
	state  worker.WorkerState
	stopCh chan struct{}
	once   sync.Once
}

func newMockWorker(initialState worker.WorkerState) *mockWorker {
	return &mockWorker{
		state:  initialState,
		stopCh: make(chan struct{}),
	}
}

func (w *mockWorker) GetID() uint                           { return 0 }
func (w *mockWorker) GetName() string                       { return "mock-worker" }
func (w *mockWorker) GetQueue() queue.Queue                 { return nil }
func (w *mockWorker) OnEvent(_ string, _ *job.Job, _ error) {}
func (w *mockWorker) Process(_ *job.Job) error              { return nil }

func (w *mockWorker) Start() { <-w.stopCh }

func (w *mockWorker) Stop() { w.once.Do(func() { close(w.stopCh) }) }

func (w *mockWorker) GetState() worker.WorkerState {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.state
}

func (w *mockWorker) SetState(s worker.WorkerState) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.state = s
}

// emptyHandlerRegistry is a HandlerRegistry with no handlers registered.
type emptyHandlerRegistry struct{}

func (r *emptyHandlerRegistry) Register(_ job.Type, _ job.Handler) {}
func (r *emptyHandlerRegistry) GetHandler(_ job.Type) (job.Handler, bool) {
	return nil, false
}

// newTestJob builds a processing-state job with the given attempt count.
func newTestJob(attempts uint) *job.Job {
	return job.NewFromData(
		1,
		job.NewPayload("test-data"),
		job.StatusProcessing,
		attempts,
		time.Time{},
		time.Time{},
		"test-worker",
		"test-token",
	)
}

// --- Manager ---

func TestManager_NilFactory_ReturnsError(t *testing.T) {
	_, err := worker.NewManager(context.Background(), "test", nil)
	if err == nil {
		t.Fatal("expected error for nil factory, got nil")
	}
}

func TestManager_StartWorker_IncrementsCount(t *testing.T) {
	ctx := t.Context()

	factory := func(_ context.Context, _ string, _ uint) worker.Worker {
		return newMockWorker(worker.StateIdle)
	}

	mgr, err := worker.NewManager(ctx, "test", factory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := mgr.StartWorker(); err != nil {
			t.Fatalf("StartWorker %d: %v", i, err)
		}
	}

	if got := mgr.Count(); got != 3 {
		t.Errorf("Count: want 3, got %d", got)
	}
}

func TestManager_StopAll_StopsAllWorkers(t *testing.T) {
	factory := func(_ context.Context, _ string, _ uint) worker.Worker {
		return newMockWorker(worker.StateIdle)
	}

	mgr, err := worker.NewManager(context.Background(), "test", factory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := mgr.StartWorker(); err != nil {
			t.Fatalf("StartWorker %d: %v", i, err)
		}
	}

	if stopped := mgr.StopAll(); stopped != 3 {
		t.Errorf("StopAll: want 3, got %d", stopped)
	}

	if got := mgr.Count(); got != 0 {
		t.Errorf("Count after StopAll: want 0, got %d", got)
	}
}

func TestManager_GetWorkerStates(t *testing.T) {
	states := []worker.WorkerState{worker.StateIdle, worker.StateFetching, worker.StateProcessing}
	idx := 0

	var mu sync.Mutex

	factory := func(_ context.Context, _ string, _ uint) worker.Worker {
		mu.Lock()
		s := states[idx]
		idx++
		mu.Unlock()

		return newMockWorker(s)
	}

	mgr, err := worker.NewManager(context.Background(), "test", factory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.StopAll()

	for i := 0; i < 3; i++ {
		if err := mgr.StartWorker(); err != nil {
			t.Fatalf("StartWorker %d: %v", i, err)
		}
	}

	snap := mgr.GetWorkerStates()
	if snap.Total != 3 {
		t.Errorf("Total: want 3, got %d", snap.Total)
	}

	if snap.Idle != 1 {
		t.Errorf("Idle: want 1, got %d", snap.Idle)
	}

	if snap.Fetching != 1 {
		t.Errorf("Fetching: want 1, got %d", snap.Fetching)
	}

	if snap.Processing != 1 {
		t.Errorf("Processing: want 1, got %d", snap.Processing)
	}
}

func TestManager_StartWorker_NilWorkerFromFactory(t *testing.T) {
	factory := func(_ context.Context, _ string, _ uint) worker.Worker { return nil }

	mgr, err := worker.NewManager(context.Background(), "test", factory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if err := mgr.StartWorker(); err == nil {
		t.Fatal("expected error when factory returns nil, got nil")
	}
}

// --- WorkerOptions ---

func TestNewDefaultWorkerOptions_Defaults(t *testing.T) {
	opts := worker.NewDefaultWorkerOptions()

	if opts.MaxAttempts != 5 {
		t.Errorf("MaxAttempts: want 5, got %d", opts.MaxAttempts)
	}

	if opts.Interval != 5*time.Second {
		t.Errorf("Interval: want 5s, got %v", opts.Interval)
	}

	if opts.Timeout != 30*time.Second {
		t.Errorf("Timeout: want 30s, got %v", opts.Timeout)
	}

	if opts.FinalizeTimeout != 10*time.Second {
		t.Errorf("FinalizeTimeout: want 10s, got %v", opts.FinalizeTimeout)
	}

	if opts.Backoff == nil {
		t.Error("Backoff should not be nil")
	}

	if opts.Registry == nil {
		t.Error("Registry should not be nil")
	}
}

func TestWithMaxAttempts(t *testing.T) {
	opts := worker.NewDefaultWorkerOptions(worker.WithMaxAttempts(10))
	if opts.MaxAttempts != 10 {
		t.Errorf("MaxAttempts: want 10, got %d", opts.MaxAttempts)
	}
}

func TestWithInterval(t *testing.T) {
	opts := worker.NewDefaultWorkerOptions(worker.WithInterval(1 * time.Second))
	if opts.Interval != 1*time.Second {
		t.Errorf("Interval: want 1s, got %v", opts.Interval)
	}
}

func TestWithTimeout(t *testing.T) {
	opts := worker.NewDefaultWorkerOptions(worker.WithTimeout(15 * time.Second))
	if opts.Timeout != 15*time.Second {
		t.Errorf("Timeout: want 15s, got %v", opts.Timeout)
	}
}

func TestWithFinalizeTimeout(t *testing.T) {
	opts := worker.NewDefaultWorkerOptions(worker.WithFinalizeTimeout(5 * time.Second))
	if opts.FinalizeTimeout != 5*time.Second {
		t.Errorf("FinalizeTimeout: want 5s, got %v", opts.FinalizeTimeout)
	}
}

// --- WorkerEventLogger ---

func TestWorkerEventLogger_Log_WritesToFile(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "worker.log")

	logger, err := worker.NewWorkerEventLogger(logFile)
	if err != nil {
		t.Fatalf("NewWorkerEventLogger: %v", err)
	}

	logger.Log(worker.WorkerEvent{
		At:         time.Now(),
		WorkerName: "test-worker",
		Event:      "test_event",
	})

	if err := logger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(logFile)
	if err != nil {
		t.Fatalf("open log file: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		t.Fatal("expected at least one line in log file")
	}

	var event worker.WorkerEvent
	if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}

	if event.WorkerName != "test-worker" {
		t.Errorf("WorkerName: want %q, got %q", "test-worker", event.WorkerName)
	}

	if event.Event != "test_event" {
		t.Errorf("Event: want %q, got %q", "test_event", event.Event)
	}
}

func TestWorkerEventLogger_Close(t *testing.T) {
	logger, err := worker.NewWorkerEventLogger(filepath.Join(t.TempDir(), "worker.log"))
	if err != nil {
		t.Fatalf("NewWorkerEventLogger: %v", err)
	}

	if err := logger.Close(); err != nil {
		t.Errorf("Close returned unexpected error: %v", err)
	}
}

// --- Polling.Process ---

func TestPollingWorker_Process_Success(t *testing.T) {
	mq := &mockQueue{}
	w := worker.NewPollingWorker(context.Background(), "test", 0, mq,
		worker.WithInterval(time.Hour),
		worker.WithTimeout(5*time.Second),
		worker.WithFinalizeTimeout(5*time.Second),
	)

	if err := w.Process(newTestJob(1)); err != nil {
		t.Fatalf("Process: %v", err)
	}

	mq.mu.Lock()
	defer mq.mu.Unlock()

	if !mq.completeCalled {
		t.Error("expected Complete to be called")
	}

	if mq.failCalled {
		t.Error("expected Fail not to be called")
	}
}

func TestPollingWorker_Process_HandlerNotFound(t *testing.T) {
	mq := &mockQueue{}
	w := worker.NewPollingWorker(context.Background(), "test", 0, mq,
		worker.WithRegistry(&emptyHandlerRegistry{}),
		worker.WithInterval(time.Hour),
		worker.WithTimeout(5*time.Second),
		worker.WithFinalizeTimeout(5*time.Second),
	)

	w.Process(newTestJob(1)) //nolint:errcheck

	mq.mu.Lock()
	defer mq.mu.Unlock()

	if !mq.failCalled {
		t.Error("expected Fail to be called when handler not found")
	}

	if mq.completeCalled {
		t.Error("expected Complete not to be called")
	}
}

//nolint:dupl
func TestPollingWorker_Process_Error_ScheduleRetry(t *testing.T) {
	mq := &mockQueue{}
	reg := job.NewDefaultHandlerRegistry()
	reg.Register(job.TypeNoop, func(_ context.Context, _ *job.Job) error {
		return errors.New("simulated failure")
	})

	w := worker.NewPollingWorker(context.Background(), "test", 0, mq,
		worker.WithRegistry(reg),
		worker.WithMaxAttempts(5),
		worker.WithBackoff(backoff.NewConstant(0)),
		worker.WithInterval(time.Hour),
		worker.WithTimeout(5*time.Second),
		worker.WithFinalizeTimeout(5*time.Second),
	)

	// attempts=1 < maxAttempts=5 → retry expected
	w.Process(newTestJob(1)) //nolint:errcheck

	mq.mu.Lock()
	defer mq.mu.Unlock()

	if !mq.scheduleRetryCalled {
		t.Error("expected ScheduleRetry to be called")
	}

	if mq.failCalled {
		t.Error("expected Fail not to be called")
	}
}

//nolint:dupl
func TestPollingWorker_Process_Error_FinalFail(t *testing.T) {
	mq := &mockQueue{}
	reg := job.NewDefaultHandlerRegistry()
	reg.Register(job.TypeNoop, func(_ context.Context, _ *job.Job) error {
		return errors.New("simulated failure")
	})

	w := worker.NewPollingWorker(context.Background(), "test", 0, mq,
		worker.WithRegistry(reg),
		worker.WithMaxAttempts(5),
		worker.WithBackoff(backoff.NewConstant(0)),
		worker.WithInterval(time.Hour),
		worker.WithTimeout(5*time.Second),
		worker.WithFinalizeTimeout(5*time.Second),
	)

	// attempts == maxAttempts → permanent fail expected
	w.Process(newTestJob(5)) //nolint:errcheck

	mq.mu.Lock()
	defer mq.mu.Unlock()

	if !mq.failCalled {
		t.Error("expected Fail to be called on final attempt")
	}

	if mq.scheduleRetryCalled {
		t.Error("expected ScheduleRetry not to be called on final attempt")
	}
}

func TestPollingWorker_Process_OnAttempt_Called(t *testing.T) {
	mq := &mockQueue{}

	var (
		recordMu sync.Mutex
		records  []worker.AttemptRecord
	)

	w := worker.NewPollingWorker(context.Background(), "test", 0, mq,
		worker.WithInterval(time.Hour),
		worker.WithTimeout(5*time.Second),
		worker.WithFinalizeTimeout(5*time.Second),
		worker.WithOnAttempt(func(r worker.AttemptRecord) {
			recordMu.Lock()
			defer recordMu.Unlock()

			records = append(records, r)
		}),
	)

	j := newTestJob(1)
	if err := w.Process(j); err != nil {
		t.Fatalf("Process: %v", err)
	}

	recordMu.Lock()
	defer recordMu.Unlock()

	if len(records) == 0 {
		t.Fatal("expected OnAttempt to be called at least once")
	}

	r := records[0]
	if r.JobID != j.ID {
		t.Errorf("AttemptRecord.JobID: want %d, got %d", j.ID, r.JobID)
	}

	if r.WorkerName != "test-0" {
		t.Errorf("AttemptRecord.WorkerName: want %q, got %q", "test-0", r.WorkerName)
	}

	if !r.Success {
		t.Error("expected AttemptRecord.Success == true")
	}
}

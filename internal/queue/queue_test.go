package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
)

// mockQueue is a minimal in-memory Queue implementation for unit tests.
type mockQueue struct {
	jobs       []job.Job
	dequeueErr error
	enqueueErr error
}

func (m *mockQueue) Enqueue(_ context.Context, j job.Job) error {
	if m.enqueueErr != nil {
		return m.enqueueErr
	}

	m.jobs = append(m.jobs, j)

	return nil
}

func (m *mockQueue) Dequeue(_ context.Context, _ string) (*job.Job, error) {
	if m.dequeueErr != nil {
		return nil, m.dequeueErr
	}

	if len(m.jobs) == 0 {
		return nil, nil
	}

	j := m.jobs[0]
	m.jobs = m.jobs[1:]

	return &j, nil
}

func (m *mockQueue) ScheduleRetry(
	_ context.Context, _ int64, _ string, _ time.Duration, _ string,
) error {
	return nil
}

func (m *mockQueue) Complete(_ context.Context, _ int64, _ string) error       { return nil }
func (m *mockQueue) Fail(_ context.Context, _ int64, _ string, _ string) error { return nil }
func (m *mockQueue) ExtendLease(_ context.Context, _ int64, _ string) (time.Time, error) {
	return time.Now().Add(time.Minute), nil
}
func (m *mockQueue) LeaseTime() time.Duration                     { return 30 * time.Second }
func (m *mockQueue) NextDue(_ context.Context) (time.Time, error) { return time.Now(), nil }

// --- CounterQueue Tests ---

func TestCounterQueue_DequeueCountsOnSuccess(t *testing.T) {
	inner := &mockQueue{jobs: []job.Job{
		job.New(job.NewPayload("a")),
		job.New(job.NewPayload("b")),
	}}
	cq := queue.NewCounterQueue(inner)
	ctx := context.Background()

	cq.Dequeue(ctx, "w1") // gets job
	cq.Dequeue(ctx, "w1") // gets job
	cq.Dequeue(ctx, "w1") // empty

	calls, empty, errs := cq.DequeueCounts()
	if calls != 3 {
		t.Errorf("calls: want 3, got %d", calls)
	}

	if empty != 1 {
		t.Errorf("empty: want 1, got %d", empty)
	}

	if errs != 0 {
		t.Errorf("errors: want 0, got %d", errs)
	}
}

func TestCounterQueue_DequeueCountsOnError(t *testing.T) {
	inner := &mockQueue{dequeueErr: errors.New("db error")}
	cq := queue.NewCounterQueue(inner)
	ctx := context.Background()

	cq.Dequeue(ctx, "w1")
	cq.Dequeue(ctx, "w1")

	calls, empty, errs := cq.DequeueCounts()
	if calls != 2 {
		t.Errorf("calls: want 2, got %d", calls)
	}

	if empty != 0 {
		t.Errorf("empty: want 0, got %d", empty)
	}

	if errs != 2 {
		t.Errorf("errors: want 2, got %d", errs)
	}
}

func TestCounterQueue_DelegatesEnqueue(t *testing.T) {
	inner := &mockQueue{}
	cq := queue.NewCounterQueue(inner)

	err := cq.Enqueue(context.Background(), job.New(job.NewPayload("x")))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if len(inner.jobs) != 1 {
		t.Errorf("expected 1 job in inner queue, got %d", len(inner.jobs))
	}
}

func TestCounterQueue_DelegatesEnqueueError(t *testing.T) {
	inner := &mockQueue{enqueueErr: errors.New("insert failed")}
	cq := queue.NewCounterQueue(inner)

	err := cq.Enqueue(context.Background(), job.New(job.NewPayload("x")))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestCounterQueue_InitialCountsAreZero(t *testing.T) {
	cq := queue.NewCounterQueue(&mockQueue{})

	calls, empty, errs := cq.DequeueCounts()
	if calls != 0 || empty != 0 || errs != 0 {
		t.Errorf("expected all-zero counts, got calls=%d empty=%d errs=%d", calls, empty, errs)
	}
}

// --- OutageQueue Tests ---

func TestOutageQueue_NormalOperation(t *testing.T) {
	inner := &mockQueue{jobs: []job.Job{job.New(job.NewPayload("test"))}}
	oq := queue.NewOutageQueue(inner)
	ctx := context.Background()

	j, err := oq.Dequeue(ctx, "w1")
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if j == nil {
		t.Fatal("expected job, got nil")
	}
}

func TestOutageQueue_DuringOutage_AllOpsReturnError(t *testing.T) {
	inner := &mockQueue{jobs: []job.Job{job.New(job.NewPayload("test"))}}
	oq := queue.NewOutageQueue(inner)
	ctx := context.Background()

	oq.StartOutage(10 * time.Second)

	if _, err := oq.Dequeue(ctx, "w1"); !errors.Is(err, queue.ErrSimulatedQueueOutage) {
		t.Errorf("Dequeue: want ErrSimulatedQueueOutage, got %v", err)
	}

	if err := oq.Enqueue(
		ctx,
		job.New(job.NewPayload("x")),
	); !errors.Is(
		err,
		queue.ErrSimulatedQueueOutage,
	) {
		t.Errorf("Enqueue: want ErrSimulatedQueueOutage, got %v", err)
	}

	if err := oq.Complete(ctx, 1, "tok"); !errors.Is(err, queue.ErrSimulatedQueueOutage) {
		t.Errorf("Complete: want ErrSimulatedQueueOutage, got %v", err)
	}

	if err := oq.Fail(ctx, 1, "tok", "err"); !errors.Is(err, queue.ErrSimulatedQueueOutage) {
		t.Errorf("Fail: want ErrSimulatedQueueOutage, got %v", err)
	}

	if err := oq.ScheduleRetry(
		ctx,
		1,
		"tok",
		0,
		"err",
	); !errors.Is(
		err,
		queue.ErrSimulatedQueueOutage,
	) {
		t.Errorf("ScheduleRetry: want ErrSimulatedQueueOutage, got %v", err)
	}

	if _, err := oq.ExtendLease(ctx, 1, "tok"); !errors.Is(err, queue.ErrSimulatedQueueOutage) {
		t.Errorf("ExtendLease: want ErrSimulatedQueueOutage, got %v", err)
	}

	if _, err := oq.NextDue(ctx); !errors.Is(err, queue.ErrSimulatedQueueOutage) {
		t.Errorf("NextDue: want ErrSimulatedQueueOutage, got %v", err)
	}
}

func TestOutageQueue_AfterOutageExpires(t *testing.T) {
	inner := &mockQueue{jobs: []job.Job{job.New(job.NewPayload("test"))}}
	oq := queue.NewOutageQueue(inner)
	ctx := context.Background()

	oq.StartOutage(10 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	j, err := oq.Dequeue(ctx, "w1")
	if err != nil {
		t.Fatalf("after outage Dequeue: %v", err)
	}

	if j == nil {
		t.Fatal("expected job after outage expires")
	}
}

func TestOutageQueue_StartOutage_DoesNotShorten(t *testing.T) {
	oq := queue.NewOutageQueue(&mockQueue{})

	oq.StartOutage(10 * time.Second)
	oq.StartOutage(1 * time.Millisecond) // should NOT shorten the outage

	if err := oq.Err(); !errors.Is(err, queue.ErrSimulatedQueueOutage) {
		t.Error("outage should still be active")
	}
}

func TestOutageQueue_Err_NoOutage(t *testing.T) {
	oq := queue.NewOutageQueue(&mockQueue{})
	if err := oq.Err(); err != nil {
		t.Errorf("expected nil before any outage, got %v", err)
	}
}

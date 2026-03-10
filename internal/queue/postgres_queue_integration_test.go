//go:build integration

package queue_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/chrispump/go-pg-jobqueue/internal/db"
	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
)

var (
	integrationDB    *db.DB
	integrationSQLDB *sql.DB
)

type testConfig struct {
	Database db.Config `yaml:"database"`
}

func loadTestConfig(path string) (testConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return testConfig{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var cfg testConfig
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return testConfig{}, fmt.Errorf("decode config: %w", err)
	}

	return cfg, nil
}

func TestMain(m *testing.M) {
	configPath := flag.String("config", "../../config.yml", "path to config.yml")
	flag.Parse()

	cfg, err := loadTestConfig(*configPath)
	if err != nil {
		log.Printf("skipping integration tests: %v", err)
		os.Exit(0)
	}

	integrationDB, err = db.NewPostgresDatabase(context.Background(), db.BuildDSN(cfg.Database))
	if err != nil {
		log.Printf("skipping integration tests: %v", err)
		os.Exit(0)
	}
	defer integrationDB.Close()

	if err := integrationDB.ResetAndMigrate(); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	integrationSQLDB = integrationDB.StdlibDB()
	defer integrationSQLDB.Close()

	os.Exit(m.Run())
}

func truncate(t *testing.T) {
	t.Helper()
	if err := integrationDB.TruncateJobsTable(context.Background()); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func newPGQueue(t *testing.T, opts ...func(*queue.PostgresQueueOptions)) *queue.PostgresQueue {
	t.Helper()
	return queue.NewPostgresQueue(integrationSQLDB, opts...)
}

func TestPostgresQueue_Enqueue_Dequeue(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	if err := q.Enqueue(ctx, job.New(job.NewPayload("enqueue-test"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q.Dequeue(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got == nil {
		t.Fatal("expected job, got nil")
	}
	if got.Payload.Data != "enqueue-test" {
		t.Errorf("payload data: want %q, got %q", "enqueue-test", got.Payload.Data)
	}
	if got.LockToken == "" {
		t.Error("expected non-empty lock token")
	}
	if got.LockedBy != "worker-1" {
		t.Errorf("locked_by: want %q, got %q", "worker-1", got.LockedBy)
	}
}

func TestPostgresQueue_Dequeue_EmptyQueue(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	got, err := q.Dequeue(ctx, "worker-1")
	if err != nil {
		t.Fatalf("unexpected error on empty queue: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil on empty queue, got %+v", got)
	}
}

func TestPostgresQueue_Dequeue_EmptyWorkerID(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	_, err := q.Dequeue(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty workerID, got nil")
	}
}

func TestPostgresQueue_Complete(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	if err := q.Enqueue(ctx, job.New(job.NewPayload("complete-test"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q.Dequeue(ctx, "worker-1")
	if err != nil || got == nil {
		t.Fatalf("Dequeue: err=%v, job=%v", err, got)
	}

	if err := q.Complete(ctx, got.ID, got.LockToken); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Queue must be empty now
	next, err := q.Dequeue(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Dequeue after Complete: %v", err)
	}
	if next != nil {
		t.Error("expected empty queue after Complete")
	}
}

func TestPostgresQueue_Complete_WrongToken(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	q.Enqueue(ctx, job.New(job.NewPayload("test")))
	got, _ := q.Dequeue(ctx, "worker-1")

	if err := q.Complete(ctx, got.ID, "wrong-token"); err == nil {
		t.Fatal("expected error for wrong lock token, got nil")
	}
}

func TestPostgresQueue_Fail_MovesToDLQ(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	q.Enqueue(ctx, job.New(job.NewPayload("fail-test")))
	got, _ := q.Dequeue(ctx, "worker-1")

	if err := q.Fail(ctx, got.ID, got.LockToken, "permanent error"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	count, err := q.DLQCount(ctx)
	if err != nil {
		t.Fatalf("DLQCount: %v", err)
	}
	if count != 1 {
		t.Errorf("DLQCount: want 1, got %d", count)
	}

	// Failed jobs must not be dequeued again
	next, err := q.Dequeue(ctx, "worker-2")
	if err != nil {
		t.Fatalf("Dequeue after Fail: %v", err)
	}
	if next != nil {
		t.Error("failed job must not be dequeued again")
	}
}

func TestPostgresQueue_Fail_WrongToken(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	q.Enqueue(ctx, job.New(job.NewPayload("test")))
	got, _ := q.Dequeue(ctx, "worker-1")

	if err := q.Fail(ctx, got.ID, "wrong-token", "err"); err == nil {
		t.Fatal("expected error for wrong lock token, got nil")
	}
}

func TestPostgresQueue_ScheduleRetry(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	q.Enqueue(ctx, job.New(job.NewPayload("retry-test")))
	got, _ := q.Dequeue(ctx, "worker-1")

	if err := q.ScheduleRetry(ctx, got.ID, got.LockToken, 0, "transient error"); err != nil {
		t.Fatalf("ScheduleRetry: %v", err)
	}

	retry, err := q.Dequeue(ctx, "worker-2")
	if err != nil {
		t.Fatalf("Dequeue after ScheduleRetry: %v", err)
	}
	if retry == nil {
		t.Fatal("expected job to be available again after ScheduleRetry with zero delay")
	}
	if retry.Attempts < 2 {
		t.Errorf("expected attempts >= 2, got %d", retry.Attempts)
	}
}

func TestPostgresQueue_ScheduleRetry_WrongToken(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	q.Enqueue(ctx, job.New(job.NewPayload("test")))
	got, _ := q.Dequeue(ctx, "worker-1")

	if err := q.ScheduleRetry(ctx, got.ID, "wrong-token", 0, "err"); err == nil {
		t.Fatal("expected error for wrong lock token, got nil")
	}
}

func TestPostgresQueue_ExtendLease(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	q.Enqueue(ctx, job.New(job.NewPayload("extend-test")))
	got, _ := q.Dequeue(ctx, "worker-1")

	newExpiry, err := q.ExtendLease(ctx, got.ID, got.LockToken)
	if err != nil {
		t.Fatalf("ExtendLease: %v", err)
	}
	if !newExpiry.After(got.LockUntil) {
		t.Errorf("new expiry %v must be after original %v", newExpiry, got.LockUntil)
	}

	q.Complete(ctx, got.ID, got.LockToken)
}

func TestPostgresQueue_ExtendLease_WrongToken(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	q.Enqueue(ctx, job.New(job.NewPayload("test")))
	got, _ := q.Dequeue(ctx, "worker-1")

	if _, err := q.ExtendLease(ctx, got.ID, "wrong-token"); err == nil {
		t.Fatal("expected error for wrong lock token, got nil")
	}
}

func TestPostgresQueue_NextDue_EmptyQueue(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	due, err := q.NextDue(ctx)
	if err != nil {
		t.Fatalf("NextDue: %v", err)
	}
	if !due.IsZero() {
		t.Errorf("expected zero time on empty queue, got %v", due)
	}
}

func TestPostgresQueue_NextDue_WithPendingJob(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	q.Enqueue(ctx, job.New(job.NewPayload("test")))

	due, err := q.NextDue(ctx)
	if err != nil {
		t.Fatalf("NextDue: %v", err)
	}
	if due.IsZero() {
		t.Error("expected non-zero NextDue with pending job")
	}
}

func TestPostgresQueue_DLQList(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	for i := 0; i < 2; i++ {
		q.Enqueue(ctx, job.New(job.NewPayload("dlq-job")))
		got, _ := q.Dequeue(ctx, "worker-1")
		q.Fail(ctx, got.ID, got.LockToken, "perm error")
	}

	entries, err := q.DLQList(ctx, 10, 0)
	if err != nil {
		t.Fatalf("DLQList: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 DLQ entries, got %d", len(entries))
	}
	for _, e := range entries {
		if e.Reason != "perm error" {
			t.Errorf("unexpected reason: %q", e.Reason)
		}
		if e.FailedAt.IsZero() {
			t.Error("FailedAt should not be zero")
		}
	}
}

func TestPostgresQueue_DLQList_Pagination(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t)

	for i := 0; i < 3; i++ {
		q.Enqueue(ctx, job.New(job.NewPayload("dlq-job")))
		got, _ := q.Dequeue(ctx, "worker-1")
		q.Fail(ctx, got.ID, got.LockToken, "err")
	}

	page1, err := q.DLQList(ctx, 2, 0)
	if err != nil {
		t.Fatalf("DLQList page1: %v", err)
	}
	if len(page1) != 2 {
		t.Errorf("page1: want 2, got %d", len(page1))
	}

	page2, err := q.DLQList(ctx, 2, 2)
	if err != nil {
		t.Fatalf("DLQList page2: %v", err)
	}
	if len(page2) != 1 {
		t.Errorf("page2: want 1, got %d", len(page2))
	}
}

func TestPostgresQueue_LockMode_SkipLocked(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t, queue.WithLockMode(queue.PgLockModeSkip))

	q.Enqueue(ctx, job.New(job.NewPayload("skip-locked")))

	got1, err := q.Dequeue(ctx, "worker-1")
	if err != nil || got1 == nil {
		t.Fatalf("first Dequeue: err=%v, job=%v", err, got1)
	}

	// Job is locked — SKIP LOCKED must skip it
	got2, err := q.Dequeue(ctx, "worker-2")
	if err != nil {
		t.Fatalf("second Dequeue: %v", err)
	}
	if got2 != nil {
		t.Error("SKIP LOCKED: second worker should not see a locked job")
	}

	q.Complete(ctx, got1.ID, got1.LockToken)
}

func TestPostgresQueue_LeaseExpiry_Recovery(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t, queue.WithLeaseTime(50*time.Millisecond))

	q.Enqueue(ctx, job.New(job.NewPayload("lease-recovery")))

	got1, err := q.Dequeue(ctx, "worker-1")
	if err != nil || got1 == nil {
		t.Fatalf("first Dequeue: err=%v, job=%v", err, got1)
	}

	// Wait for lease to expire
	time.Sleep(100 * time.Millisecond)

	got2, err := q.Dequeue(ctx, "worker-2")
	if err != nil {
		t.Fatalf("Dequeue after lease expiry: %v", err)
	}
	if got2 == nil {
		t.Fatal("expected job to be recoverable after lease expiry")
	}
	if got2.ID != got1.ID {
		t.Errorf("expected same job ID %d, got %d", got1.ID, got2.ID)
	}

	q.Complete(ctx, got2.ID, got2.LockToken)
}

func TestPostgresQueue_AtLeastOnce_NoJobLost(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	q := newPGQueue(t, queue.WithLeaseTime(50*time.Millisecond))

	const n = 10
	for i := 0; i < n; i++ {
		q.Enqueue(ctx, job.New(job.NewPayload("consistency")))
	}

	completed := 0
	for {
		// Simulate worker crash by abandoning every other job (no Complete call)
		j, err := q.Dequeue(ctx, "worker-1")
		if err != nil || j == nil {
			break
		}
		if j.Attempts%2 == 0 {
			q.Complete(ctx, j.ID, j.LockToken)
			completed++
		}
		// odd attempts: abandoned — lease will expire and job will be recovered
	}

	// Wait for all leases to expire, then drain remaining jobs
	time.Sleep(150 * time.Millisecond)
	for {
		j, err := q.Dequeue(ctx, "worker-2")
		if err != nil || j == nil {
			break
		}
		q.Complete(ctx, j.ID, j.LockToken)
		completed++
	}

	if completed != n {
		t.Errorf("at-least-once violated: completed %d of %d jobs", completed, n)
	}
}

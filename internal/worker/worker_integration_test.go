//go:build integration

package worker_test

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/chrispump/go-pg-jobqueue/internal/backoff"
	"github.com/chrispump/go-pg-jobqueue/internal/db"
	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
	"github.com/chrispump/go-pg-jobqueue/internal/worker"
)

var (
	integrationDB    *db.DB
	integrationSQLDB *sql.DB
)

type workerTestConfig struct {
	Database db.Config `yaml:"database"`
}

func loadWorkerTestConfig(path string) (workerTestConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return workerTestConfig{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var cfg workerTestConfig
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return workerTestConfig{}, fmt.Errorf("decode config: %w", err)
	}

	return cfg, nil
}

func TestMain(m *testing.M) {
	configPath := flag.String("config", "../../config.yml", "path to config.yml")
	flag.Parse()

	cfg, err := loadWorkerTestConfig(*configPath)
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

func waitForCondition(t *testing.T, timeout time.Duration, check func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func TestPollingWorker_ProcessesJobs(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	q := queue.NewPostgresQueue(integrationSQLDB, queue.WithLockMode(queue.PgLockModeSkip))

	// Enqueue 5 noop jobs
	for i := 0; i < 5; i++ {
		if err := q.Enqueue(ctx, job.New(job.NewPayload(fmt.Sprintf("job-%d", i)))); err != nil {
			t.Fatalf("Enqueue job %d: %v", i, err)
		}
	}

	factory := func(ctx context.Context, label string, id uint) worker.Worker {
		return worker.NewPollingWorker(ctx, label, id, q,
			worker.WithInterval(10*time.Millisecond),
			worker.WithTimeout(2*time.Second),
			worker.WithFinalizeTimeout(2*time.Second),
		)
	}

	mgr, err := worker.NewManager(ctx, "test-polling", factory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := mgr.StartWorker(); err != nil {
			t.Fatalf("StartWorker %d: %v", i, err)
		}
	}

	allCompleted := waitForCondition(t, 5*time.Second, func() bool {
		jobs, err := integrationDB.LoadJobs(ctx, string(job.StatusCompleted))
		if err != nil {
			return false
		}
		return len(jobs) == 5
	})

	mgr.StopAll()

	if !allCompleted {
		jobs, _ := integrationDB.LoadJobs(ctx, string(job.StatusCompleted))
		t.Errorf("expected 5 completed jobs, got %d", len(jobs))
	}
}

func TestPollingWorker_Retry_ThenSucceed(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	q := queue.NewPostgresQueue(integrationSQLDB, queue.WithLockMode(queue.PgLockModeSkip))

	if err := q.Enqueue(ctx, job.New(job.NewPayload("retry-job"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var callCount atomic.Int32

	reg := job.NewDefaultHandlerRegistry()
	reg.Register(job.TypeNoop, func(_ context.Context, _ *job.Job) error {
		n := callCount.Add(1)
		if n <= 2 {
			return errors.New("transient failure")
		}
		return nil
	})

	factory := func(ctx context.Context, label string, id uint) worker.Worker {
		return worker.NewPollingWorker(ctx, label, id, q,
			worker.WithRegistry(reg),
			worker.WithMaxAttempts(5),
			worker.WithBackoff(backoff.NewConstant(0)),
			worker.WithInterval(10*time.Millisecond),
			worker.WithTimeout(2*time.Second),
			worker.WithFinalizeTimeout(2*time.Second),
		)
	}

	mgr, err := worker.NewManager(ctx, "test-retry", factory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if err := mgr.StartWorker(); err != nil {
		t.Fatalf("StartWorker: %v", err)
	}

	completed := waitForCondition(t, 5*time.Second, func() bool {
		jobs, err := integrationDB.LoadJobs(ctx, string(job.StatusCompleted))
		if err != nil {
			return false
		}
		return len(jobs) == 1
	})

	mgr.StopAll()

	if !completed {
		t.Fatal("expected job to complete after retries")
	}

	jobs, err := integrationDB.LoadJobs(ctx, string(job.StatusCompleted))
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 completed job, got %d", len(jobs))
	}
	if jobs[0].Attempts < 2 {
		t.Errorf("expected attempts >= 2, got %d", jobs[0].Attempts)
	}
}

func TestPollingWorker_ExceedsMaxAttempts_MovesToDLQ(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	q := queue.NewPostgresQueue(integrationSQLDB, queue.WithLockMode(queue.PgLockModeSkip))

	if err := q.Enqueue(ctx, job.New(job.NewPayload("dlq-job"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	reg := job.NewDefaultHandlerRegistry()
	reg.Register(job.TypeNoop, func(_ context.Context, _ *job.Job) error {
		return errors.New("permanent failure")
	})

	const maxAttempts uint = 3

	factory := func(ctx context.Context, label string, id uint) worker.Worker {
		return worker.NewPollingWorker(ctx, label, id, q,
			worker.WithRegistry(reg),
			worker.WithMaxAttempts(maxAttempts),
			worker.WithBackoff(backoff.NewConstant(0)),
			worker.WithInterval(10*time.Millisecond),
			worker.WithTimeout(2*time.Second),
			worker.WithFinalizeTimeout(2*time.Second),
		)
	}

	mgr, err := worker.NewManager(ctx, "test-dlq", factory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if err := mgr.StartWorker(); err != nil {
		t.Fatalf("StartWorker: %v", err)
	}

	failed := waitForCondition(t, 5*time.Second, func() bool {
		jobs, err := integrationDB.LoadJobs(ctx, string(job.StatusFailed))
		if err != nil {
			return false
		}
		return len(jobs) == 1
	})

	mgr.StopAll()

	if !failed {
		t.Fatal("expected job to be moved to DLQ (status=failed)")
	}

	jobs, err := integrationDB.LoadJobs(ctx, string(job.StatusFailed))
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 failed job, got %d", len(jobs))
	}
	if uint(jobs[0].Attempts) != maxAttempts {
		t.Errorf("expected attempts == %d, got %d", maxAttempts, jobs[0].Attempts)
	}
}

func TestPollingWorker_Stop_GracefulShutdown(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	q := queue.NewPostgresQueue(integrationSQLDB, queue.WithLockMode(queue.PgLockModeSkip))

	factory := func(ctx context.Context, label string, id uint) worker.Worker {
		return worker.NewPollingWorker(ctx, label, id, q,
			worker.WithInterval(100*time.Millisecond),
			worker.WithTimeout(2*time.Second),
			worker.WithFinalizeTimeout(2*time.Second),
		)
	}

	mgr, err := worker.NewManager(ctx, "test-stop", factory)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if err := mgr.StartWorker(); err != nil {
		t.Fatalf("StartWorker: %v", err)
	}

	done := make(chan int)
	go func() {
		done <- mgr.StopAll()
	}()

	select {
	case stopped := <-done:
		if stopped != 1 {
			t.Errorf("StopAll: want 1, got %d", stopped)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StopAll hung: graceful shutdown did not complete in time")
	}

	if got := mgr.Count(); got != 0 {
		t.Errorf("Count after StopAll: want 0, got %d", got)
	}
}

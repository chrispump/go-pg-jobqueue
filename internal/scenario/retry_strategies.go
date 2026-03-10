package scenario

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/backoff"
	"github.com/chrispump/go-pg-jobqueue/internal/db"
	"github.com/chrispump/go-pg-jobqueue/internal/export"
	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
	"github.com/chrispump/go-pg-jobqueue/internal/worker"
)

const (
	retryBaseDelay         = 100 * time.Millisecond
	retryMaxAttempts       = 5
	retryJobsPerCase       = 1_000
	retryWorkerCount       = 16
	retryCaseTimeout       = 60 * time.Second
	retryFinalizeTimeout   = 30 * time.Second
	retryWorkerInterval    = 50 * time.Millisecond
	retryTickerInterval    = 100 * time.Millisecond
	percentFactor          = 100
	retryExponentialFactor = 2.0
)

var errCaseDurationReached = errors.New("case duration reached")

// RetryStrategiesScenario compares retry strategies (constant, linear, exponential) with different jitter options.
type RetryStrategiesScenario struct {
	strategies      []backoff.Backoff
	errorRate       float32
	maxAttempts     uint
	jobsPerCase     uint
	workerCount     uint
	lockHold        time.Duration
	caseTimeout     time.Duration
	finalizeTimeout time.Duration
	processedJobs   uint32
}

func NewRetryStrategiesScenario() Scenario {
	return &RetryStrategiesScenario{
		strategies: []backoff.Backoff{
			// Constant backoff
			backoff.NewConstant(retryBaseDelay),
			backoff.NewConstant(retryBaseDelay, backoff.WithJitter(backoff.NewFullJitter)),
			backoff.NewConstant(retryBaseDelay, backoff.WithJitter(backoff.NewEqualJitter)),

			// Linear backoff
			backoff.NewLinear(retryBaseDelay, retryBaseDelay),
			backoff.NewLinear(
				retryBaseDelay,
				retryBaseDelay,
				backoff.WithJitter(backoff.NewFullJitter),
			),
			backoff.NewLinear(
				retryBaseDelay,
				retryBaseDelay,
				backoff.WithJitter(backoff.NewEqualJitter),
			),

			// Exponential backoff
			backoff.NewExponential(retryBaseDelay, retryExponentialFactor),
			backoff.NewExponential(
				retryBaseDelay,
				retryExponentialFactor,
				backoff.WithJitter(backoff.NewFullJitter),
			),
			backoff.NewExponential(
				retryBaseDelay,
				retryExponentialFactor,
				backoff.WithJitter(backoff.NewEqualJitter),
			),
		},
		errorRate:       1.0,
		maxAttempts:     retryMaxAttempts,
		jobsPerCase:     retryJobsPerCase,
		workerCount:     retryWorkerCount,
		lockHold:        0 * time.Millisecond,
		caseTimeout:     retryCaseTimeout,
		finalizeTimeout: retryFinalizeTimeout,
	}
}

func (s RetryStrategiesScenario) Name() string {
	return "retry_strategies"
}

func (s RetryStrategiesScenario) Describe() string {
	return "Compares retry strategies with different jitter options under a fixed error rate. " +
		"Measures how jitter affects retry timing distribution."
}

func (s *RetryStrategiesScenario) Run(parentCtx context.Context, dbconn *db.DB) (Result, error) {
	// Create worker event logger
	logger, err := worker.NewWorkerEventLogger(fmt.Sprintf("logs/worker_%s.log", s.Name()))
	if err != nil {
		return Result{}, fmt.Errorf("failed to create worker logger: %w", err)
	}
	defer logger.Close()

	var (
		allAttempts []export.RetryAttemptRow
		attemptsMu  sync.Mutex
	)

	for i, strategy := range s.strategies {
		caseID := uint(i + 1)
		log.Printf("[%s][Case %d] Starting: %s", s.Name(), caseID, strategy.String())

		if err := s.runSingleCase(
			parentCtx,
			dbconn,
			caseID,
			strategy,
			&allAttempts,
			&attemptsMu,
			logger,
		); err != nil {
			return Result{}, err
		}
	}

	resultsFile, err := export.ScenarioCSVPath(s.Name(), "results")
	if err != nil {
		return Result{}, err
	}

	if err := export.ExportRetryAttemptsToCSV(allAttempts, resultsFile); err != nil {
		return Result{}, err
	}

	log.Printf("[%s] Exported %d attempt records to %s", s.Name(), len(allAttempts), resultsFile)

	return Result{FilePath: resultsFile}, nil
}

func (s *RetryStrategiesScenario) runSingleCase(
	parentCtx context.Context,
	dbconn *db.DB,
	caseID uint,
	strategy backoff.Backoff,
	allAttempts *[]export.RetryAttemptRow,
	attemptsMu *sync.Mutex,
	logger *worker.WorkerEventLogger,
) error {
	caseCtx, caseCancel := context.WithCancel(parentCtx)
	defer caseCancel()

	strategyLabel := strategy.String()

	if err := dbconn.TruncateJobsTable(caseCtx); err != nil {
		return err
	}

	q := queue.NewPostgresQueue(
		dbconn.StdlibDB(),
		queue.WithLockMode(queue.PgLockModeSkip),
		queue.WithHoldLock(s.lockHold),
	)

	atomic.StoreUint32(&s.processedJobs, 0)

	reg := job.NewDefaultHandlerRegistry()
	reg.Register(job.TypeNoop, func(ctx context.Context, j *job.Job) error {
		err := job.NoopHandler(ctx, j)
		if err == nil {
			count := atomic.AddUint32(&s.processedJobs, 1)
			log.Printf("[%s][Case %d] Progress: %d/%d jobs",
				s.Name(), caseID, count, s.jobsPerCase)
		}

		return err
	})

	onAttempt := func(rec worker.AttemptRecord) {
		attemptsMu.Lock()
		defer attemptsMu.Unlock()

		*allAttempts = append(*allAttempts, export.RetryAttemptRow{
			CaseID:        caseID + 1,
			StrategyType:  extractStrategyType(strategy),
			StrategyLabel: strategyLabel,
			Jitter:        strategy.GetJitter(),
			ErrorRate:     s.errorRate,
			JobID:         rec.JobID,
			WorkerName:    rec.WorkerName,
			Attempt:       rec.Attempt,
			AttemptedAt:   rec.AttemptedAt,
			Success:       rec.Success,
			RetryDelayMs:  rec.RetryDelay.Milliseconds(),
		})
	}

	manager, err := worker.NewManager(caseCtx, fmt.Sprintf("retry-%d", caseID),
		func(ctx context.Context, label string, id uint) worker.Worker {
			return worker.NewPollingWorker(ctx, label, id, q,
				worker.WithRegistry(reg),
				worker.WithBackoff(strategy),
				worker.WithMaxAttempts(s.maxAttempts),
				worker.WithInterval(retryWorkerInterval),
				worker.WithOnAttempt(onAttempt),
				worker.WithOnWorkerEvent(logger.Log),
			)
		})
	if err != nil {
		return fmt.Errorf("failed to create worker manager: %w", err)
	}

	log.Printf("[%s][Case %d] Enqueuing %d jobs with %.0f%% error rate",
		s.Name(), caseID, s.jobsPerCase, s.errorRate*percentFactor)

	if err := s.enqueueJobs(caseCtx, q, s.jobsPerCase); err != nil {
		manager.StopAll()

		return err
	}

	for range s.workerCount {
		if err := manager.StartWorker(); err != nil {
			manager.StopAll()

			return err
		}
	}

	waitErr := s.waitForAllJobsProcessed(caseCtx, dbconn)
	s.waitForFinalize(caseCtx, dbconn)

	manager.StopAll()

	if waitErr != nil && !errors.Is(waitErr, errCaseDurationReached) {
		return waitErr
	}

	attemptsMu.Lock()

	caseAttempts := 0

	for _, a := range *allAttempts {
		if a.CaseID == caseID {
			caseAttempts++
		}
	}

	attemptsMu.Unlock()

	log.Printf("[%s][Case %d] Completed: recorded %d attempts",
		s.Name(), caseID, caseAttempts)

	return nil
}

// waitForAllJobsProcessed polls until all pending and processing jobs are done or the case timeout is exceeded.
func (s *RetryStrategiesScenario) waitForAllJobsProcessed(
	ctx context.Context,
	dbconn *db.DB,
) error {
	ticker := time.NewTicker(retryTickerInterval)
	defer ticker.Stop()

	deadline := time.After(s.caseTimeout)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("%w (%s)", errCaseDurationReached, s.caseTimeout)
		case <-ticker.C:
			pending, err := dbconn.CountJobsByStatus(ctx, "pending")
			if err != nil {
				continue
			}

			processing, err := dbconn.CountJobsByStatus(ctx, "processing")
			if err != nil {
				continue
			}

			if pending == 0 && processing == 0 {
				return nil
			}
		}
	}
}

// waitForFinalize gives in-flight jobs a grace period to complete after the main wait.
func (s *RetryStrategiesScenario) waitForFinalize(ctx context.Context, dbconn *db.DB) {
	if s.finalizeTimeout <= 0 {
		return
	}

	finalizeDeadline := time.After(s.finalizeTimeout)
	finalizeTicker := time.NewTicker(retryTickerInterval)

	defer finalizeTicker.Stop()

	for {
		select {
		case <-finalizeDeadline:
			return
		case <-finalizeTicker.C:
			pending, _ := dbconn.CountJobsByStatus(ctx, "pending")

			processing, _ := dbconn.CountJobsByStatus(ctx, "processing")
			if pending == 0 && processing == 0 {
				return
			}
		}
	}
}

// enqueueJobs enqueues a specified number of jobs with the scenarios error rate.
func (s *RetryStrategiesScenario) enqueueJobs(ctx context.Context, q queue.Queue, n uint) error {
	for i := range n {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		payload := job.NewPayload(
			fmt.Sprintf("job-%d", i),
			job.WithErrorRate(s.errorRate),
		)
		if err := q.Enqueue(ctx, job.New(payload)); err != nil {
			return err
		}
	}

	return nil
}

// extractStrategyType returns the name of the backoff strategy.
func extractStrategyType(b backoff.Backoff) string {
	switch b.(type) {
	case *backoff.Constant:
		return "constant"
	case *backoff.Linear:
		return "linear"
	case *backoff.Exponential:
		return "exponential"
	default:
		return "unknown"
	}
}

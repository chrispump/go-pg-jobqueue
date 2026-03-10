package scenario

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/db"
	"github.com/chrispump/go-pg-jobqueue/internal/export"
	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
	"github.com/chrispump/go-pg-jobqueue/internal/worker"
)

const (
	dlqMaxAttempts    = 5
	dlqTotalJobs      = 100
	dlqWorkerCount    = 8
	dlqLockHold       = 800 * time.Millisecond
	dlqTimeout        = 120 * time.Second
	dlqOutageDuration = 500 * time.Millisecond
	dlqOutageStart    = 500 * time.Millisecond
	dlqOutageCount    = 5
	dlqWorkerInterval = 50 * time.Millisecond
	dlqExportTimeout  = 30 * time.Second
	dlqRecoveryWait   = 500 * time.Millisecond
)

// dlqRunCase represents a single test case configuration.
type dlqRunCase struct {
	label     string
	useOutage bool
	errorRate float32
}

// DLQConsistencyScenario tests DLQ consistency under simulated outages and error rates.
type DLQConsistencyScenario struct {
	maxAttempts    uint
	totalJobs      uint
	workerCount    uint
	lockHold       time.Duration
	timeout        time.Duration
	outageDuration time.Duration
	outageInterval time.Duration
	outageStart    time.Duration
	outageCount    uint
}

func NewDLQConsistencyScenario() Scenario {
	return &DLQConsistencyScenario{
		maxAttempts:    dlqMaxAttempts,
		totalJobs:      dlqTotalJobs,
		workerCount:    dlqWorkerCount,
		lockHold:       dlqLockHold,
		timeout:        dlqTimeout,
		outageDuration: dlqOutageDuration,
		outageInterval: 1 * time.Second,
		outageStart:    dlqOutageStart,
		outageCount:    dlqOutageCount,
	}
}

func (s DLQConsistencyScenario) Name() string {
	return "dlq_consistency"
}

func (s DLQConsistencyScenario) Describe() string {
	return "Tests DLQ consistency under simulated outages and different error rates."
}

func (s *DLQConsistencyScenario) Run(parentCtx context.Context, dbconn *db.DB) (Result, error) {
	// Create worker event logger
	logger, err := worker.NewWorkerEventLogger(fmt.Sprintf("logs/worker_%s.log", s.Name()))
	if err != nil {
		return Result{}, fmt.Errorf("failed to create worker logger: %w", err)
	}
	defer logger.Close()

	var results []export.DLQConsistencyRow

	cases := s.buildRunCases()

	for i, runCase := range cases {
		caseID := i + 1
		log.Printf("[%s][Case %d] Starting: %s", s.Name(), caseID, runCase.label)

		caseRows, err := s.runSingleCase(parentCtx, dbconn, caseID, runCase, logger)
		if err != nil {
			return Result{}, err
		}

		results = append(results, caseRows...)
	}

	resultsFile, err := export.ScenarioCSVPath(s.Name(), "results")
	if err != nil {
		return Result{}, err
	}

	if err := export.ExportDLQConsistencyToCSV(results, resultsFile); err != nil {
		return Result{}, err
	}

	log.Printf("[%s] Exported results: %s", s.Name(), resultsFile)

	return Result{FilePath: resultsFile}, nil
}

func (s *DLQConsistencyScenario) runSingleCase(
	parentCtx context.Context,
	dbconn *db.DB,
	caseID int,
	rc dlqRunCase,
	logger *worker.WorkerEventLogger,
) ([]export.DLQConsistencyRow, error) {
	caseCtx, caseCancel := context.WithCancel(parentCtx)
	defer caseCancel()

	if err := dbconn.TruncateJobsTable(caseCtx); err != nil {
		return nil, err
	}

	pgQueue := queue.NewPostgresQueue(
		dbconn.StdlibDB(),
		queue.WithLockMode(queue.PgLockModeSkip),
		queue.WithHoldLock(s.lockHold),
	)

	var (
		q           queue.Queue = pgQueue
		outageQueue *queue.OutageQueue
	)

	if rc.useOutage {
		outageQueue = queue.NewOutageQueue(pgQueue)
		q = outageQueue
	}

	reg := job.NewDefaultHandlerRegistry()
	reg.Register(job.TypeNoop, job.NoopHandler)

	manager, err := worker.NewManager(caseCtx, fmt.Sprintf("dlq-%s", rc.label),
		func(ctx context.Context, label string, id uint) worker.Worker {
			return worker.NewPollingWorker(ctx, label, id, q,
				worker.WithRegistry(reg),
				worker.WithMaxAttempts(s.maxAttempts),
				worker.WithInterval(dlqWorkerInterval),
				worker.WithOnWorkerEvent(logger.Log),
			)
		})
	if err != nil {
		return nil, fmt.Errorf("failed to create worker manager: %w", err)
	}

	log.Printf("[%s][Case %d] Enqueuing %d jobs (max_attempts=%d, error_rate=%.2f)",
		s.Name(), caseID, s.totalJobs, s.maxAttempts, rc.errorRate)

	if err := s.enqueueJobs(caseCtx, pgQueue, rc); err != nil {
		manager.StopAll()

		return nil, err
	}

	for range s.workerCount {
		if err := manager.StartWorker(); err != nil {
			manager.StopAll()

			return nil, err
		}
	}

	outageCancel := func() {}

	if outageQueue != nil {
		outageCtx, cancelOutages := context.WithCancel(caseCtx)
		outageCancel = cancelOutages

		go s.simulateOutages(outageCtx, outageQueue, caseID)
	}

	s.waitForCompletion(caseCtx, dbconn, caseID)

	outageCancel()

	manager.StopAll()

	exportCtx, exportCancel := context.WithTimeout(parentCtx, dlqExportTimeout)
	jobRows, err := dbconn.LoadJobs(exportCtx, "")

	exportCancel()

	if err != nil {
		return nil, err
	}

	var dlq queue.DeadLetterQueue = pgQueue

	failedCount, err := dlq.DLQCount(parentCtx)
	if err != nil {
		return nil, fmt.Errorf("dlq count: %w", err)
	}

	var completed, pending, processing uint

	for _, j := range jobRows {
		switch j.Status {
		case "completed":
			completed++
		case "pending":
			pending++
		case "processing":
			processing++
		}
	}

	failed := uint(failedCount)
	accountedFor := completed + failed + pending + processing
	lost := s.totalJobs - accountedFor

	log.Printf("[%s][Case %d] Results:", s.Name(), caseID)
	log.Printf("[%s][Case %d] \tCreated:    %d", s.Name(), caseID, s.totalJobs)
	log.Printf("[%s][Case %d] \tCompleted:  %d", s.Name(), caseID, completed)
	log.Printf("[%s][Case %d] \tFailed/DLQ: %d", s.Name(), caseID, failed)
	log.Printf("[%s][Case %d] \tPending:    %d", s.Name(), caseID, pending)
	log.Printf("[%s][Case %d] \tProcessing: %d", s.Name(), caseID, processing)
	log.Printf("[%s][Case %d] \tLost:       %d", s.Name(), caseID, lost)

	if lost == 0 && pending == 0 && processing == 0 {
		log.Printf("[%s][Case %d] Consistency check passed", s.Name(), caseID)
	} else {
		log.Printf("[%s][Case %d] Consistency check failed", s.Name(), caseID)
	}

	rows := make([]export.DLQConsistencyRow, 0, len(jobRows))

	for _, j := range jobRows {
		rows = append(rows, export.DLQConsistencyRow{
			CaseLabel:     rc.label,
			TotalJobs:     s.totalJobs,
			CompletedJobs: completed,
			FailedJobs:    failed,
			LostJobs:      lost,
			Job:           j,
		})
	}

	return rows, nil
}

// buildRunCases creates all test case combinations.
func (s *DLQConsistencyScenario) buildRunCases() []dlqRunCase {
	return []dlqRunCase{
		{label: "no_outage_error_rate_0", useOutage: false, errorRate: 0.0},
		{label: "no_outage_error_rate_100", useOutage: false, errorRate: 1.0},
		{label: "with_outage_error_rate_0", useOutage: true, errorRate: 0.0},
		{label: "with_outage_error_rate_100", useOutage: true, errorRate: 1.0},
	}
}

// waitForCompletion polls job counts until all jobs are done or timeout is reached.
func (s *DLQConsistencyScenario) waitForCompletion(ctx context.Context, dbconn *db.DB, caseID int) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.timeout):
			log.Printf("[%s][Case %d] Timeout reached", s.Name(), caseID)

			return
		case <-time.After(dlqRecoveryWait):
			pending, _ := dbconn.CountJobsByStatus(ctx, "pending")
			processing, _ := dbconn.CountJobsByStatus(ctx, "processing")
			completed, _ := dbconn.CountJobsByStatus(ctx, "completed")
			failed, _ := dbconn.CountJobsByStatus(ctx, "failed")

			log.Printf(
				"[%s][Case %d] Status: pending=%4d, processing=%4d, completed=%4d, failed=%4d",
				s.Name(),
				caseID,
				pending,
				processing,
				completed,
				failed,
			)

			if pending == 0 && processing == 0 {
				log.Printf("[%s][Case %d] All jobs completed", s.Name(), caseID)

				return
			}
		}
	}
}

// simulateOutages triggers queue outages at regular intervals.
func (s *DLQConsistencyScenario) simulateOutages(
	ctx context.Context,
	q *queue.OutageQueue,
	caseID int,
) {
	if s.outageCount <= 0 {
		return
	}

	if s.outageStart > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.outageStart):
		}
	}

	for i := 0; i < int(s.outageCount); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		log.Printf("[%s][Case %d] Simulating %s queue outage (%d/%d)",
			s.Name(), caseID, s.outageDuration, i+1, s.outageCount)
		q.StartOutage(s.outageDuration)

		if s.outageInterval <= 0 {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(s.outageInterval):
		}
	}
}

// enqueueJobs adds jobs to the queue.
func (s *DLQConsistencyScenario) enqueueJobs(
	ctx context.Context,
	q queue.Queue,
	runCase dlqRunCase,
) error {
	for i := range s.totalJobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		payload := job.NewPayload(
			fmt.Sprintf("job-%d", i),
			job.WithErrorRate(runCase.errorRate),
		)

		if err := q.Enqueue(ctx, job.New(payload)); err != nil {
			return err
		}
	}

	return nil
}

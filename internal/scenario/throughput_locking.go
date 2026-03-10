package scenario

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/db"
	"github.com/chrispump/go-pg-jobqueue/internal/export"
	"github.com/chrispump/go-pg-jobqueue/internal/job"
	"github.com/chrispump/go-pg-jobqueue/internal/queue"
	"github.com/chrispump/go-pg-jobqueue/internal/worker"
)

const (
	throughputTotalJobs                 = 15_000
	throughputDuration                  = 60 * time.Second
	throughputLockHold                  = 100 * time.Millisecond
	throughputMetricsInterval           = 100 * time.Millisecond
	throughputMetricsChanBuffer         = 1_000
	throughputExportTimeout             = 30 * time.Second
	throughputJobStressLevel    float32 = 0.25
)

// throughputLockingRunCase represents a single test case configuration.
type throughputLockingRunCase struct {
	lockMode    queue.PgLockMode
	workerCount uint
}

// Snapshot captures worker state at a point in time used for metrics collection.
type Snapshot struct {
	Timestamp           time.Time
	WorkerStateSnapshot worker.WorkerStateSnapshot
}

// ThroughputLockingScenario tests throughput under different lock modes.
type ThroughputLockingScenario struct {
	lockModes       []queue.PgLockMode
	workerCounts    []uint
	totalJobs       uint
	duration        time.Duration
	processedJobs   uint32
	lockHold        time.Duration
	metricsInterval time.Duration
}

func NewThroughputLockingScenario() Scenario {
	return &ThroughputLockingScenario{
		lockModes:       []queue.PgLockMode{queue.PgLockModeWait, queue.PgLockModeSkip},
		workerCounts:    []uint{1, 2, 4, 16, 32, 64},
		totalJobs:       throughputTotalJobs,
		duration:        throughputDuration,
		lockHold:        throughputLockHold,
		metricsInterval: throughputMetricsInterval,
	}
}

func (s ThroughputLockingScenario) Name() string {
	return "throughput_locking"
}

func (s ThroughputLockingScenario) Describe() string {
	return "Compares throughput under FOR UPDATE and SKIP LOCKED with varying worker counts. " +
		"Measures how lock contention affects processing rates and worker states."
}

func (s ThroughputLockingScenario) Run(parentCtx context.Context, dbconn *db.DB) (Result, error) {
	// Create worker event logger
	logger, err := worker.NewWorkerEventLogger(fmt.Sprintf("logs/worker_%s.log", s.Name()))
	if err != nil {
		return Result{}, fmt.Errorf("failed to create worker logger: %w", err)
	}
	defer logger.Close()

	var (
		results []export.ThroughputLockingRow
		metrics []export.ThroughputLockingMetricsRow
	)

	cases := s.buildRunCases()

	for i, runCase := range cases {
		caseID := uint(i + 1)
		log.Printf("[%s][Case %d] Starting: %d workers with %s",
			s.Name(), caseID, runCase.workerCount, runCase.lockMode)

		caseResults, caseMetrics, err := s.runSingleCase(parentCtx, dbconn, caseID, runCase, logger)
		if err != nil {
			return Result{}, err
		}

		results = append(results, caseResults...)
		metrics = append(metrics, caseMetrics...)
	}

	resultsFile, err := export.ScenarioCSVPath(s.Name(), "results")
	if err != nil {
		return Result{}, err
	}

	if err := export.ExportThroughputResultsToCSV(results, resultsFile); err != nil {
		return Result{}, err
	}

	log.Printf("[%s] Exported results: %s", s.Name(), resultsFile)

	metricsFile, err := export.ScenarioCSVPath(s.Name(), "metrics")
	if err != nil {
		return Result{}, err
	}

	if err := export.ExportThroughputMetricsToCSV(metrics, metricsFile); err != nil {
		return Result{}, err
	}

	log.Printf("[%s] Exported metrics: %s", s.Name(), metricsFile)

	return Result{
		FilePath:        resultsFile,
		MetricsFilePath: metricsFile,
	}, nil
}

func (s *ThroughputLockingScenario) runSingleCase(
	parentCtx context.Context,
	dbconn *db.DB,
	caseID uint,
	rc throughputLockingRunCase,
	logger *worker.WorkerEventLogger,
) ([]export.ThroughputLockingRow, []export.ThroughputLockingMetricsRow, error) {
	caseCtx, caseCancel := context.WithCancel(parentCtx)
	defer caseCancel()

	if err := dbconn.TruncateJobsTable(caseCtx); err != nil {
		return nil, nil, err
	}

	q := queue.NewPostgresQueue(
		dbconn.StdlibDB(),
		queue.WithLockMode(rc.lockMode),
		queue.WithHoldLock(s.lockHold),
	)

	atomic.StoreUint32(&s.processedJobs, 0)

	reg := job.NewDefaultHandlerRegistry()
	reg.Register(job.TypeNoop, func(ctx context.Context, j *job.Job) error {
		err := job.NoopHandler(ctx, j)
		if err == nil {
			completed := atomic.AddUint32(&s.processedJobs, 1)
			log.Printf("[%s][Case %d] Progress: %d/%d jobs",
				s.Name(), caseID, completed, s.totalJobs)
		}

		return err
	})

	manager, err := worker.NewManager(
		caseCtx,
		fmt.Sprintf("%s-%d", rc.lockMode, rc.workerCount),
		func(ctx context.Context, label string, id uint) worker.Worker {
			return worker.NewPollingWorker(ctx, label, id, q,
				worker.WithRegistry(reg),
				worker.WithOnWorkerEvent(logger.Log),
			)
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create worker manager: %w", err)
	}

	log.Printf("[%s][Case %d] Enqueuing %d jobs", s.Name(), caseID, s.totalJobs)

	if err := s.enqueueJobs(caseCtx, q, s.totalJobs); err != nil {
		manager.StopAll()

		return nil, nil, err
	}

	for range rc.workerCount {
		if err := manager.StartWorker(); err != nil {
			manager.StopAll()

			return nil, nil, err
		}
	}

	metricsCtx, metricsCancel := context.WithCancel(caseCtx)
	metricsChan := make(chan Snapshot, throughputMetricsChanBuffer)

	go s.collectMetrics(metricsCtx, manager, metricsChan)

	select {
	case <-caseCtx.Done():
		metricsCancel()
		manager.StopAll()

		return nil, nil, caseCtx.Err()
	case <-time.After(s.duration):
		log.Printf("[%s][Case %d] Test duration reached (%s), stopping workers...",
			s.Name(), caseID, s.duration)
	}

	metricsCancel()
	manager.StopAll()

	snapshots := make([]Snapshot, 0, throughputMetricsChanBuffer)
	for snap := range metricsChan {
		snapshots = append(snapshots, snap)
	}

	exportCtx, exportCancel := context.WithTimeout(parentCtx, throughputExportTimeout)
	jobs, err := dbconn.LoadJobs(exportCtx, "completed")

	exportCancel()

	if err != nil {
		return nil, nil, err
	}

	log.Printf("[%s][Case %d] Cleanup and export completed", s.Name(), caseID)

	caseResults := make([]export.ThroughputLockingRow, 0, len(jobs))

	for _, j := range jobs {
		caseResults = append(caseResults, export.ThroughputLockingRow{
			CaseID:      caseID + 1,
			WorkerCount: rc.workerCount,
			LockMode:    string(rc.lockMode),
			Job:         j,
		})
	}

	caseMetrics := make([]export.ThroughputLockingMetricsRow, 0, len(snapshots))

	for _, snap := range snapshots {
		caseMetrics = append(caseMetrics, export.ThroughputLockingMetricsRow{
			CaseID:            caseID + 1,
			WorkerCount:       rc.workerCount,
			LockMode:          string(rc.lockMode),
			Timestamp:         snap.Timestamp,
			TimestampMs:       snap.Timestamp.UnixMilli(),
			WorkersIdle:       snap.WorkerStateSnapshot.Idle,
			WorkersFetching:   snap.WorkerStateSnapshot.Fetching,
			WorkersProcessing: snap.WorkerStateSnapshot.Processing,
		})
	}

	return caseResults, caseMetrics, nil
}

// buildRunCases creates all test case combinations.
func (s *ThroughputLockingScenario) buildRunCases() []throughputLockingRunCase {
	cases := make([]throughputLockingRunCase, 0, len(s.workerCounts)*len(s.lockModes))

	for _, lockMode := range s.lockModes {
		for _, workerCount := range s.workerCounts {
			cases = append(cases, throughputLockingRunCase{
				lockMode:    lockMode,
				workerCount: workerCount,
			})
		}
	}

	return cases
}

// enqueueJobs adds n jobs to the queue.
func (s ThroughputLockingScenario) enqueueJobs(ctx context.Context, q queue.Queue, n uint) error {
	for i := range n {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		payload := job.NewPayload(
			fmt.Sprintf("job-%d", i),
			job.WithStressLevel(throughputJobStressLevel),
		)

		if err := q.Enqueue(ctx, job.New(payload)); err != nil {
			return err
		}
	}

	return nil
}

// collectMetrics captures worker state snapshots at regular intervals.
func (s *ThroughputLockingScenario) collectMetrics(
	ctx context.Context,
	manager *worker.Manager,
	snapshot chan<- Snapshot,
) {
	defer close(snapshot)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.metricsInterval):
			snapshot <- Snapshot{
				Timestamp:           time.Now(),
				WorkerStateSnapshot: manager.GetWorkerStates(),
			}
		}
	}
}

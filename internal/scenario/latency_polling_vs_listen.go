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
	latencyJobsPerCase     = 1_000
	latencyEnqueueInterval = 250 * time.Millisecond
	latencyExportTimeout   = 30 * time.Second

	workerInitDelay = 2 * time.Second
)

// latencyRunCase represents a single test case configuration.
type latencyRunCase struct {
	label    string
	mode     WorkerMode
	interval time.Duration
	lottery  bool
	winners  uint
	workers  uint
}

// WorkerMode represents the type of worker used for job processing.
type WorkerMode string

const (
	ModePolling   WorkerMode = "polling"
	ModeListening WorkerMode = "listening"
)

// LatencyPollingVsListenScenario compares polling vs listening workers.
type LatencyPollingVsListenScenario struct {
	jobsPerCase     uint
	processedJobs   uint32
	lockHold        time.Duration
	enqueueInterval time.Duration
}

func NewLatencyPollingVsListenScenario() Scenario {
	return &LatencyPollingVsListenScenario{
		jobsPerCase:     latencyJobsPerCase,
		lockHold:        0,
		enqueueInterval: latencyEnqueueInterval,
	}
}

func (s LatencyPollingVsListenScenario) Name() string {
	return "latency_polling_vs_listen"
}

func (s LatencyPollingVsListenScenario) Describe() string {
	return "Compares job processing latency between polling and listening workers. " +
		"Measures how polling intervals and lottery scheduling affect reaction time."
}

func (s LatencyPollingVsListenScenario) Run(
	parentCtx context.Context,
	dbconn *db.DB,
) (Result, error) {
	// Create worker event logger
	logger, err := worker.NewWorkerEventLogger(fmt.Sprintf("logs/worker_%s.log", s.Name()))
	if err != nil {
		return Result{}, fmt.Errorf("failed to create worker logger: %w", err)
	}
	defer logger.Close()

	var rows []export.LatencyPollingVsListenRow

	cases := s.buildRunCases()

	for i, runCase := range cases {
		caseID := uint(i + 1)
		log.Printf("[%s][Case %d] Starting: %s with %d workers",
			s.Name(), caseID, runCase.label, runCase.workers)

		caseRows, err := s.runSingleCase(parentCtx, dbconn, caseID, runCase, logger)
		if err != nil {
			return Result{}, err
		}

		rows = append(rows, caseRows...)
	}

	resultsFile, err := export.ResultsFilePath(s.Name())
	if err != nil {
		return Result{}, err
	}

	if err := export.ExportLatencyPollingVsListenToCSV(rows, resultsFile); err != nil {
		return Result{}, err
	}

	log.Printf("[%s] Exported combined results to %s", s.Name(), resultsFile)

	return Result{FilePath: resultsFile}, nil
}

func (s *LatencyPollingVsListenScenario) runSingleCase(
	parentCtx context.Context,
	dbconn *db.DB,
	caseID uint,
	rc latencyRunCase,
	logger *worker.WorkerEventLogger,
) ([]export.LatencyPollingVsListenRow, error) {
	caseCtx, caseCancel := context.WithCancel(parentCtx)
	defer caseCancel()

	if err := dbconn.TruncateJobsTable(caseCtx); err != nil {
		return nil, err
	}

	baseQueue := queue.NewPostgresQueue(
		dbconn.StdlibDB(),
		queue.WithLockMode(queue.PgLockModeSkip),
		queue.WithHoldLock(s.lockHold),
	)
	q := queue.NewCounterQueue(baseQueue)

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

	var factory worker.Factory

	switch rc.mode {
	case ModePolling:
		factory = func(ctx context.Context, label string, id uint) worker.Worker {
			return worker.NewPollingWorker(ctx, label, id, q,
				worker.WithRegistry(reg),
				worker.WithInterval(rc.interval),
				worker.WithOnWorkerEvent(logger.Log),
			)
		}
	case ModeListening:
		factory = func(ctx context.Context, label string, id uint) worker.Worker {
			w := worker.NewListeningWorker(ctx, label, id, q, dbconn.Pool,
				worker.WithRegistry(reg),
				worker.WithOnWorkerEvent(logger.Log),
			)
			if rc.lottery {
				w.UseLotteryScheduling()

				if rc.winners > 0 {
					w.SetMaxNotifyWinners(rc.winners)
				}
			}

			return w
		}
	}

	manager, err := worker.NewManager(caseCtx, rc.label, factory)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker manager: %w", err)
	}

	for range rc.workers {
		if err := manager.StartWorker(); err != nil {
			manager.StopAll()

			return nil, err
		}
	}

	time.Sleep(workerInitDelay)

	log.Printf("[%s][Case %d] Enqueuing jobs with %s interval until %d completed",
		s.Name(), caseID, s.enqueueInterval, s.jobsPerCase)

	enqueueCtx, enqueueCancel := context.WithCancel(caseCtx)
	enqueueDone := make(chan error, 1)

	go func() {
		enqueueDone <- s.enqueueJobsUntilCompleted(enqueueCtx, q, s.jobsPerCase, s.enqueueInterval)
	}()

	waitErr := <-enqueueDone

	enqueueCancel()

	manager.StopAll()

	exportCtx, exportCancel := context.WithTimeout(parentCtx, latencyExportTimeout)
	jobs, loadErr := dbconn.LoadJobs(exportCtx, "completed")

	exportCancel()

	if waitErr != nil {
		return nil, waitErr
	}

	if loadErr != nil {
		return nil, loadErr
	}

	intervalMs := int64(0)
	if rc.mode == ModePolling {
		intervalMs = rc.interval.Milliseconds()
	}

	var lottery uint

	if rc.lottery {
		lottery = 1
	}

	dequeueCalls, dequeueEmpty, _ := q.DequeueCounts()

	rows := make([]export.LatencyPollingVsListenRow, 0, len(jobs))

	exportCount := 0
	for _, j := range jobs {
		if exportCount >= int(s.jobsPerCase) {
			break
		}

		rows = append(rows, export.LatencyPollingVsListenRow{
			CaseID:            caseID,
			WorkerMode:        string(rc.mode),
			Label:             rc.label,
			PollingIntervalMs: intervalMs,
			Lottery:           lottery,
			NotifyWinners:     rc.winners,
			WorkerCount:       rc.workers,
			DequeueCalls:      dequeueCalls,
			DequeueEmpty:      dequeueEmpty,
			Job:               j,
		})
		exportCount++
	}

	log.Printf("[%s][Case %d] Completed: %d jobs collected, dequeue calls=%d, empty=%d",
		s.Name(), caseID, len(jobs), dequeueCalls, dequeueEmpty)

	return rows, nil
}

// buildRunCases creates all test case combinations.
func (s *LatencyPollingVsListenScenario) buildRunCases() []latencyRunCase {
	workerCounts := []uint{1, 4, 16}
	pollingConfigs := []struct {
		label    string
		interval time.Duration
	}{
		{"polling_50ms", 50 * time.Millisecond},
		{"polling_1000ms", 1000 * time.Millisecond},
	}
	listeningConfigs := []struct {
		label   string
		lottery bool
		winners uint
	}{
		{"listening", false, 0},
		{"listening_lottery_1win", true, 1},
		{"listening_lottery_5win", true, 5},
	}

	cases := make(
		[]latencyRunCase,
		0,
		len(workerCounts)*len(pollingConfigs)+len(workerCounts)*len(listeningConfigs),
	)

	for _, cfg := range pollingConfigs {
		for _, workers := range workerCounts {
			cases = append(cases, latencyRunCase{
				label:    cfg.label,
				mode:     ModePolling,
				interval: cfg.interval,
				workers:  workers,
			})
		}
	}

	for _, cfg := range listeningConfigs {
		for _, workers := range workerCounts {
			cases = append(cases, latencyRunCase{
				label:   cfg.label,
				mode:    ModeListening,
				lottery: cfg.lottery,
				winners: cfg.winners,
				workers: workers,
			})
		}
	}

	return cases
}

// enqueueJobsUntilCompleted enqueues jobs until the target count is processed.
func (s *LatencyPollingVsListenScenario) enqueueJobsUntilCompleted(
	ctx context.Context,
	q queue.Queue,
	targetCompleted uint,
	interval time.Duration,
) error {
	enqueuedCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Stop if enough jobs have been processed
		if atomic.LoadUint32(&s.processedJobs) >= uint32(targetCompleted) {
			return nil
		}

		// Enqueue next job
		payload := job.NewPayload(fmt.Sprintf("job-%d", enqueuedCount))
		if err := q.Enqueue(ctx, job.New(payload)); err != nil {
			return err
		}

		enqueuedCount++

		// Wait interval before next job
		if interval > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
		}
	}
}

package export

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/backoff"
	"github.com/chrispump/go-pg-jobqueue/internal/db"
)

// ScenarioCSVPath builds a consistent CSV output path for a scenario name and suffix.
// Example: ScenarioCSVPath("throughput_locking", "metrics") -> ./csv/throughput_locking_metrics.csv.
func ScenarioCSVPath(scenarioName, suffix string) (string, error) {
	if scenarioName == "" {
		return "", fmt.Errorf("scenarioName is empty")
	}

	if suffix == "" {
		return "", fmt.Errorf("suffix is empty")
	}

	return filepath.Join(".", "csv", scenarioName+"_"+suffix+".csv"), nil
}

// ResultsFilePath builds a consistent CSV output path for a scenario name.
func ResultsFilePath(scenarioName string) (string, error) {
	return ScenarioCSVPath(scenarioName, "results")
}

// ThroughputLockingRow represents a single job result for the throughput/locking scenario.
type ThroughputLockingRow struct {
	CaseID      uint
	WorkerCount uint
	LockMode    string
	Job         db.JobRow
}

// LatencyPollingVsListenRow represents a single job result for the polling vs LISTEN/NOTIFY scenario.
type LatencyPollingVsListenRow struct {
	CaseID            uint
	WorkerMode        string
	Label             string
	PollingIntervalMs int64
	Lottery           uint
	NotifyWinners     uint
	WorkerCount       uint
	DequeueCalls      uint
	DequeueEmpty      uint
	Job               db.JobRow
}

// RetryAttemptRow represents a single retry attempt for the retry strategy scenario.
type RetryAttemptRow struct {
	CaseID        uint
	StrategyType  string
	StrategyLabel string
	Jitter        backoff.Jitter
	ErrorRate     float32
	JobID         int64
	WorkerName    string
	Attempt       uint
	AttemptedAt   time.Time
	Success       bool
	RetryDelayMs  int64
}

// DLQConsistencyRow represents a single job result for the DLQ and consistency scenario.
type DLQConsistencyRow struct {
	CaseLabel     string
	TotalJobs     uint
	CompletedJobs uint
	FailedJobs    uint
	LostJobs      uint
	Job           db.JobRow
}

// ThroughputLockingMetricsRow represents worker state metrics collected during throughput tests.
type ThroughputLockingMetricsRow struct {
	CaseID            uint
	WorkerCount       uint
	LockMode          string
	Timestamp         time.Time
	TimestampMs       int64
	WorkersIdle       uint
	WorkersFetching   uint
	WorkersProcessing uint
}

// Helper functions

// formatTime formats a sql.NullTime as RFC3339Nano or empty string if NULL.
func formatTime(nt sql.NullTime) string {
	if !nt.Valid {
		return ""
	}

	return nt.Time.UTC().Format(time.RFC3339Nano)
}

// calculateLatency returns the latency in milliseconds between enqueued and dequeued times.
func calculateLatency(enqueued, dequeued sql.NullTime) string {
	if !enqueued.Valid || !dequeued.Valid {
		return ""
	}

	d := dequeued.Time.Sub(enqueued.Time)

	return fmt.Sprintf("%.3f", float64(d)/float64(time.Millisecond))
}

// exportToCSV is a generic helper that writes rows to a CSV file.
func exportToCSV[T any](
	rows []T,
	filePath string,
	header []string,
	rowConverter func(T) []string,
) error {
	if filePath == "" {
		return fmt.Errorf("filePath is empty")
	}

	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create csv file: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	for _, row := range rows {
		if err := w.Write(rowConverter(row)); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}

	w.Flush()

	if err := w.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}

	return nil
}

// Export functions

// ExportThroughputResultsToCSV writes throughput/locking scenario results to a CSV file.
func ExportThroughputResultsToCSV(rows []ThroughputLockingRow, filePath string) error {
	header := []string{
		"case_id", "worker_count", "lock_mode",
		"id", "job_type", "status",
		"created_at", "enqueued_at", "dequeued_at", "completed_at", "failed_at",
		"latency_ms",
	}

	return exportToCSV(rows, filePath, header, func(r ThroughputLockingRow) []string {
		j := r.Job

		return []string{
			strconv.FormatUint(uint64(r.CaseID), 10),
			strconv.FormatUint(uint64(r.WorkerCount), 10),
			r.LockMode,
			strconv.FormatInt(j.ID, 10),
			j.JobType,
			j.Status,
			formatTime(j.CreatedAt),
			formatTime(j.EnqueuedAt),
			formatTime(j.DequeuedAt),
			formatTime(j.CompletedAt),
			formatTime(j.FailedAt),
			calculateLatency(j.EnqueuedAt, j.DequeuedAt),
		}
	})
}

// ExportLatencyPollingVsListenToCSV writes latency comparison scenario results to a CSV file.
func ExportLatencyPollingVsListenToCSV(rows []LatencyPollingVsListenRow, filePath string) error {
	header := []string{
		"case_id", "worker_mode", "label", "polling_interval_ms",
		"lottery", "notify_winners", "worker_count",
		"dequeue_calls", "dequeue_empty",
		"id", "payload_name", "job_type", "status",
		"created_at", "enqueued_at", "dequeued_at", "completed_at", "failed_at",
		"latency_ms",
	}

	return exportToCSV(rows, filePath, header, func(r LatencyPollingVsListenRow) []string {
		j := r.Job

		return []string{
			strconv.FormatUint(uint64(r.CaseID), 10),
			r.WorkerMode,
			r.Label,
			strconv.FormatUint(uint64(r.PollingIntervalMs), 10),
			strconv.FormatUint(uint64(r.Lottery), 10),
			strconv.FormatUint(uint64(r.NotifyWinners), 10),
			strconv.FormatUint(uint64(r.WorkerCount), 10),
			strconv.FormatUint(uint64(r.DequeueCalls), 10),
			strconv.FormatUint(uint64(r.DequeueEmpty), 10),
			strconv.FormatInt(j.ID, 10),
			j.PayloadName,
			j.JobType,
			j.Status,
			formatTime(j.CreatedAt),
			formatTime(j.EnqueuedAt),
			formatTime(j.DequeuedAt),
			formatTime(j.CompletedAt),
			formatTime(j.FailedAt),
			calculateLatency(j.EnqueuedAt, j.DequeuedAt),
		}
	})
}

// ExportRetryAttemptsToCSV writes retry strategy scenario results to a CSV file.
func ExportRetryAttemptsToCSV(rows []RetryAttemptRow, filePath string) error {
	header := []string{
		"case_id", "strategy_type", "strategy_label", "jitter", "error_rate",
		"job_id", "worker_id", "attempt",
		"attempted_at", "attempted_at_ms",
		"success", "retry_delay_ms",
	}

	return exportToCSV(rows, filePath, header, func(r RetryAttemptRow) []string {
		jitterStr := "none"
		if r.Jitter != nil {
			jitterStr = r.Jitter.String()
		}

		return []string{
			strconv.FormatUint(uint64(r.CaseID), 10),
			r.StrategyType,
			r.StrategyLabel,
			jitterStr,
			fmt.Sprintf("%.2f", r.ErrorRate),
			strconv.FormatInt(r.JobID, 10),
			r.WorkerName,
			strconv.FormatUint(uint64(r.Attempt), 10),
			r.AttemptedAt.UTC().Format(time.RFC3339Nano),
			strconv.FormatInt(r.AttemptedAt.UnixMilli(), 10),
			strconv.FormatBool(r.Success),
			strconv.FormatInt(r.RetryDelayMs, 10),
		}
	})
}

// ExportDLQConsistencyToCSV writes DLQ and consistency scenario results to a CSV file.
func ExportDLQConsistencyToCSV(rows []DLQConsistencyRow, filePath string) error {
	header := []string{
		"case_label", "total_jobs", "completed_jobs", "failed_jobs", "lost_jobs",
		"job_id", "payload_name", "job_type", "status", "attempts", "last_error",
		"created_at", "enqueued_at", "dequeued_at", "completed_at", "failed_at",
	}

	return exportToCSV(rows, filePath, header, func(r DLQConsistencyRow) []string {
		j := r.Job

		return []string{
			r.CaseLabel,
			strconv.FormatUint(uint64(r.TotalJobs), 10),
			strconv.FormatUint(uint64(r.CompletedJobs), 10),
			strconv.FormatUint(uint64(r.FailedJobs), 10),
			strconv.FormatUint(uint64(r.LostJobs), 10),
			strconv.FormatInt(j.ID, 10),
			j.PayloadName,
			j.JobType,
			j.Status,
			strconv.Itoa(j.Attempts),
			j.LastError,
			formatTime(j.CreatedAt),
			formatTime(j.EnqueuedAt),
			formatTime(j.DequeuedAt),
			formatTime(j.CompletedAt),
			formatTime(j.FailedAt),
		}
	})
}

// ExportThroughputMetricsToCSV writes worker state metrics to a CSV file.
func ExportThroughputMetricsToCSV(rows []ThroughputLockingMetricsRow, filePath string) error {
	header := []string{
		"case_id", "worker_count", "lock_mode",
		"timestamp", "timestamp_ms",
		"workers_idle", "workers_fetching", "workers_processing",
	}

	return exportToCSV(rows, filePath, header, func(r ThroughputLockingMetricsRow) []string {
		return []string{
			strconv.FormatUint(uint64(r.CaseID), 10),
			strconv.FormatUint(uint64(r.WorkerCount), 10),
			r.LockMode,
			r.Timestamp.UTC().Format(time.RFC3339Nano),
			strconv.FormatInt(r.TimestampMs, 10),
			strconv.FormatUint(uint64(r.WorkersIdle), 10),
			strconv.FormatUint(uint64(r.WorkersFetching), 10),
			strconv.FormatUint(uint64(r.WorkersProcessing), 10),
		}
	})
}

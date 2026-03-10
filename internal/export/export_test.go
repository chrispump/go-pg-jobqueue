package export

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/backoff"
	"github.com/chrispump/go-pg-jobqueue/internal/db"
)

// TestScenarioCSVPath verifies that scenario CSV paths are generated correctly.
func TestScenarioCSVPath(t *testing.T) {
	tests := []struct {
		name         string
		scenarioName string
		suffix       string
		expectError  bool
		expectPath   string
	}{
		{
			name:         "valid inputs",
			scenarioName: "throughput_locking",
			suffix:       "results",
			expectError:  false,
			expectPath:   filepath.Join(".", "csv", "throughput_locking_results.csv"),
		},
		{
			name:         "empty scenario name",
			scenarioName: "",
			suffix:       "results",
			expectError:  true,
		},
		{
			name:         "empty suffix",
			scenarioName: "test",
			suffix:       "",
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := ScenarioCSVPath(tt.scenarioName, tt.suffix)
			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				if path != tt.expectPath {
					t.Errorf("expected path %q, got %q", tt.expectPath, path)
				}
			}
		})
	}
}

// TestResultsFilePath verifies that results file paths are generated correctly.
func TestResultsFilePath(t *testing.T) {
	path, err := ResultsFilePath("test_scenario")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expectedPath := filepath.Join(".", "csv", "test_scenario_results.csv")
	if path != expectedPath {
		t.Errorf("expected path %q, got %q", expectedPath, path)
	}
}

// TestExportThroughputResultsToCSV verifies throughput results export.
func TestExportThroughputResultsToCSV(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "throughput_results.csv")

	now := time.Now()
	rows := []ThroughputLockingRow{
		{
			CaseID:      1,
			WorkerCount: 4,
			LockMode:    "SKIP LOCKED",
			Job: db.JobRow{
				ID:         100,
				JobType:    "noop",
				Status:     "completed",
				EnqueuedAt: db.NewNullTime(now),
				DequeuedAt: db.NewNullTime(now.Add(10 * time.Millisecond)),
			},
		},
	}

	err := ExportThroughputResultsToCSV(rows, filePath)
	if err != nil {
		t.Fatalf("ExportThroughputResultsToCSV failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("expected file to exist at %s", filePath)
	}

	// Read and verify content
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")

	// Should have header + 1 data row
	if len(lines) != 2 {
		t.Errorf("expected 2 lines (header + 1 data), got %d", len(lines))
	}

	// Verify header contains case_id, worker_count, lock_mode
	if !strings.Contains(lines[0], "case_id") ||
		!strings.Contains(lines[0], "worker_count") ||
		!strings.Contains(lines[0], "lock_mode") {
		t.Errorf("header missing expected columns: %s", lines[0])
	}

	// Verify data contains case info
	if !strings.Contains(lines[1], "1,4,SKIP LOCKED") {
		t.Error("expected data row to contain case_id=1, worker_count=4, lock_mode=SKIP LOCKED")
	}
}

// TestExportLatencyPollingVsListenToCSV verifies latency comparison export.
func TestExportLatencyPollingVsListenToCSV(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "latency_results.csv")

	now := time.Now()
	rows := []LatencyPollingVsListenRow{
		{
			CaseID:            1,
			WorkerMode:        "listening",
			Label:             "listening_lottery_5win",
			PollingIntervalMs: 0,
			Lottery:           1,
			NotifyWinners:     5,
			WorkerCount:       16,
			DequeueCalls:      1000,
			DequeueEmpty:      500,
			Job: db.JobRow{
				ID:          200,
				JobType:     "noop",
				Status:      "completed",
				PayloadName: "test-payload",
				EnqueuedAt:  db.NewNullTime(now),
				DequeuedAt:  db.NewNullTime(now.Add(5 * time.Millisecond)),
			},
		},
	}

	err := ExportLatencyPollingVsListenToCSV(rows, filePath)
	if err != nil {
		t.Fatalf("ExportLatencyPollingVsListenToCSV failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("expected file to exist at %s", filePath)
	}

	// Read and verify content
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")

	// Should have header + 1 data row
	if len(lines) != 2 {
		t.Errorf("expected 2 lines (header + 1 data), got %d", len(lines))
	}

	// Verify header contains specific columns
	expectedColumns := []string{
		"case_id", "worker_mode", "label", "lottery",
		"notify_winners", "dequeue_calls", "dequeue_empty",
	}
	for _, col := range expectedColumns {
		if !strings.Contains(lines[0], col) {
			t.Errorf("header missing column %q", col)
		}
	}

	// Verify data contains expected values
	if !strings.Contains(lines[1], "1,listening,") {
		t.Error("expected data row to contain case_id=1 and worker_mode=listening")
	}

	if !strings.Contains(lines[1], ",1,5,16,1000,500,") {
		t.Error(
			"expected data row to contain lottery=1, notify_winners=5, worker_count=16, dequeue stats",
		)
	}
}

// TestExportRetryAttemptsToCSV verifies retry attempts export.
func TestExportRetryAttemptsToCSV(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "retry_attempts.csv")

	now := time.Now()
	rows := []RetryAttemptRow{
		{
			CaseID:        1,
			StrategyType:  "exponential",
			StrategyLabel: "exp_2x",
			Jitter:        backoff.NewFullJitter(),
			ErrorRate:     0.5,
			JobID:         300,
			WorkerName:    "worker-1",
			Attempt:       1,
			AttemptedAt:   now,
			Success:       false,
			RetryDelayMs:  1000,
		},
	}

	err := ExportRetryAttemptsToCSV(rows, filePath)
	if err != nil {
		t.Fatalf("ExportRetryAttemptsToCSV failed: %v", err)
	}

	// Verify file exists and has content
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")

	if len(lines) != 2 {
		t.Errorf("expected 2 lines, got %d", len(lines))
	}

	// Verify header
	if !strings.Contains(lines[0], "strategy_type") ||
		!strings.Contains(lines[0], "jitter") ||
		!strings.Contains(lines[0], "error_rate") {
		t.Errorf("header missing expected columns: %s", lines[0])
	}
}

// TestExportDLQConsistencyToCSV verifies DLQ consistency export.
func TestExportDLQConsistencyToCSV(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "dlq_consistency.csv")

	now := time.Now()
	rows := []DLQConsistencyRow{
		{
			CaseLabel:     "baseline",
			TotalJobs:     100,
			CompletedJobs: 90,
			FailedJobs:    8,
			LostJobs:      2,
			Job: db.JobRow{
				ID:          400,
				PayloadName: "test-payload",
				JobType:     "noop",
				Status:      "failed",
				Attempts:    5,
				LastError:   "max attempts exceeded",
				FailedAt:    db.NewNullTime(now),
			},
		},
	}

	err := ExportDLQConsistencyToCSV(rows, filePath)
	if err != nil {
		t.Fatalf("ExportDLQConsistencyToCSV failed: %v", err)
	}

	// Verify file exists and has content
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")

	if len(lines) != 2 {
		t.Errorf("expected 2 lines, got %d", len(lines))
	}

	// Verify header
	expectedCols := []string{
		"case_label",
		"total_jobs",
		"completed_jobs",
		"failed_jobs",
		"lost_jobs",
		"attempts",
		"last_error",
	}
	for _, col := range expectedCols {
		if !strings.Contains(lines[0], col) {
			t.Errorf("header missing column %q", col)
		}
	}

	// Verify data
	if !strings.Contains(lines[1], "baseline,100,90,8,2") {
		t.Error("expected data row to contain case statistics")
	}
}

// TestExportThroughputMetricsToCSV verifies worker metrics export.
func TestExportThroughputMetricsToCSV(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "throughput_metrics.csv")

	now := time.Now()
	rows := []ThroughputLockingMetricsRow{
		{
			CaseID:            1,
			WorkerCount:       16,
			LockMode:          "SKIP LOCKED",
			Timestamp:         now,
			TimestampMs:       now.UnixMilli(),
			WorkersIdle:       5,
			WorkersFetching:   3,
			WorkersProcessing: 8,
		},
	}

	err := ExportThroughputMetricsToCSV(rows, filePath)
	if err != nil {
		t.Fatalf("ExportThroughputMetricsToCSV failed: %v", err)
	}

	// Verify file exists and has content
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")

	if len(lines) != 2 {
		t.Errorf("expected 2 lines, got %d", len(lines))
	}

	// Verify header
	expectedCols := []string{"workers_idle", "workers_fetching", "workers_processing"}
	for _, col := range expectedCols {
		if !strings.Contains(lines[0], col) {
			t.Errorf("header missing column %q", col)
		}
	}

	// Verify worker state counts
	if !strings.Contains(lines[1], ",5,3,8") {
		t.Error(
			"expected data row to contain worker state counts: 5 idle, 3 fetching, 8 processing",
		)
	}
}

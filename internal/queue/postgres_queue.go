package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/job"
)

type PgLockMode string

const (
	PgLockModeWait PgLockMode = "FOR UPDATE"
	PgLockModeSkip PgLockMode = "FOR UPDATE SKIP LOCKED"
)

const defaultLeaseTime = 30 * time.Second

// Blank-identifier assertion that PostgresQueue implements the Queue and DeadLetterQueue interfaces.
// This will cause a compile-time error if PostgresQueue does not implement all required methods of these interfaces.
var (
	_ Queue           = (*PostgresQueue)(nil)
	_ DeadLetterQueue = (*PostgresQueue)(nil)
)

type PostgresQueue struct {
	db   *sql.DB
	opts *PostgresQueueOptions
}

type PostgresQueueOptions struct {
	// LeaseTime defines how long a worker holds the lease on a dequeued job before it becomes available again.
	LeaseTime time.Duration
	// LockMode defines the locking behavior when dequeuing jobs.
	LockMode PgLockMode
	// HoldLock defines an optional duration to hold the row lock during dequeueing (only for simulation/testing).
	HoldLock time.Duration
}

func WithLockMode(mode PgLockMode) func(*PostgresQueueOptions) {
	return func(opts *PostgresQueueOptions) {
		switch mode {
		case PgLockModeWait, PgLockModeSkip:
			opts.LockMode = mode
		default:
			opts.LockMode = PgLockModeSkip
		}
	}
}

func WithLeaseTime(d time.Duration) func(*PostgresQueueOptions) {
	return func(opts *PostgresQueueOptions) {
		opts.LeaseTime = d
	}
}

func WithHoldLock(d time.Duration) func(*PostgresQueueOptions) {
	return func(opts *PostgresQueueOptions) {
		opts.HoldLock = d
	}
}

// NewDefaultPostgresQueueOptions creates PostgresQueueOptions with defaults and applies any modifiers.
func NewDefaultPostgresQueueOptions(opts ...func(*PostgresQueueOptions)) *PostgresQueueOptions {
	o := &PostgresQueueOptions{
		LeaseTime: defaultLeaseTime,
		LockMode:  PgLockModeSkip,
		HoldLock:  0,
	}
	for _, opt := range opts {
		opt(o)
	}

	return o
}

// NewPostgresQueue creates a new PostgresQueue with the given database connection and options.
func NewPostgresQueue(db *sql.DB, opts ...func(*PostgresQueueOptions)) *PostgresQueue {
	return &PostgresQueue{
		db:   db,
		opts: NewDefaultPostgresQueueOptions(opts...),
	}
}

func (q *PostgresQueue) Enqueue(ctx context.Context, j job.Job) error {
	label := "enqueue"

	b, err := json.Marshal(j.Payload)
	if err != nil {
		return fmt.Errorf("%s: marshal job payload: %w", label, err)
	}

	_, err = q.useTx(ctx, label, func(tx *sql.Tx) (*job.Job, error) {
		if _, err := tx.ExecContext(ctx, "INSERT INTO jobs (payload) VALUES ($1)", b); err != nil {
			return nil, fmt.Errorf("%s: insert job: %w", label, err)
		}

		notifyPayload := fmt.Sprintf("%d", time.Now().UnixMilli())
		if _, err := tx.ExecContext(
			ctx,
			"SELECT pg_notify('job_enqueued', $1)",
			notifyPayload,
		); err != nil {
			return nil, fmt.Errorf("%s: notify: %w", label, err)
		}

		return nil, nil
	})

	return err
}

func (q *PostgresQueue) Dequeue(ctx context.Context, workerID string) (*job.Job, error) {
	label := "dequeue"
	if workerID == "" {
		return nil, fmt.Errorf("%s: workerID cannot be empty", label)
	}

	return q.useTx(ctx, label, func(tx *sql.Tx) (*job.Job, error) {
		// Build the dequeue query with the specified lock mode.
		query := fmt.Sprintf(`
			UPDATE jobs j
			SET status = $1,
				attempts = j.attempts + 1,
				lock_until = NOW() + ($3 * interval '1 millisecond'),
				locked_by = $4,
				lock_token = md5(random()::text),
				dequeued_at = COALESCE(j.dequeued_at, clock_timestamp())
			WHERE j.id = (
				SELECT id
				FROM jobs
				WHERE not_before <= NOW()
				AND (
					status = $2
					OR (
						status = $1
						AND (lock_until IS NULL OR lock_until < NOW())
					)
				)
				ORDER BY id
				LIMIT 1
				%s
			)
			RETURNING j.id, j.payload, j.status, j.attempts, j.not_before, j.lock_until, j.locked_by, j.lock_token;
		`, q.opts.LockMode)

		// Execute the dequeue query to select and lock a job for processing.
		row := tx.QueryRowContext(
			ctx,
			query,
			job.StatusProcessing,
			job.StatusPending,
			q.LeaseTime().Milliseconds(),
			workerID,
		)

		var (
			id        int64
			status    job.Status
			attempts  uint
			runAt     time.Time
			lockUntil time.Time
			lockedBy  string
			lockToken string
			payload   []byte
		)
		if err := row.Scan(
			&id,
			&payload,
			&status,
			&attempts,
			&runAt,
			&lockUntil,
			&lockedBy,
			&lockToken,
		); err != nil {
			if err == sql.ErrNoRows {
				return nil, err
			}

			return nil, fmt.Errorf("%s: scan job row: %w", label, err)
		}

		// Decode the JSON payload into a job.Payload struct.
		var jsonPayload job.Payload
		if err := json.Unmarshal(payload, &jsonPayload); err != nil {
			return nil, fmt.Errorf("%s: decode job payload: %w", label, err)
		}

		// In case HoldLock is set, keep the transaction open to hold the row lock for the specified duration.
		if q.opts.HoldLock > 0 {
			_, err := tx.ExecContext(ctx, "SELECT pg_sleep($1);", q.opts.HoldLock.Seconds())
			if err != nil {
				return nil, fmt.Errorf("%s: hold lock: %w", label, err)
			}
		}

		return job.NewFromData(
			id,
			jsonPayload,
			status,
			attempts,
			runAt,
			lockUntil,
			lockedBy,
			lockToken,
		), nil
	})
}

func (q *PostgresQueue) ExtendLease(
	ctx context.Context,
	jobId int64,
	lockToken string,
) (time.Time, error) {
	if lockToken == "" {
		return time.Time{}, fmt.Errorf("extend lease: missing lock token")
	}

	query := `
		UPDATE jobs
		SET lock_until = NOW() + ($1 * interval '1 millisecond')
		WHERE id = $2
		  AND status = $3
		  AND lock_token = $4
		RETURNING lock_until;
	`
	row := q.db.QueryRowContext(
		ctx,
		query,
		q.LeaseTime().Milliseconds(),
		jobId,
		job.StatusProcessing,
		lockToken,
	)

	var lu sql.NullTime
	if err := row.Scan(&lu); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, fmt.Errorf("extend lease: lock lost")
		}

		return time.Time{}, fmt.Errorf("extend lease: %w", err)
	}

	if !lu.Valid {
		return time.Time{}, nil
	}

	return lu.Time, nil
}

func (q *PostgresQueue) LeaseTime() time.Duration {
	return q.opts.LeaseTime
}

func (q *PostgresQueue) ScheduleRetry(
	ctx context.Context,
	id int64,
	lockToken string,
	delay time.Duration,
	lastErr string,
) error {
	label := "schedule retry"
	if lockToken == "" {
		return fmt.Errorf("%s: missing lock token", label)
	}

	notBefore := time.Now().Add(delay)

	_, err := q.useTx(ctx, label, func(tx *sql.Tx) (*job.Job, error) {
		query := `
			UPDATE jobs
			SET status = $1,
				not_before = $2,
				last_error = $3,
				lock_until = NULL,
				locked_by = NULL,
				lock_token = NULL,
				enqueued_at = clock_timestamp(),
				dequeued_at = NULL,
				completed_at = NULL,
				failed_at = NULL
			WHERE id = $4 AND status = $5 AND lock_token = $6;
		`

		res, err := tx.ExecContext(
			ctx,
			query,
			job.StatusPending,
			notBefore,
			lastErr,
			id,
			job.StatusProcessing,
			lockToken,
		)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", label, err)
		}

		if rows, _ := res.RowsAffected(); rows == 0 {
			return nil, fmt.Errorf("%s: lock lost", label)
		}

		// Notify listening workers about the rescheduled job
		notifyPayload := fmt.Sprintf("%d", notBefore.UnixMilli())
		if _, err := tx.ExecContext(
			ctx,
			"SELECT pg_notify('job_enqueued', $1)",
			notifyPayload,
		); err != nil {
			return nil, fmt.Errorf("%s: notify: %w", label, err)
		}

		return nil, nil
	})

	return err
}

func (q *PostgresQueue) Complete(ctx context.Context, jobId int64, lockToken string) error {
	label := "complete job"
	if lockToken == "" {
		return fmt.Errorf("%s: missing lock token", label)
	}

	_, err := q.useTx(ctx, label, func(tx *sql.Tx) (*job.Job, error) {
		query := `
			UPDATE jobs
			SET status = $1, lock_until = NULL, locked_by = NULL, lock_token = NULL, completed_at = clock_timestamp()
			WHERE id = $2 AND status = $3 AND lock_token = $4;
		`

		res, err := tx.ExecContext(
			ctx,
			query,
			job.StatusCompleted,
			jobId,
			job.StatusProcessing,
			lockToken,
		)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", label, err)
		}

		if rows, _ := res.RowsAffected(); rows == 0 {
			return nil, fmt.Errorf("%s: lock lost", label)
		}

		return nil, nil
	})

	return err
}

func (q *PostgresQueue) Fail(
	ctx context.Context,
	jobId int64,
	lockToken string,
	lastError string,
) error {
	label := "fail job"
	if lockToken == "" {
		return fmt.Errorf("%s: missing lock token", label)
	}

	_, err := q.useTx(ctx, label, func(tx *sql.Tx) (*job.Job, error) {
		query := `
			UPDATE jobs
			SET status = $1,
				lock_until = NULL,
				locked_by = NULL,
				lock_token = NULL,
				failed_at = clock_timestamp(),
				last_error = $2
			WHERE id = $3 AND status = $4 AND lock_token = $5;
		`

		res, err := tx.ExecContext(
			ctx,
			query,
			job.StatusFailed,
			lastError,
			jobId,
			job.StatusProcessing,
			lockToken,
		)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", label, err)
		}

		if rows, _ := res.RowsAffected(); rows == 0 {
			return nil, fmt.Errorf("%s: lock lost", label)
		}

		return nil, nil
	})

	return err
}

func (q *PostgresQueue) Count(ctx context.Context) (int64, error) {
	query := `SELECT COUNT(*) FROM jobs;`
	row := q.db.QueryRowContext(ctx, query)

	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("count all jobs: %w", err)
	}

	return count, nil
}

func (q *PostgresQueue) List(ctx context.Context, limit, offset int) ([]job.Job, error) {
	label := "list jobs"
	query := `
		SELECT id, payload, status, attempts, not_before, lock_until, locked_by, lock_token
		FROM jobs
		ORDER BY id
		LIMIT $1
		OFFSET $2;
	`

	rows, err := q.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	defer rows.Close()

	var jobs []job.Job

	for rows.Next() {
		var (
			id        int64
			status    job.Status
			attempts  uint
			runAt     time.Time
			lockUntil sql.NullTime
			lockedBy  sql.NullString
			lockTok   sql.NullString
			payload   []byte
		)

		if err := rows.Scan(
			&id,
			&payload,
			&status,
			&attempts,
			&runAt,
			&lockUntil,
			&lockedBy,
			&lockTok,
		); err != nil {
			return nil, fmt.Errorf("%s: scan row: %w", label, err)
		}

		var jsonPayload job.Payload
		if err := json.Unmarshal(payload, &jsonPayload); err != nil {
			return nil, fmt.Errorf("%s: decode payload: %w", label, err)
		}

		var lockUntilTime time.Time
		if lockUntil.Valid {
			lockUntilTime = lockUntil.Time
		}

		owner := ""
		if lockedBy.Valid {
			owner = lockedBy.String
		}

		tok := ""
		if lockTok.Valid {
			tok = lockTok.String
		}

		jobs = append(
			jobs,
			*job.NewFromData(id, jsonPayload, status, attempts, runAt, lockUntilTime, owner, tok),
		)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate rows: %w", label, err)
	}

	return jobs, nil
}

func (q *PostgresQueue) NextDue(ctx context.Context) (time.Time, error) {
	label := "next due"
	query := `
		SELECT LEAST(
			(SELECT MIN(not_before) FROM jobs WHERE status = $1),
			(SELECT MIN(lock_until) FROM jobs WHERE status = $2 AND lock_until IS NOT NULL)
		);
	`
	row := q.db.QueryRowContext(ctx, query, job.StatusPending, job.StatusProcessing)

	var due sql.NullTime
	if err := row.Scan(&due); err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", label, err)
	}

	if !due.Valid {
		return time.Time{}, nil
	}

	return due.Time, nil
}

func (q *PostgresQueue) DLQCount(ctx context.Context) (int64, error) {
	label := "dlq count"
	query := `SELECT COUNT(*) FROM jobs WHERE status = $1;`
	row := q.db.QueryRowContext(ctx, query, job.StatusFailed)

	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("%s: %w", label, err)
	}

	return count, nil
}

func (q *PostgresQueue) DLQList(
	ctx context.Context,
	limit, offset int,
) ([]DeadLetterQueueEntry, error) {
	label := "dlq list"
	query := `
		SELECT id AS job_id, failed_at, last_error AS reason
		FROM jobs
		WHERE status = $3
		ORDER BY id ASC
		LIMIT $1
		OFFSET $2;
	`

	rows, err := q.db.QueryContext(ctx, query, limit, offset, job.StatusFailed)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	defer rows.Close()

	var entries []DeadLetterQueueEntry

	for rows.Next() {
		var e DeadLetterQueueEntry
		if err := rows.Scan(&e.JobID, &e.FailedAt, &e.Reason); err != nil {
			return nil, fmt.Errorf("%s: scan row: %w", label, err)
		}

		entries = append(entries, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate rows: %w", label, err)
	}

	return entries, nil
}

// useTx is a helper method to execute a function within a database transaction.
// It ensures proper commit/rollback and error handling for transactional operations.
func (q *PostgresQueue) useTx(
	ctx context.Context,
	label string,
	fn func(*sql.Tx) (*job.Job, error),
) (*job.Job, error) {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: begin transaction: %w", label, err)
	}

	defer func() { _ = tx.Rollback() }()

	job, err := fn(tx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("%s: commit: %w", label, err)
	}

	return job, nil
}

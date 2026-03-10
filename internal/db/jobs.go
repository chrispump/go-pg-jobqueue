package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type JobRow struct {
	ID          int64
	PayloadName string
	JobType     string
	Status      string
	Attempts    int
	LastError   string
	CreatedAt   sql.NullTime
	EnqueuedAt  sql.NullTime
	DequeuedAt  sql.NullTime
	CompletedAt sql.NullTime
	FailedAt    sql.NullTime
}

// NewNullTime creates a valid sql.NullTime from a time.Time value.
func NewNullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: true}
}

// LoadJobs loads jobs from the database, optionally filtered by status.
// If status is empty, all jobs are returned.
func (db *DB) LoadJobs(ctx context.Context, status string) ([]JobRow, error) {
	whereClause := ""
	if status != "" {
		whereClause = fmt.Sprintf("WHERE status = '%s'", status)
	}

	query := fmt.Sprintf(`
		SELECT
			id,
			COALESCE(payload->>'data', '') AS payload_name,
			payload->>'type' AS job_type,
			status,
			attempts,
			COALESCE(last_error, '') AS last_error,
			created_at,
			enqueued_at,
			dequeued_at,
			completed_at,
			failed_at
		FROM jobs
		%s
		ORDER BY created_at ASC
	`, whereClause)

	rows, err := db.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	var out []JobRow

	for rows.Next() {
		var j JobRow
		if err := rows.Scan(
			&j.ID,
			&j.PayloadName,
			&j.JobType,
			&j.Status,
			&j.Attempts,
			&j.LastError,
			&j.CreatedAt,
			&j.EnqueuedAt,
			&j.DequeuedAt,
			&j.CompletedAt,
			&j.FailedAt,
		); err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}

		out = append(out, j)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job rows: %w", err)
	}

	return out, nil
}

// CountJobsByStatus counts jobs with the specified status.
func (db *DB) CountJobsByStatus(ctx context.Context, status string) (int, error) {
	var count int

	err := db.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status = $1`, status).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count jobs by status: %w", err)
	}

	return count, nil
}

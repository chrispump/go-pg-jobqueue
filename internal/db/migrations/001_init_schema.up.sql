-- 001_init_schema.up.sql
CREATE TABLE jobs (
    -- A unique identifier for each job.
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The jobs data as  flexible JSONB format.
    payload JSONB NOT NULL,
    -- The current status of the job.
    status TEXT NOT NULL DEFAULT 'pending',
    -- The number of times this job has been attempted.
    attempts INTEGER NOT NULL DEFAULT 0,
    -- The timestamp after which this job can be processed. Used for scheduling and retries.
    not_before TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Lease time for jobs in processing.
    lock_until TIMESTAMPTZ,
    -- Lease ownership information
    locked_by TEXT,
    -- Lock token used to validate ownership of the lock
    lock_token TEXT,
    -- Creation timestamp of the job (when it was inserted into the database)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Enqueue timestamp of the job (when it becomes pending and available for processing)
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Dequeue timestamp of the job (when a worker starts processing it)
    dequeued_at TIMESTAMPTZ,
    -- Completion timestamp of the job (when processing finishes successfully)
    completed_at TIMESTAMPTZ,
    -- Failure timestamp of the job (when processing finishes with failure after all retries)
    failed_at TIMESTAMPTZ,
    -- The last error message from a failed processing attempt
    last_error TEXT
);

CREATE INDEX idx_jobs_on_status_and_not_before ON jobs (status, not_before);
CREATE INDEX idx_jobs_on_status_and_lock_until ON jobs (status, lock_until);
CREATE INDEX idx_jobs_on_lock_token ON jobs (lock_token);
-- 001_init_schema.down.sql
DROP INDEX IF EXISTS idx_jobs_on_lock_token;
DROP INDEX IF EXISTS idx_jobs_on_status_and_lock_until;
DROP INDEX IF EXISTS idx_jobs_on_status_and_not_before;

DROP TABLE IF EXISTS jobs;
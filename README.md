# Fault-Tolerant Job Queue with Go and PostgreSQL

[![CI](https://github.com/chrispump/go-pg-jobqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/chrispump/go-pg-jobqueue/actions/workflows/ci.yml)

A proof-of-concept implementation and benchmark suite developed as part of a Bachelor's thesis at [Hochschule Flensburg](https://www.hs-flensburg.de), investigating whether PostgreSQL and Go can serve as a reliable foundation for fault-tolerant job queue systems.

## Research Question

> How do different implementations of fault-tolerant job queues with Go and PostgreSQL compare in terms of throughput, latency, and consistency under failure conditions?

The thesis evaluates four strategies across four benchmark scenarios:

| Strategy          | Variants                                                                             |
| ----------------- | ------------------------------------------------------------------------------------ |
| **Locking**       | `FOR UPDATE` vs. `FOR UPDATE SKIP LOCKED`                                            |
| **Job retrieval** | Polling (50 ms / 1000 ms) vs. `LISTEN/NOTIFY` (with optional probabilistic variants) |
| **Retry**         | Constant, Linear, Exponential Backoff with No Jitter, Full Jitter, Equal Jitter      |
| **Consistency**   | Dead-Letter Queue under simulated outages and error injection                        |

## Pre-recorded Results

The `csv/` directory contains benchmark results recorded on a **Raspberry Pi 4B** (Quad-core ARM Cortex-A72 @ 1.5 GHz, 8 GB RAM). The constrained hardware intentionally amplifies the effects of lock contention and scaling limits, making differences between strategies clearly measurable without large-scale load tests.

| File                                        | Scenario                                                              |
| ------------------------------------------- | --------------------------------------------------------------------- |
| `csv/throughput_locking_results.csv`        | Jobs/s vs. worker count for both locking strategies                   |
| `csv/throughput_locking_metrics.csv`        | Worker state snapshots (idle/fetching/processing) over time           |
| `csv/latency_polling_vs_listen_results.csv` | Dequeue latency and idle query counts per retrieval variant           |
| `csv/retry_strategies_results.csv`          | Timing records for all 9 retry strategy combinations per attempt      |
| `csv/dlq_consistency_results.csv`           | Job counts (completed / DLQ) per test case, consistency check results |

## Running the Benchmarks

> **Note:** Running the benchmarks will overwrite the pre-recorded CSV files. To preserve the original results, back up the `csv/` directory first.

### With Docker (recommended)

```bash
# Run all scenarios (includes PostgreSQL)
docker compose run --rm jqcli

# Run a single scenario
docker compose run --rm jqcli -scenario throughput_locking
```

Available scenario names: `throughput_locking`, `latency_polling_vs_listen`, `retry_strategies`, `dlq_consistency`

### Locally

```bash
# Start PostgreSQL
docker compose up -d postgres

# Run all scenarios
go run -mod=vendor ./cmd/jqcli

# Run a single scenario
go run -mod=vendor ./cmd/jqcli -scenario retry_strategies
```

CSV results are written to `./csv/`, worker event logs to `./logs/`.

### Configuration

Database connection is configured via `config.yml`:

```yaml
database:
  hosts:
    - postgres # docker-compose service name
    - localhost # local fallback
  port: 5432
  user: postgres
  password: postgres
  name: go_pg_queue
  sslmode: disable
```

## Analysing Results

Open `notebook.ipynb` to reproduce all figures from the thesis:

```bash
jupyter notebook notebook.ipynb
```

The notebook loads the CSV files from `./csv/` and produces:

- Throughput vs. worker count charts (FOR UPDATE vs. SKIP LOCKED)
- Dequeue latency box plots and scatter plots (Polling vs. LISTEN/NOTIFY variants)
- Empty dequeue counts per retrieval strategy (idle database load)
- Retry timing scatter and box plots showing the effect of jitter
- DLQ consistency summary table

## How It Works

1. **Scenario** wires all components together for a specific test case and creates the queue (optionally wrapped with `CounterQueue` or `OutageQueue` decorators), configures the worker factory with a specific retry strategy and worker implementation, enqueues jobs in a background goroutine, and starts N workers via the **Manager**.

2. **Workers** (`PollingWorker` / `ListeningWorker`) dequeue jobs atomically using `SELECT … FOR UPDATE [SKIP LOCKED] … RETURNING`. On success they call `Complete`. On failure they consult **Backoff + Jitter** for a delay and call `ScheduleRetry` (or `Fail` after max attempts, moving the job to the DLQ).

3. **Lease / Heartbeat**: every dequeued job receives a `lock_token`. A background goroutine periodically extends the lease via `ExtendLease` while the job is being processed. If a worker crashes, the lease expires and another worker reclaims the job providing the at-least-once delivery guarantee.

4. **DLQ**: jobs that exeed the max retry attempts are set to status `failed` within the same `jobs` table and excluded from future dequeue queries. No data is ever deleted.

## Academic Context

This repository accompanies the Bachelor's thesis:

> **Untersuchung transaktionaler Ansätze für fehlertolerante Job-Queues mit Go und PostgreSQL**
> Christopher Pump — Hochschule Flensburg, 2026
> Supervisor: Prof. Dr. Niklas Klein · Second reviewer: Prof. Dr. Simon Olberding

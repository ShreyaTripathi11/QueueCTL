# queuectl

A CLI-based background job queue system with worker processes, automatic
retries with exponential backoff, and a Dead Letter Queue (DLQ) for
permanently failed jobs.

Built with **plain Node.js — zero external dependencies.** No `npm install`
required; only Node's built-in modules (`fs`, `child_process`, etc.) are used.

## Demo

_(Add your recorded CLI demo link here before submission, e.g. a Google Drive link.)_

## 1. Setup Instructions

**Requirements:** Node.js >= 16

```bash
git clone <your-repo-url>
cd queuectl

# No install step needed - zero dependencies!

# Make the CLI available as `queuectl` on your PATH (optional):
npm link
# or just invoke it directly:
node bin/queuectl.js help
```

All job/config/worker state is stored in `data/state.json`, created
automatically on first use. Worker logs are written to `data/logs/`.

## 2. Usage Examples

```bash
$ queuectl enqueue '{"id":"job1","command":"echo Hello World"}'
Enqueued job "job1" (state: pending, max_retries: 3)

$ queuectl enqueue '{"id":"job2","command":"exit 1","max_retries":2}'
Enqueued job "job2" (state: pending, max_retries: 2)

$ queuectl worker start --count 3
Started worker "worker-...-0-a1b2" (pid 4821) - logs: data/logs/worker-...-0-a1b2.log
Started worker "worker-...-1-c3d4" (pid 4822) - logs: data/logs/worker-...-1-c3d4.log
Started worker "worker-...-2-e5f6" (pid 4823) - logs: data/logs/worker-...-2-e5f6.log

$ queuectl status
Job counts:
  pending      0
  processing   0
  completed    1
  failed       0
  dead         1
Total jobs:     2
Active workers: 3
Workers:
  worker-...-0-a1b2 (pid 4821) - running
  worker-...-1-c3d4 (pid 4822) - running
  worker-...-2-e5f6 (pid 4823) - running
Config: {"backoff_base":2,"max_retries":3}

$ queuectl list --state pending
No jobs found.

$ queuectl dlq list
job2	attempts=2	last_error=Command failed: exit 1

$ queuectl dlq retry job2
Job "job2" re-queued from DLQ.

$ queuectl config set max-retries 5
Config updated: max_retries = 5

$ queuectl config set backoff-base 3
Config updated: backoff_base = 3

$ queuectl worker stop
Sent SIGTERM to worker "worker-...-0-a1b2" (pid 4821)
Sent SIGTERM to worker "worker-...-1-c3d4" (pid 4822)
Sent SIGTERM to worker "worker-...-2-e5f6" (pid 4823)
Workers will finish their current job before exiting.
```

### All commands

| Category | Command | Description |
|---|---|---|
| Enqueue | `queuectl enqueue '{"id":"job1","command":"sleep 2"}'` | Add a new job to the queue |
| Workers | `queuectl worker start --count 3` | Start N worker processes (default 1) |
| Workers | `queuectl worker stop` | Gracefully stop all running workers |
| Status | `queuectl status` | Show job state counts & active workers |
| List | `queuectl list [--state <state>]` | List jobs, optionally filtered |
| DLQ | `queuectl dlq list` | List permanently failed jobs |
| DLQ | `queuectl dlq retry <job_id>` | Re-queue a job from the DLQ |
| Config | `queuectl config set max-retries 3` | Set default max retries |
| Config | `queuectl config set backoff-base 2` | Set exponential backoff base |
| Config | `queuectl config get` | Show current configuration |

You can also pass `max_retries` and a delayed start (`run_at`, ISO 8601) per
job at enqueue time, e.g.:

```bash
queuectl enqueue '{"id":"job3","command":"echo later","run_at":"2026-07-18T00:00:00Z"}'
```

## 3. Architecture Overview

### Job lifecycle

```
pending ──► processing ──► completed
   ▲             │
   │             ▼
 failed ◄────────┘ (retry, if attempts < max_retries)
   │
   ▼
 dead (DLQ, once attempts >= max_retries)
```

- `pending` / `failed` jobs are eligible for pickup once `next_run_at <= now`.
- A worker atomically claims a job (`pending`/`failed` → `processing`).
- On success → `completed`.
- On failure: `attempts += 1`. If `attempts >= max_retries` → `dead` (DLQ).
  Otherwise → `failed`, with `next_run_at = now + backoff_base^attempts` seconds.

### Data persistence

All state (jobs, config, worker registry) lives in a single JSON file at
`data/state.json`. Writes are atomic: a `state.json.tmp-<pid>` file is
written first and then `fs.renameSync`'d over the real file, so a crash
mid-write can never leave a half-written file behind.

I considered SQLite for stronger transactional guarantees, but for a system
with a modest number of jobs, a lock-protected JSON file is simpler, has
zero dependencies, and is trivially inspectable/editable by hand — which
matters for a CLI tool people will want to debug.

### Concurrency & locking

Multiple worker **processes** (not just threads) need to claim jobs without
racing each other. `queuectl` implements a cross-process mutex using
`fs.mkdirSync()`, which is atomic on POSIX filesystems (it throws `EEXIST`
if the directory already exists). Every read-modify-write of `state.json`
(claiming a job, finishing a job, enqueueing, config changes) happens inside
this lock:

```
acquire lock (mkdir .lock, retry with jitter until success or timeout)
  read state.json
  mutate in memory (e.g. atomically flip a job from pending -> processing)
  write state.json (atomic rename)
release lock (rmdir .lock)
```

This guarantees exactly one worker can transition a given job out of
`pending`/`failed`, which is what prevents duplicate execution. A stale lock
(e.g. from a process killed with `-9` mid-transaction) is force-broken after
15 seconds so the system can self-heal.

### Worker processes

`queuectl worker start --count N` spawns N independent, **detached** OS
processes (`src/worker.js`), each running its own poll loop:

1. Try to claim the oldest eligible job (under the lock).
2. If none, sleep briefly (default 500ms) and retry.
3. If claimed, run `command` via `child_process.exec`, using the exit code
   to decide success/failure.
4. Record the outcome (`finishJob`) and loop.

Workers register themselves in `state.workers` on start and update their
`status` on stop, so `queuectl status` / `worker stop` can see and signal
them even though they're separate OS processes started from a previous CLI
invocation.

### Graceful shutdown

`queuectl worker stop` sends `SIGTERM` to every worker PID recorded in
`state.workers`. Each worker's `SIGTERM` handler just sets a `stopping`
flag — it does **not** kill the in-flight command. The worker finishes its
current job, persists the result, marks itself `stopped`, and only then
exits. If a worker is idle when signaled, it exits on its next poll tick.

## 4. Assumptions & Trade-offs

- **JSON file store over SQLite/Redis**: chosen for zero dependencies and
  easy inspection. Trade-off: all reads/writes serialize through a single
  file lock, which would become a bottleneck at very high job throughput.
  For the scale implied by this assignment (a CLI tool, not a
  high-throughput broker) this is a reasonable trade.
- **Polling over pub/sub**: workers poll every 500ms rather than being
  pushed jobs. Simpler and requires no extra broker process, at the cost of
  up to ~500ms latency before a job is picked up.
- **`child_process.exec` with a shell**: commands are run via the system
  shell (e.g. `sleep 2`, `exit 1` work as shell built-ins), matching the
  examples in the spec. This means `command` strings are trusted input —
  there's no sandboxing, since this is a queue for jobs *you* enqueue, not
  a multi-tenant service.
- **No job timeout by default** in the base implementation (see Bonus
  section below for the note on this) — a hung command will hold a worker
  indefinitely. In a real production system I'd add a `timeout` field.
- **`max_retries` is per-job**, defaulting to the global `config.max_retries`
  at enqueue time if not specified. `backoff_base` is a single global config
  value applied to all jobs' backoff calculations.
- **Worker IDs** are generated (`worker-<timestamp>-<index>-<random>`)
  rather than user-supplied, since the spec's `worker start --count N`
  doesn't ask for named workers.
- **Detached processes**: workers are spawned detached so `worker start`
  returns immediately and workers keep running after the CLI process exits
  (this is what lets `status`/`stop` be invoked later as separate commands).

## 5. Testing Instructions

An automated end-to-end test script is included at `test/run_tests.sh`. It
runs against an **isolated** data directory (`test/.tmp-data`, via the
`QUEUECTL_DATA_DIR` env var) so it never touches your real queue, and cleans
up after itself (including killing any worker processes it started).

```bash
npm test
# or directly:
bash test/run_tests.sh
```

It covers all five scenarios from the assignment:

1. A basic job (`echo hi`) completes successfully.
2. A failing job (`exit 1`, `max_retries: 2`) retries with backoff and ends
   up in the DLQ with the correct attempt count.
3. 10 jobs across 4 concurrent worker processes all complete with **zero**
   duplicate executions (verified by grepping worker logs for how many
   times each job ID was actually run).
4. An invalid/nonexistent command fails gracefully — moves to the DLQ
   without crashing the worker or the CLI.
5. Job data persists across process restarts (state is re-read fresh from
   `state.json` on every CLI invocation, simulating a restart).

You can also drive it manually — see the Usage Examples above.

## Bonus features implemented

- ✅ **Scheduled/delayed jobs** via an optional `run_at` (ISO 8601) field on
  enqueue — the job won't be picked up until that time.
- ✅ **Job output logging** — each worker writes its own log file to
  `data/logs/<worker-id>.log`, including per-job command output/errors.
- ✅ **Basic execution stats** via `queuectl status` (counts per state,
  active worker count).

Not implemented (noted as out of scope for this submission): job priority
queues, per-job timeout enforcement, and a web dashboard.

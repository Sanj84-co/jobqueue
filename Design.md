# jobqueue: Design (v1)

## Proposed change

Producers put tasks into a job queue, and a worker takes them out and runs them. In v1, there is only one worker, and jobs are kept in memory. Producers can move on to other tasks while the worker runs their jobs in the background. Each job gets up to three attempts, so temporary problems like a network blip can be resolved without showing up as a failure for the user.

## Goals and non-goals

### Goals (v1)
- A producer can submit a task to the queue; it starts as **pending**.
- The worker takes jobs from the queue one at a time and marks each one **running**.
- When the work succeeds, the job is marked **done**.
- When the work fails, it is retried; after 3 attempts it is permanently marked **failed**.
- Every job's final state is delivered to its producer. No producer is ever left waiting forever.

### Non-goals (v1)
- Multiple concurrent workers
- Persistence (Postgres); crash recovery
- Scheduled or delayed jobs; priority
- A network API for submitting jobs. v1 only accepts jobs from within the same process.

## High-level architecture

### Actors
- **Producer:** calls `Submit` (same process in v1).
- **Queue:** a buffered FIFO channel (capacity 100) of pending jobs.
- **Worker:** one goroutine, started by `NewQueue`, that takes jobs in order and runs them.
- **Storage:** in memory; a map of ID → job, protected by a mutex. Lost when the program exits.
- **Admin tooling:** none in v1.

### Deployment
A single program (one binary).

### Data flow
```
Producer ──Submit(payload)──▶ Queue (channel) ──▶ Worker ──▶ handler(payload)
   ▲                                                │
   │                                                ├─ updates status in job map (mutex)
   └──── result channel (buffer 1) ◀────────────────┘
         + Get(id) lookup at any time
```

## Interfaces

- `NewQueue(handler func(string) error) *Queue`: creates the queue and starts its single worker.
- `Submit(payload string) (int, <-chan *Job, error)`: returns the job ID and a receive-only result channel; errors if the payload is empty, the queue is closed, or the queue is full.
- `Get(id int) (*Job, bool)`: looks up a job by ID; `false` if not found.
- `Close()`: stops accepting jobs, waits for the worker to finish pending jobs, then returns. Safe to call more than once.

The **handler** is the function that does the actual work for a job. The queue never knows what a payload means; the user of the queue supplies the handler. Return `nil` for success or an error for failure.

## Key components

### Job
Fields: `ID`, `Payload`, `Status`, `Attempts`, `Error`, `Result` (a per-job channel with buffer 1).

`Status` is its own type with constants (`Pending`, `Running`, `Done`, `Failed`), so a typo is a compile error instead of a silent bug.

### Queue
- **Enqueue:** `Submit` validates the payload, then, under the lock, checks the queue isn't closed, assigns the next ID, stores the job in the map, and does a **non-blocking send** into the queue channel. If the buffer is full, the job is removed from the map and `Submit` returns an error.
- **Claim:** the worker receives the next job from the channel. Each value sent on a channel is received by exactly one receiver, so a job can never be claimed twice.
- **Delivery guarantee:** none yet. Jobs are lost if the program crashes. v3 aims for at-least-once delivery.

### Worker
- One goroutine, started inside `NewQueue`, looping over the queue channel with `range`.
- For each job: up to 3 attempts. Each attempt marks it running and increments `Attempts` (under the lock), then calls the handler **outside** the lock.
- Success: mark **done**, stop retrying. Failure: record `err.Error()`; retry while attempts < 3; otherwise mark **failed**.
- After the final state is set, the job is sent on its result channel **exactly once**.
- Shutdown: when the queue channel is closed, `range` drains the remaining jobs, then the loop ends and the worker closes `done`.

### Retry and failure handling
- Max 3 attempts, retried immediately (no backoff).
- `Attempts` is incremented when an attempt **starts**, so attempts that crash still count, and a poison job can never retry forever.
- `failed` is terminal; in v1 it plays the role of a dead-letter state.

### Persistence
Deferred to v3. In v1, all jobs live in memory and are lost on exit.

## Job lifecycle

```
pending → running            worker takes the job (attempts + 1)
running → running            attempt failed, attempts < 3: retry immediately
running → done               attempt succeeded
running → failed             attempt failed, attempts = 3
```

`done` and `failed` are final. The result is sent on the job's result channel exactly once, after the final state is set.

1. **Submission:** the producer calls `Submit` with a payload and gets back a job ID and a result channel; the job starts as pending.
2. **Enqueue:** the job is placed in the in-memory queue channel; it is lost if the program exits.
3. **Claim:** the worker receives the next job from the channel.
4. **Execution:** the worker marks the job running, increments `Attempts`, and calls the handler.
5. **Completion:** on success, the job is marked done and sent through its result channel.
6. **Failure path:** on failure with attempts below 3, it is retried immediately; at 3 attempts it is marked failed and sent through its result channel.

## Concurrency and locking

- The **mutex** protects the job map, the ID counter, the `closed` flag, and job status, attempt, and error changes.
- The **queue channel** handles its own synchronization.
- Rules followed:
  - Unlock before every early return.
  - Never hold the lock across anything that can wait (a blocking send, the handler, waiting on `done`).
  - `Get` uses `defer` to unlock; `Submit` and `Close` unlock manually because they must release the lock partway through.
- The worker reads `job.Attempts` and `job.Status` without the lock; this is safe because each job is handled by only one worker, which is its only writer.

## Failure modes

- **Process crash:** all jobs are lost. Accepted for v1; v3 adds persistence.
- **Poison job:** stopped by the 3-attempt limit.
- **Full queue:** `Submit` rejects with an error; the producer decides what to do.
- **Submit after Close:** rejected with an error instead of a panic from sending on a closed channel.
- **Close called twice:** the second call returns immediately.

## Key decisions

- **In-memory storage and a single worker:** build and test the core state logic before adding concurrency between workers and a database.
- **Push results through a per-job channel (buffer 1):** decoupling, so the worker and producer don't depend on each other's speed. A slow producer can never block the worker, and the buffer means the worker never waits to deliver a result.
- **Map plus channel:** the map for lookup by ID, the channel for FIFO order, both pointing at the same `*Job`.
- **Retry inside the worker's loop (not by re-queueing):** re-queueing would have the worker send into the queue that only it reads; if the buffer were full, the worker would block waiting for itself, a deadlock.
- **`NewQueue` starts the worker:** `Close` can never wait on a worker that doesn't exist, and the worker can't be started twice. This removes two classes of bugs instead of handling them.
- **Non-blocking send under the lock in `Submit`:** rejects work when the queue is full instead of blocking, and closes the race where `Close` could close the channel between `Submit`'s closed check and its send.
- **Graceful shutdown:** closing the queue channel lets the worker finish pending jobs; `Close` waits on `done` so it only returns once the worker is finished.
- **Principle:** no producer should ever be left waiting forever.

## Open questions

Decided:
- When the queue is full, `Submit` rejects with an error instead of blocking.

Still open:
- Duplicate jobs: should producers be able to pass an idempotency key?
- Backoff: should retries wait between attempts instead of retrying immediately?
- Error types: how do we tell retryable errors from permanent ones?

## Implementation milestones

### v1: single process, in-memory, one worker (complete)
Shipped: Submit/Get, single worker, up to 3 attempts with immediate retry, failed state with recorded error, per-job result channels, graceful Close, reject-when-full, race-detector-clean tests.

### v2: multiple concurrent workers
- Configurable worker pool; shutdown waits for all workers (`sync.WaitGroup`).
- Re-check shared state: test handlers that count calls must count per job and be concurrency-safe.

### v3: persistence (jobs survive a crash)
- Durable job storage in Postgres; recovery on restart; at-least-once delivery.
- Claiming a job needs a transaction.

### v4: advanced retry and failure handling
- Exponential backoff; retryable vs. permanent errors; attempt history.

### v5: network-accessible job submission
- HTTP API for submitting jobs; authentication and rate limiting.s
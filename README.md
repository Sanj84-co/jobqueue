# jobqueue

A small, in-memory job queue written in Go. Producers hand off work to the queue and move on to other tasks while a single background worker processes jobs in order.

This is v1: one process, one worker, jobs kept in memory. See [DESIGN.md](DESIGN.md) for the full design and roadmap.

## Features

- `Submit` takes a payload and returns a job ID and a result channel; empty payloads are rejected.
- Each job's result is delivered on its own result channel.
- Any job can be looked up by its ID.
- Failed jobs are retried immediately; after three failed attempts, the job is marked failed and the error is recorded.
- `Close` shuts down gracefully: pending jobs finish, and later submits are rejected.
- `Submit` rejects jobs when the queue is full instead of blocking.
- Safe for concurrent use: all locking happens inside the queue.

## Usage

```go
q := NewQueue(func(payload string) error {
    // do the actual work for a job here
    return nil
})

_, ch, err := q.Submit("payload")
if err != nil {
    log.Fatal(err)
}

msg := <-ch
fmt.Println(msg.Status) // done
q.Close()
```

The handler is the function that does the work for each job. Return `nil` for success or an error for failure; failures are retried up to three attempts in total.

## Job states

```
pending → running            worker takes the job (attempts + 1)
running → running            attempt failed, attempts < 3: retry immediately
running → done               attempt succeeded
running → failed             attempt failed, attempts = 3
```

`done` and `failed` are final.

## Running the tests

```bash
go test ./...
go test -race ./...
```

The race detector checks that no data is accessed by two goroutines at once without a lock.

## Design

Key decisions, explained in [DESIGN.md](DESIGN.md):

- `NewQueue` starts the worker, so a queue always has one running and `Close` can always wait for it.
- Retries happen in the worker's loop instead of re-queueing, which avoids the worker blocking on its own full queue.
- `Submit` enqueues with a non-blocking send under the lock, which rejects work when the queue is full and closes the race with `Close`.

## Roadmap

- **v2:** multiple concurrent workers
- **v3:** persistence in Postgres, so jobs survive a crash
- **v4:** retry backoff, transient vs. permanent errors, attempt history
- **v5:** HTTP API for submitting jobs

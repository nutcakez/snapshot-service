# Snapshot Service

A Go microservice that manages snapshots of virtual machine disks (volumes)
through a REST API. Snapshots are created and deleted in the background against
a simulated storage backend that is slow and sometimes fails.

## Running and testing

Every task has a `make` target. Run `make` on its own to list them.

| Command            | What it does                                              | Needs          |
|--------------------|-----------------------------------------------------------|----------------|
| `make run`         | Runs the service locally on `localhost:8080`              | Go 1.27+       |
| `make test`        | Runs all tests                                            | Go 1.27+       |
| `make test-race`   | Runs all tests with the race detector                     | Go 1.27+, gcc  |
| `make docker-run`  | Builds the image and runs the service on `localhost:8080` | Docker Compose |
| `make docker-test` | Builds the test image and runs all tests with `-race`     | Docker Compose |

Without `make`, the Docker targets are plain Compose commands:

```sh
docker compose up --build              # run the service
docker compose run --rm --build test   # run the tests with -race
```

The race detector needs cgo and a C compiler. If `make test-race` fails
locally for that reason, `make docker-test` runs the same tests in a container
that has everything installed.

### Docker image

The `Dockerfile` is multi-stage:

- **test** runs `go test -race` on the full source.
- **build** compiles a static binary without cgo.
- **runtime** is a distroless image (about 16 MB) with only the binary,
  running as a non-root user.

## Configuration

All settings come from environment variables. Unset variables use the default.
A variable that is set but invalid, including an empty number, stops the
service at startup with an error naming it.

| Variable                | Default   | Meaning                                                     |
|-------------------------|-----------|-------------------------------------------------------------|
| `ADDR`                  | `:8080`   | Address the HTTP server listens on                          |
| `SHUTDOWN_TIMEOUT`      | `35s`     | How long shutdown waits for running work                    |
| `STORAGE_MIN_DELAY`     | `2s`      | Shortest simulated storage call                             |
| `STORAGE_MAX_DELAY`     | `10s`     | Longest simulated storage call                              |
| `STORAGE_FAILURE_RATE`  | `0.2`     | Probability that a storage call fails, from 0 to 1          |
| `STORAGE_SEED`          | random    | Fixed seed, makes storage delays and failures reproducible  |
| `RANDOM_SEED`           | see below | `true` to always use a random seed                          |
| `WORKER_MAX_CONCURRENT` | `3`       | Storage operations running at once, across all volumes      |
| `WORKER_MAX_ATTEMPTS`   | `3`       | Tries per operation before it fails                         |
| `WORKER_CALL_TIMEOUT`   | `30s`     | Time limit for a single storage call                        |
| `WORKER_BASE_BACKOFF`   | `1s`      | Wait before the first retry, doubled for each further retry |

Durations use Go's format, such as `500ms`, `2s` or `1m`.

`RANDOM_SEED` defaults to `true` unless `STORAGE_SEED` is set, so setting only
a seed is enough for reproducible runs. Setting both `RANDOM_SEED=true` and
`STORAGE_SEED` is rejected as a conflict.

Example with fast, reproducible storage:

```sh
STORAGE_MIN_DELAY=100ms STORAGE_MAX_DELAY=500ms STORAGE_SEED=42 make run
```

## Trying the API

With the service running:

```sh
# Create a snapshot (returns 202 with the snapshot in "pending")
curl -s -X POST localhost:8080/v1/volumes/vol-1/snapshots \
  -H 'X-Tenant-ID: tenant-a' -H 'Idempotency-Key: my-key-1' \
  -d '{"name":"nightly"}'

# List the tenant's snapshots, optionally filtered
curl -s 'localhost:8080/v1/snapshots?volume_id=vol-1&status=ready' -H 'X-Tenant-ID: tenant-a'

# Get or delete one snapshot
curl -s localhost:8080/v1/snapshots/<id> -H 'X-Tenant-ID: tenant-a'
curl -s -X DELETE localhost:8080/v1/snapshots/<id> -H 'X-Tenant-ID: tenant-a'
```

## Quota

A tenant can have at most 10 snapshots that are not `deleted`. Every other
state counts, including `failed` and `error_deleting`, so a failed snapshot
keeps its slot until the tenant deletes it. A slot frees up when the delete
finishes (`deleted`), not when it is requested (`deleting`).

Creating an 11th snapshot returns `409 Conflict` with the code
`quota_exceeded`. 409 fits because the request conflicts with the tenant's
current state and succeeds once a snapshot is deleted. 429 would suggest
that waiting helps, and 403 usually means missing permissions.

Repeating a request with an `Idempotency-Key` that was already used still
returns the existing snapshot when the tenant is at its quota, because it
creates nothing new. A request rejected by the quota does not use up its key,
so the client can retry it with the same key after freeing a slot.

The count and the insert happen under the same lock, so concurrent requests
can't exceed the limit.

## Logging

Logs are structured `key=value` lines from `log/slog`, written to stdout.

Every request gets a new random request ID, returned in the `X-Request-ID`
response header. A request ID sent by the client is ignored, so IDs are always
unique and generated the same way. Each request's log lines also carry the
tenant from the `X-Tenant-ID` header, when there is one.

Both IDs appear on the access log line for the request and on every log line
of the background job it started, so one search finds everything for a
request, or everything for a tenant:

```
level=INFO msg=request method=POST path=/v1/volumes/vol-1/snapshots status=202 duration=201µs request_id=TWJ4Q… tenant_id=t1
level=INFO msg="storage operation started" op=create snapshot_id=FMVE3C… volume_id=vol-1 tenant_id=t1 request_id=TWJ4Q…
level=INFO msg="storage operation finished" op=create snapshot_id=FMVE3C… volume_id=vol-1 tenant_id=t1 request_id=TWJ4Q…
```

In production I'd add distributed tracing (OpenTelemetry) on top, with trace
IDs passed between services, and switch to JSON log output for the log
pipeline. `slog.NewJSONHandler` is a one-line change in `cmd/server/main.go`.

## Graceful shutdown

On `SIGTERM` (what `docker compose stop` and Kubernetes send) or `SIGINT`
(Ctrl+C), the service shuts down in this order, within one shared deadline of
`SHUTDOWN_TIMEOUT` (default 35s):

1. **HTTP stops.** New connections are refused right away. Requests already in
   progress may finish.
2. **The worker drains.** It accepts no new jobs, but every job submitted
   before the signal may still run and finish, including retries. That covers
   jobs still waiting for their volume too: their request already got a 202.
3. **At the deadline**, running storage calls, backoff waits and jobs still
   waiting for their volume are cancelled.

The gate for new work is at the HTTP level: the server stops taking requests
before the worker is told to shut down, so in a normal shutdown no request
reaches the worker afterwards.

No snapshot is left in `pending`, `creating` or `deleting`. A create that
didn't finish ends `failed` and a delete ends `error_deleting`, with the reason
in `error`:

- `service shut down before the operation started`
- `operation interrupted by service shutdown`

Both states can be deleted by the client afterwards. A cancelled call isn't
counted as a failed attempt. Because a create interrupted mid-call may still
have created the snapshot in storage, deletes are idempotent on the storage
side, so deleting the `failed` snapshot cleans it up either way.

If the HTTP shutdown runs past its deadline and a handler only submits its
job after the worker has stopped, that request gets
`503 Service Unavailable` with the code `shutting_down` and a `Retry-After`
header, and its snapshot is settled as not started.

The process exits with code 0 when everything finished in time, and 1 with
`stopping worker: deadline passed, running jobs were cancelled` when the
deadline was hit. A second signal during shutdown kills the process
immediately.

Docker gives containers 10s after `SIGTERM` before killing them, so
`compose.yaml` sets `stop_grace_period: 40s`, longer than `SHUTDOWN_TIMEOUT`.
In Kubernetes, `terminationGracePeriodSeconds` plays the same role.

The store is in memory, so all state is lost when the process exits anyway.
The point of settling snapshots is that the code already behaves correctly
with a persistent store: after a restart, nothing would be stuck in a state
that no running process will ever finish.

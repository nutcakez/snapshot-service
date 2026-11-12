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

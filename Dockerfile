# syntax=docker/dockerfile:1

# --- base: Go toolchain and module download, shared by the other stages ---
FROM golang:1.27-bookworm AS base
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .

# --- test: runs the whole suite with the race detector ---
# -race needs cgo, so this stage keeps CGO_ENABLED=1 (gcc is in the image).
FROM base AS test
ENV CGO_ENABLED=1
CMD ["go", "test", "-race", "-count=1", "./..."]

# --- build: static binary with no cgo, so it runs on a minimal image ---
FROM base AS build
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# --- runtime: only the binary, running as a non-root user ---
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /out/server /server
EXPOSE 8080
USER nonroot:nonroot
# Exec form: the server is PID 1 and receives SIGTERM directly.
ENTRYPOINT ["/server"]

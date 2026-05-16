# syntax=docker/dockerfile:1.7
#
# Build context MUST be the repo root so the proto module is available alongside
# the service module (the go.mod replace directive points at ../../proto).
# Build with:  docker build -f services/messagequeue/Dockerfile -t gpu-telemetry/messagequeue:dev .

ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

# Copy module manifests first so Docker layer caching speeds up rebuilds when
# only Go source changes.
COPY proto/go.mod proto/go.sum ./proto/
COPY services/messagequeue/go.mod services/messagequeue/go.sum ./services/messagequeue/

# Pre-fetch dependencies (replace directive resolves ../../proto locally).
WORKDIR /src/services/messagequeue
RUN go mod download

WORKDIR /src
COPY proto/ ./proto/
COPY services/messagequeue/ ./services/messagequeue/

WORKDIR /src/services/messagequeue
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/messagequeue \
    ./cmd/server

# ──────────────────────────────────────────────────────────────────────
# Runtime: distroless static — no shell, no package manager, just the binary.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/messagequeue /messagequeue

# WAL persistence: mount a volume here for crash safety.
VOLUME ["/var/lib/mq-wal"]
ENV MQ_WAL_DIR=/var/lib/mq-wal

EXPOSE 9090 9091

ENTRYPOINT ["/messagequeue"]

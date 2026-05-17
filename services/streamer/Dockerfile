# syntax=docker/dockerfile:1.7
# Build with:  docker build -f services/streamer/Dockerfile -t gpu-telemetry/streamer:dev .

ARG GO_VERSION=1.23

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

COPY proto/go.mod proto/go.sum ./proto/
COPY services/streamer/go.mod services/streamer/go.sum ./services/streamer/

WORKDIR /src/services/streamer
RUN go mod download

WORKDIR /src
COPY proto/ ./proto/
COPY services/streamer/ ./services/streamer/

WORKDIR /src/services/streamer
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/streamer \
    ./cmd/server

# ──────────────────────────────────────────────────────────────────────
# Runtime: distroless static. Sample CSV is baked in at /data/sample_data.csv
# so pods start with no external dependency (no ConfigMap, no PV, no fetch).
# Override $CSV_PATH at runtime to point at a different file mounted elsewhere.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/streamer /streamer
COPY data/sample_data.csv /data/sample_data.csv

ENV CSV_PATH=/data/sample_data.csv

EXPOSE 9091

ENTRYPOINT ["/streamer"]

# syntax=docker/dockerfile:1.7
# Build with:  docker build -f services/collector/Dockerfile -t gpu-telemetry/collector:dev .
#
# The migrations/ directory is embedded into the binary at build time via
# go:embed, so no extra volume mount is needed for SQL files.

ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

COPY proto/go.mod proto/go.sum ./proto/
COPY services/collector/go.mod services/collector/go.sum ./services/collector/

WORKDIR /src/services/collector
RUN go mod download

WORKDIR /src
COPY proto/ ./proto/
COPY services/collector/ ./services/collector/

WORKDIR /src/services/collector
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/collector \
    ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/collector /collector

EXPOSE 9091

ENTRYPOINT ["/collector"]

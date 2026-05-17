# syntax=docker/dockerfile:1.7
# Build with:  docker build -f services/gateway/Dockerfile -t gpu-telemetry/gateway:dev .
#
# Gateway has no proto dependency — only Postgres. Build context is repo root for
# consistency with the other services.

ARG GO_VERSION=1.23

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

COPY services/gateway/go.mod services/gateway/go.sum ./services/gateway/

WORKDIR /src/services/gateway
RUN go mod download

WORKDIR /src
COPY services/gateway/ ./services/gateway/

WORKDIR /src/services/gateway
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/gateway \
    ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/gateway /gateway

EXPOSE 8080 9091

ENTRYPOINT ["/gateway"]

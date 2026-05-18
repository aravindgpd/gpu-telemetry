# GPU Telemetry Pipeline

[![Go Version](https://img.shields.io/badge/go-1.23-blue.svg)](https://go.dev/) [![License](https://img.shields.io/badge/license-MIT-green.svg)](#license)

An **elastic, horizontally-scalable telemetry pipeline** for AI clusters running NVIDIA GPUs. Reads DCGM exporter CSV, fans it through a **custom message queue** (no Kafka / RabbitMQ / ZeroMQ), persists to PostgreSQL, and exposes a REST API.

Built as a take-home project. Designed to run on Kubernetes via Helm.

```
   ┌─────────────┐   proto bytes   ┌──────────────┐   proto bytes   ┌────────────┐    SQL    ┌────────────┐    HTTP    ┌────────┐
   │  Streamer   │ ──────────────► │ Custom MQ    │ ──────────────► │ Collector  │ ───────►  │ PostgreSQL │ ◄───────── │Gateway │ ──► User
   │ (StatefulSet│   gRPC bidi     │ (Broker, WAL,│  gRPC server    │ (Deployment│           │            │            │ (REST) │
   │  ≤ 10 pods) │   stream        │  ring, group │  stream         │  ≤ 10 pods)│           │            │            │        │
   └─────────────┘                 │  rebalance)  │                 └────────────┘           └────────────┘            └────────┘
                                   └──────────────┘
```

---

## Contents

- [Quick start (local)](#quick-start-local)
- [Architecture overview](#architecture-overview)
- [Design decisions & assumptions](#design-decisions--assumptions)
- [Project layout](#project-layout)
- [Build & packaging](#build--packaging)
- [Installation workflow](#installation-workflow)
- [Sample user workflow](#sample-user-workflow)
- [API reference](#api-reference)
- [Running tests + coverage](#running-tests--coverage)
- [Further reading](#further-reading)
- [How AI was used](#how-ai-was-used)

---

## Quick start (local)

The fastest way to see everything work end-to-end is `docker compose`:

```powershell
docker compose up --build
# (in another terminal)
curl http://localhost:8080/api/v1/gpus
curl 'http://localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?limit=5'
# Or open the interactive Swagger UI:
#   http://localhost:8080/swagger/index.html
docker compose down -v
```

Within 30 seconds you should see two GPUs and rows of telemetry returning. The streamer replays [data/sample_data.csv](data/sample_data.csv) in a loop, so the dataset grows continuously until you stop the stack.

For Kubernetes (kind cluster, public images on Docker Hub):

```powershell
kind create cluster --name gpu-telemetry
helm dependency update deploy/helm/gpu-telemetry
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry `
    --namespace gpu-telemetry --create-namespace `
    --values deploy/helm/gpu-telemetry/values.yaml `
    --wait --timeout 5m
kubectl --namespace gpu-telemetry port-forward svc/gpu-telemetry-gateway 8080:8080
```

Full runbook with smoke-test verification in **[docs/HELM_INSTALL_AND_SMOKE_TEST.md](docs/HELM_INSTALL_AND_SMOKE_TEST.md)**.

---

## Architecture overview

Four services and a database, all containerized:

| Service | Type | Job |
|---|---|---|
| **Streamer** | StatefulSet (1–10 replicas) | Reads a DCGM CSV, partitions rows by ordinal (`row_num % STREAMER_TOTAL == STREAMER_INDEX`), publishes to the MQ via a bidi gRPC stream |
| **Message Queue** | StatefulSet (1 replica + PVC for WAL) | Custom broker with topic/partition model, ring buffer per partition, Write-Ahead Log on disk, consumer groups, partition rebalancing |
| **Collector** | Deployment + HPA (1–10 replicas) | Subscribes to the MQ via server-streaming gRPC, deserialises payloads, writes to PostgreSQL with idempotent upserts |
| **Gateway** | Deployment + HPA (2–10 replicas) | REST API for querying GPUs and telemetry; chi router, Swagger-annotated handlers |
| **PostgreSQL** | StatefulSet (Bitnami subchart) | Storage. Long/melted schema (one row per `<gpu, metric, timestamp>` sample) |

The MQ broker is the substantive piece of original code (~1000 lines). Its design is documented in depth in [docs/SYSTEM_WALKTHROUGH.md](docs/SYSTEM_WALKTHROUGH.md), but the short story is: each partition is a fixed-capacity in-memory ring buffer backed by an append-only write-ahead log. Subscribers register a coalesced notify channel per partition. Consumer groups round-robin partitions across members with **stable rebalancing** — when a fleet member joins or leaves, only members whose assigned set actually changed are forced to reconnect (avoids the "stop the world" thrashing that naïve Kafka-style protocols suffer).

---

## Design decisions & assumptions

Three of these came back from clarification questions with the interviewer; the rest were judgment calls documented here so the trade-offs are visible.

### Canonical sample timestamp = Streamer's wall-clock at publish time

*(Confirmed with interviewer.)* The CSV's own timestamp column is decorative and ignored. Each pass through the CSV produces fresh samples with fresh timestamps, simulating a continuous live feed. This is set in [reader.go:152-181](services/streamer/internal/reader/reader.go#L152) where `time.Now().UnixNano()` is assigned to both `IngestedUnixNs` and `SampleUnixNs` of every `TelemetryRecord`.

### Shared single PostgreSQL across all Collectors

*(Confirmed with interviewer; your call.)* All Collector replicas write to one Postgres instance. Idempotency is provided by a `UNIQUE(uuid, metric_name, sample_at)` constraint plus `ON CONFLICT DO NOTHING` inserts — see [postgres.go:102-117](services/collector/internal/store/postgres.go#L102).

**Why:** at the spec's 10-replica cap, one Postgres easily handles the throughput. Keeping the Gateway stateless and backed by a unified view simplifies reads. DB-level dedup means Collectors require zero coordination and can scale freely. At-least-once delivery from the MQ combined with idempotent inserts gives **effectively exactly-once** persistence without distributed transactions.

*Alternatives considered:* per-Collector storage with Gateway federation (operationally complex); TimescaleDB (better fit for telemetry but adds a new dependency).

### Streamer coordination = deterministic row-partitioning by ordinal

*(Confirmed with interviewer; "just avoid duplication".)* Streamers run as a StatefulSet, giving each pod a stable ordinal `STREAMER_INDEX`. Each Streamer publishes only the CSV rows where `row_number % STREAMER_TOTAL == STREAMER_INDEX`. Implemented in [coordinator.go:21](services/streamer/internal/coordinator/coordinator.go#L21).

**Why:** no inter-Streamer coordination needed; each row published exactly once across the fleet; scaling is deterministic. Adding Streamer N+1 simply increments the modulus.

### Message Queue: standalone gRPC service (not a library)

The spec allowed either; we chose a service. Survives Kubernetes pod churn cleanly, lets the streamer and collector tiers scale independently, and keeps the broker's persistence (WAL) decoupled from any specific producer/consumer.

### API: UUID as identifier + limit/offset pagination

`{id}` in `GET /api/v1/gpus/{id}/telemetry` is the GPU UUID (globally unique, stable across re-cabling). Pagination via `limit` (default 100, max 1000) + `offset`. Time-window filters `start_time` / `end_time` are inclusive, RFC3339-formatted. All these are enforced in [gpu.go:49-107](services/gateway/internal/handler/gpu.go#L49).

### Sample data delivery to Streamer pods

The DCGM CSV is **baked into the Streamer Docker image** at `/data/sample_data.csv`. ConfigMap mounting was considered (and previously implemented) but the image-baking approach gives a self-contained deployable with no `--set-file` magic at install time. To use a different dataset, rebuild the Streamer image with your CSV at that path.

### MQ persistence semantics

WAL writes happen **before** the in-memory ring buffer update — durability first. `MQ_WAL_SYNC_BYTES` (default 4096) controls how aggressively fsync runs. Setting it to 0 syncs on every record (highest durability, lowest throughput); the default amortises fsync over ~4KB of writes.

### Rebalance protocol

"Stable rebalance" (described above). Detailed walkthrough in [docs/SYSTEM_WALKTHROUGH.md §4.3](docs/SYSTEM_WALKTHROUGH.md). Notable safety net: when a Subscribe handler exits due to a rebalance, the broker keeps the member entry around for a 30-second grace window before garbage-collecting it ([broker.go:289](services/messagequeue/internal/broker/broker.go#L289)). This prevents brief reconnects from triggering cascading rebalances.

### Multi-cluster topology (each service on its own cluster)

Every component has an `enabled: true/false` flag in [values.yaml](deploy/helm/gpu-telemetry/values.yaml). The chart can render any subset. Cross-cluster wiring uses three overrides:

- **`messagequeue.externalAddress`** — `host:port` of an MQ broker living in a different cluster. Wins over the in-cluster service name in the `mqAddress` helper.
- **`messagequeue.service.type=LoadBalancer` (or `NodePort`)** — provisions a second `*-external` Service alongside the headless one, so other clusters can reach the broker.
- **`postgresql.enabled=false` + `externalDatabase.*`** — Collector + Gateway point at a Postgres in a different cluster (or a managed RDS-style DB).

Four example values files in [deploy/helm/gpu-telemetry/examples/](deploy/helm/gpu-telemetry/examples/) cover the four canonical topologies: MQ-only, streamer-only, collector-only, gateway-only. Pick one per cluster.

The full procedure (LoadBalancer setup, cross-cluster routing, sanity checks, limitations) is in **[docs/MULTI_CLUSTER.md](docs/MULTI_CLUSTER.md)**.

#### Configuring an external Message Queue endpoint

This is the most common cross-cluster case: the broker lives in Cluster A; streamers and collectors live in Cluster B. Here's exactly what happens when you set `messagequeue.externalAddress`.

**1. Expose the broker (Cluster A) externally.**  Default service type is `ClusterIP` (headless), which is only reachable inside the cluster. Switch it via values or `--set`:

```bash
# Cluster A — install just the broker, exposed via LoadBalancer
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
    --namespace gpu-telemetry --create-namespace \
    --values deploy/helm/gpu-telemetry/examples/messagequeue-only.values.yaml
# The example file sets messagequeue.service.type=LoadBalancer, which renders
# a second Service called <release>-messagequeue-external.

# Discover the external IP / DNS name:
kubectl --namespace gpu-telemetry get svc gpu-telemetry-messagequeue-external
#   EXTERNAL-IP   PORT(S)
#   34.120.20.45  9090:32341/TCP
```

Save `34.120.20.45:9090` (or your DNS name) — that's the address streamers and collectors in other clusters will dial.

**2. Tell the streamer/collector cluster to use it.**  In Cluster B, install the streamer (or collector) and supply the external address:

```bash
# Cluster B — streamers pointing at the Cluster A broker
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
    --namespace gpu-telemetry --create-namespace \
    --values deploy/helm/gpu-telemetry/examples/streamer-only.values.yaml \
    --set messagequeue.externalAddress=34.120.20.45:9090
```

The example file already has `messagequeue.enabled: false` and `streamer.enabled: true`, so the chart only renders streamer resources.

**3. How the value reaches the pod.**  A single Helm helper resolves the address, and both streamer and collector templates pull from it:

```
values.yaml                  _helpers.tpl                 statefulset/deployment       pod env var               Go code
  messagequeue.        →  {{- define mqAddress }}    →  env:                       →  MQ_ADDRESS=         →  cfg.MQAddress
    externalAddress         picks external OR            - name: MQ_ADDRESS              "34.120.20.45:9090"      ↓
                            in-cluster DNS                 value: {{ helper }}                                   grpc.NewClient(...)
```

So one knob in values.yaml flows into the `MQ_ADDRESS` env var that both [streamer/statefulset.yaml:28-29](deploy/helm/gpu-telemetry/templates/streamer/statefulset.yaml#L28) and [collector/deployment.yaml:30-31](deploy/helm/gpu-telemetry/templates/collector/deployment.yaml#L30) inject. The streamer's [config.go:24](services/streamer/internal/config/config.go#L24) and collector's [config.go:23](services/collector/internal/config/config.go#L23) read `MQ_ADDRESS`, and both pass it to `grpc.NewClient(cfg.MQAddress, ...)`.

**4. Verify the wiring worked.**  After install, check the actual env var that ended up in the running pod:

```bash
kubectl --namespace gpu-telemetry exec gpu-telemetry-streamer-0 -- env | grep MQ_ADDRESS
# MQ_ADDRESS=34.120.20.45:9090

# Logs should show the streamer connecting (with retry on slow networks):
kubectl --namespace gpu-telemetry logs gpu-telemetry-streamer-0 | grep -E "MQ|broker"
# {"level":"info","msg":"connecting to MQ broker","address":"34.120.20.45:9090","topic":"gpu-telemetry"}
# {"level":"info","msg":"dependency reachable","op":"CreateTopic","attempts":1}
```

If the streamer is stuck in `waiting for dependency op=CreateTopic` retries, the cross-cluster network path isn't open. Test connectivity directly:

```bash
kubectl --namespace gpu-telemetry exec gpu-telemetry-streamer-0 -- nc -zv 34.120.20.45 9090
```

**5. Why only `messagequeue.externalAddress` and not separate `streamer.externalMQAddress` / `collector.externalMQAddress`?**  Both consumers of the broker must agree on the same endpoint, so one knob feeds both via the shared `mqAddress` helper — there's no way to point them at different places by accident. The streamer and collector themselves are pure clients (nothing dials *into* them), so they don't have their own `externalAddress` fields.

**Default behaviour (`externalAddress` empty):** the helper falls back to the in-cluster DNS name `gpu-telemetry-messagequeue:9090`. The single-cluster install in §[Quick start](#quick-start-local) is exactly that case.

### Startup-order resilience (any pod, any machine, any order)

Each of the four services can come up **independently of its dependencies** and waits politely instead of crashlooping. This matters when:

- The components run on different machines (network may not have converged at startup)
- You're testing one service in isolation (e.g. bring up the Collector before MQ exists)
- Pods restart in arbitrary order after a node failure

| Service | Hard dependency | Strategy |
|---|---|---|
| **MQ broker** | none | Always starts standalone |
| **Streamer** | MQ broker | `publisher.New(ctx, ...)` wraps the initial `CreateTopic` + `Publish`-stream open in [exponential backoff](services/streamer/internal/publisher/retry.go) (200 ms → 10 s cap). Returns only when MQ is reachable or ctx is cancelled. The `obs` server (`/healthz`, `/readyz`, `/metrics`) starts FIRST so liveness probes answer immediately during the wait. |
| **Collector** | Postgres + MQ | Postgres: `pool.Ping` wrapped in the same retry loop ([retry.go](services/collector/internal/store/retry.go)). MQ: `consumer.Run` already had reconnect-on-Subscribe-error built in ([consumer.go:71-83](services/collector/internal/consumer/consumer.go#L71)). |
| **Gateway** | Postgres | Same retry pattern as the Collector. `obs` server starts first; once Postgres is reachable, full HTTP server comes up. |

Logs make the wait visible — every retry warns with `op`, `attempt`, `next_retry_in`, and the underlying error. When the dependency comes up, a single info log records `attempts: N`. Operators see exactly which dependency is missing.

The retry helpers are cancellable: SIGTERM during the wait exits cleanly via `ctx.Done()` rather than blocking shutdown.

---

## Project layout

```
gpu-telemetry/
├── api/                              ← Generated OpenAPI spec (make openapi)
│   └── swagger.yaml
├── buf.yaml, buf.gen.yaml            ← Proto generation config
├── data/sample_data.csv              ← 60 rows DCGM data, 2 GPUs × 10 metrics × 3 timestamps
├── deploy/helm/gpu-telemetry/        ← Umbrella Helm chart (4 workloads + Postgres subchart)
├── docker-compose.yml                ← Local end-to-end stack
├── docs/
│   ├── SYSTEM_WALKTHROUGH.md         ← Architecture deep-dive
│   ├── HELM_INSTALL_AND_SMOKE_TEST.md ← Real-cluster runbook
│   └── AI_USAGE.md                   ← How AI assisted this build
├── go.work                           ← Go workspace (5 modules)
├── Makefile                          ← All operational targets
├── proto/                            ← Module: shared .proto + generated bindings
│   ├── mq/mq.proto                   ← Custom MQ protocol
│   └── telemetry/telemetry.proto     ← Payload schema
└── services/
    ├── messagequeue/   ← Custom broker (gRPC server + ring buffer + WAL + group state)
    ├── streamer/       ← CSV reader + MQ publisher
    ├── collector/      ← MQ consumer + PostgreSQL writer (embedded migrations)
    └── gateway/        ← REST API (chi router, Swagger annotations)
```

Every service module is independent (`go.mod` + `go.sum`) and Docker-buildable from the repo root. The `go.work` file at the top ties them together for local development; `replace` directives in each service's `go.mod` point at `../../proto` so single-module Docker builds work without the workspace.

---

## Build & packaging

All operational tasks are driven from the [Makefile](Makefile):

```powershell
make help               # full list of targets

# Code generation
make proto              # buf generate → proto/*/*.pb.go
make openapi            # swag init → api/swagger.yaml

# Build
make build              # all 4 service binaries → ./bin/
make images             # all 4 Docker images (tag: dev)

# Quality
make test               # go test in every module
make coverage           # coverage.out + coverage.html with total %
make lint               # go vet + golangci-lint (if installed)

# Local dev
make up                 # docker compose up --build
make down               # docker compose down -v
make logs               # tail compose logs

# Kubernetes
make helm-deps          # helm dependency update (Bitnami Postgres)
make helm-install       # install chart into gpu-telemetry namespace
make helm-uninstall     # tear down

# Housekeeping
make tidy               # go mod tidy across every module
make clean              # remove bin/, coverage files, test caches
```

### Prerequisites

| Tool | Purpose |
|---|---|
| Go 1.23+ | Build & test |
| `buf` 1.50+ | Proto code generation (`go install github.com/bufbuild/buf/cmd/buf@latest`) |
| `swag` | OpenAPI generation (`go install github.com/swaggo/swag/cmd/swag@latest`) |
| Docker 24+ | Container builds and `docker compose` |
| Helm 3.13+ | Chart installation |
| `kind` (or any k8s) | Local cluster for the smoke test |

---

## Installation workflow

### Option A: docker compose (laptop)

See [Quick start](#quick-start-local).

### Option B: Helm + kind (local cluster simulating production)

The chart's `values.yaml` defaults to the **published Docker Hub images**:

```
aravindgpd/gpu-telemetry-messagequeue:dev
aravindgpd/gpu-telemetry-streamer:dev
aravindgpd/gpu-telemetry-collector:dev
aravindgpd/gpu-telemetry-gateway:dev
```

So installation is a single `helm upgrade --install`. No build or `kind load` required. The complete procedure (including verification, scaling tests, and troubleshooting) is in **[docs/HELM_INSTALL_AND_SMOKE_TEST.md](docs/HELM_INSTALL_AND_SMOKE_TEST.md)**.

### Option C: Real cluster (EKS / GKE / AKS / on-prem)

Same as Option B with three caveats:
1. Replace `aravindgpd/*` with your private registry refs (`--set image.registry=...`)
2. Override the Postgres password with a real secret (the default `changeme` is a placeholder)
3. Turn on `gateway.ingress.enabled=true` and configure your `host` + TLS

---

## Sample user workflow

After `docker compose up --build` (give it ~15 seconds for migrations + initial publishes):

```powershell
# 1. List GPUs for which telemetry has been seen
curl http://localhost:8080/api/v1/gpus
# [
#   {"uuid":"GPU-5fd4f087-86f3-7a43-b711-4771313afc50","gpu_index":"0","device":"nvidia0",
#    "model_name":"NVIDIA H100 80GB HBM3","hostname":"mtv5-dgx1-hgpu-031",...},
#   {"uuid":"GPU-bc7a12ab-4998-fdc5-0785-2678a929a142", ...}
# ]

# 2. Pull recent telemetry for one GPU
curl "http://localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?limit=10"

# 3. Filter by metric name
curl "http://localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?metric_name=DCGM_FI_DEV_GPU_UTIL&limit=20"

# 4. Filter by time window (ISO 8601 / RFC 3339)
curl "http://localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2026-05-17T12:00:00Z&end_time=2026-05-17T12:05:00Z"

# 5. Discover what GPU models exist — no need to guess the exact string
curl http://localhost:8080/api/v1/models
# [{"model_name":"NVIDIA H100 80GB HBM3","gpu_count":12}, ...]

# 6. Cross-GPU search by metric — model_name does case-insensitive substring match,
#    so "h100" works instead of the full URL-encoded "NVIDIA+H100+80GB+HBM3"
curl "http://localhost:8080/api/v1/telemetry?metric_name=DCGM_FI_DEV_GPU_UTIL&model_name=h100&limit=100"

# 7. Filter GPU listing by model (substring also works on hostname)
curl "http://localhost:8080/api/v1/gpus?model_name=h100&hostname=dgx"

# 8. Liveness / readiness
curl http://localhost:8080/healthz   # always 200
curl http://localhost:8080/readyz    # 200 if DB is reachable, 503 otherwise

# 9. Or skip curl entirely — interactive UI with "Try it out" for every endpoint:
#    Open http://localhost:8080/swagger/index.html in a browser
```

To verify scaling, edit `docker-compose.yml` and bump `streamer` to 2 replicas, or scale collectors via `docker compose up --scale collector=3`. The MQ broker handles partition rebalancing transparently.

---

## API reference

### Interactive Swagger UI

The gateway serves a live Swagger UI you can use to explore and try every endpoint without writing curl by hand:

| URL | What it serves |
|---|---|
| `http://localhost:8080/swagger/index.html` | Interactive API explorer (Try-it-out buttons) |
| `http://localhost:8080/swagger/doc.json` | Raw OpenAPI 2.0 spec (JSON) |
| `http://localhost:8080/swagger` | Convenience redirect to `/swagger/index.html` |

For Kubernetes deployments, port-forward the gateway first:

```powershell
kubectl --namespace gpu-telemetry port-forward svc/gpu-telemetry-gateway 8080:8080
# Then browse http://localhost:8080/swagger/index.html
```

When `gateway.ingress.enabled=true`, Swagger is also reachable at `http(s)://<your-host>/swagger/index.html`.

### Static spec

The OpenAPI spec is also checked into the repo at [api/swagger.yaml](api/swagger.yaml) and [api/swagger.json](api/swagger.json). Regenerate with `make openapi` whenever you change handler annotations.

### Endpoint list

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/gpus` | List GPUs (with optional model/host filters + pagination) |
| `GET` | `/api/v1/gpus/{id}/telemetry` | Samples for one GPU |
| `GET` | `/api/v1/telemetry` | Cross-GPU telemetry search (filter by metric/model/uuid/time) |
| `GET` | `/api/v1/models` | Discover distinct GPU model names + GPU counts |
| `GET` | `/healthz` | Liveness probe (always 200) |
| `GET` | `/readyz` | Readiness probe (200 / 503 based on DB ping) |
| `GET` | `/swagger/*` | Interactive API explorer + raw spec |

### Friendly filtering: `model_name` and `hostname` use case-insensitive substring match

The `model_name` and `hostname` filters on `/gpus` and `/telemetry` perform `ILIKE %X%` matching, so you don't have to URL-encode long model strings:

| What you want | What you can type | What also works (exact) |
|---|---|---|
| All H100s | `?model_name=h100` | `?model_name=NVIDIA+H100+80GB+HBM3` |
| All A100 80GB | `?model_name=a100` | `?model_name=NVIDIA+A100+80GB` |
| All hosts on the dgx1 rack | `?hostname=dgx1` | `?hostname=mtv5-dgx1-hgpu-031` |

If you're not sure what model strings exist, hit `GET /api/v1/models` first.

### `GET /api/v1/gpus` query parameters

| Param | Type | Default | Notes |
|---|---|---|---|
| `model_name` | string | (any) | Case-insensitive substring match — `h100` matches `NVIDIA H100 80GB HBM3` |
| `hostname`   | string | (any) | Case-insensitive substring match — `dgx1` matches `mtv5-dgx1-hgpu-031` |
| `limit`      | int    | 100   | Capped at 1000 |
| `offset`     | int    | 0     | Page offset |

### `GET /api/v1/gpus/{id}/telemetry` query parameters

| Param | Type | Default | Notes |
|---|---|---|---|
| `metric_name` | string | (all) | e.g. `DCGM_FI_DEV_GPU_UTIL`. Omit for every metric. |
| `start_time` | RFC 3339 | (no lower bound) | Inclusive lower bound on `sample_at` |
| `end_time` | RFC 3339 | (no upper bound) | Inclusive upper bound on `sample_at` |
| `limit` | int | 100 | Capped at 1000 |
| `offset` | int | 0 | Page offset |

### `GET /api/v1/telemetry` query parameters

Cross-GPU variant — all filters optional, combine freely. Returns samples ordered by `sample_at` ASC.

| Param | Type | Default | Notes |
|---|---|---|---|
| `metric_name` | string | (any) | e.g. `DCGM_FI_DEV_POWER_USAGE` |
| `model_name`  | string | (any) | GPU model — case-insensitive substring (uses ILIKE; JOINs `gpus`). `h100` is enough. |
| `uuid`        | string | (any) | Filter to one specific GPU |
| `start_time`  | RFC 3339 | (no lower bound) | Inclusive lower bound on `sample_at` |
| `end_time`    | RFC 3339 | (no upper bound) | Inclusive upper bound on `sample_at` |
| `limit`       | int    | 100 | Capped at 1000 |
| `offset`      | int    | 0   | Page offset |

Example questions this endpoint answers in one call:

| Question | Query |
|---|---|
| "GPU_UTIL across every H100 in the last 5 min" | `?metric_name=DCGM_FI_DEV_GPU_UTIL&model_name=NVIDIA+H100+80GB+HBM3&start_time=...` |
| "All metrics from host mtv5-dgx1-hgpu-031" | `?` *(then filter client-side using gpus index)* |
| "Power draw history for one specific GPU" | `?uuid=GPU-5fd4...&metric_name=DCGM_FI_DEV_POWER_USAGE` |

---

## Running tests + coverage

```powershell
make test       # all modules, all tests, race detector on
make coverage   # produces coverage.html + prints aggregate %
```

### Current state

**Aggregate: 61.2%** across all packages.

Coverage on the testable business logic is uniformly high:

| Package | Coverage |
|---|---:|
| `messagequeue/internal/broker` | 84.4% |
| `messagequeue/internal/config` | 94.1% |
| `messagequeue/internal/server` (bufconn-based gRPC tests) | 55.9% |
| `messagequeue/internal/obs` | 95.8% |
| `streamer/internal/coordinator` | **100%** |
| `streamer/internal/reader` | 87.0% |
| `streamer/internal/config` | 94.4% |
| `streamer/internal/obs` | 95.8% |
| `collector/internal/config` | 92.9% |
| `collector/internal/consumer` | 30.2% (`process` 100%; `Run`/`runOnce`/`ack` need a full bufconn fixture) |
| `collector/internal/obs` | 95.8% |
| `gateway/internal/config` | 90.9% |
| `gateway/internal/handler` | **97.3%** |

Packages at 0% are integration-only and need infrastructure tests:
- `streamer/internal/publisher` (gRPC client — needs bufconn)
- `collector/internal/store` (needs testcontainers/postgres)
- `gateway/internal/store` (same)
- `*/cmd/server` (main packages — typically excluded)

A handful of `bufconn`-based publisher tests and a `dockertest`/`testcontainers` setup for the stores would push aggregate above 80%. Both are scoped as future work.

### Test design notes

- **Broker** has dedicated WAL, partition, group, topic, and orchestrator tests. Includes:
  - WAL torn-write recovery test (truncate mid-record, assert clean replay)
  - Ring-buffer wrap test with concrete tail/head verification
  - Consumer group **stable rebalance** test (proves no eviction-thrashing on rejoin)
  - Round-trip restart test (publish → close → reopen → resume)
  - Grace-period eviction test for `LeaveIfSameMember`
- **Gateway** uses `httptest` + a fake `Repository` mock; covers parameter validation, pagination edges, DB error paths, both health endpoints.
- **MQ gRPC service** uses `google.golang.org/grpc/test/bufconn` for in-process end-to-end RPC tests including the streaming Publish and Subscribe paths.
- **Consumer** uses a fake `store.Repository` to test the `process` path; gRPC-streaming paths (`Run` / `runOnce`) are integration-shaped and not unit-testable without a full broker fixture.

---

## Further reading

- **[docs/SYSTEM_WALKTHROUGH.md](docs/SYSTEM_WALKTHROUGH.md)** — Architecture deep-dive, end-to-end data flow, broker internals (~700 lines, the canonical project doc)
- **[docs/HELM_INSTALL_AND_SMOKE_TEST.md](docs/HELM_INSTALL_AND_SMOKE_TEST.md)** — Step-by-step runbook for deploying to a real Kubernetes cluster
- **[docs/AI_USAGE.md](docs/AI_USAGE.md)** — Detailed account of how AI assistance was used during development

---

## How AI was used

This entire project was built with extensive AI assistance (Claude in IDE). Brief summary:

- **Architecture & design decisions**: drafted via conversational design sessions, then refined manually
- **Boilerplate code**: AI-generated and reviewed (Dockerfiles, Helm templates, gRPC service handlers, env loaders)
- **Custom MQ broker**: AI wrote the initial design (WAL + ring buffer + group state), I manually refined the rebalance protocol after seeing thrashing in tests
- **Unit tests**: AI-generated with my edits to add missed edge cases (torn writes, stable rebalance, grace-period eviction)
- **Documentation**: AI-drafted, I edited for accuracy and added the design-decisions framing

Where AI fell short: workspace-mode `replace` directive interaction with `go work sync` (had to debug by hand), Bitnami Postgres image namespace change in Aug 2025 (had to find the legacy redirect manually), the grace-timer leave protocol (designed and implemented manually after AI's initial naïve version caused rebalance storms).

Full breakdown with specific prompts and their outputs in **[docs/AI_USAGE.md](docs/AI_USAGE.md)**.

---

## License

MIT. See your typical MIT terms — copy, modify, sub-license freely.

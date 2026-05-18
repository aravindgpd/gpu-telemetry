# Helm Install + Smoke Test — Runbook

A step-by-step procedure for deploying the GPU Telemetry pipeline to a Kubernetes cluster and verifying every component is live. Designed to run end-to-end in roughly 10 minutes on a developer laptop.

---

## 1. Prerequisites

You need these on your `PATH`:

| Tool | Minimum | Purpose | Install hint |
|---|---|---|---|
| `docker` | 24+ | Build images, run kind | https://docs.docker.com/get-docker/ |
| `kubectl` | 1.28+ | Talk to the cluster | `winget install Kubernetes.kubectl` |
| `helm` | 3.13+ | Install the chart | `winget install Helm.Helm` |
| `kind` | 0.23+ | Local Kubernetes (recommended) | `go install sigs.k8s.io/kind@latest` |
| `go` | 1.23+ | Already installed | — |
| `buf` | 1.50+ | Already installed | — |
| `make` | optional | Drives the targets in [Makefile](../Makefile). If absent, run the underlying commands manually as shown below. | `winget install GnuWin32.Make` |

Confirm everything:

```powershell
docker version
kubectl version --client
helm version
kind --version
go version
```

---

## 2. The Five-Minute Path (kind + Helm, pre-built images)

The chart's `values.yaml` defaults to public images on Docker Hub:

```
aravindgpd/gpu-telemetry-messagequeue:dev
aravindgpd/gpu-telemetry-streamer:dev
aravindgpd/gpu-telemetry-collector:dev
aravindgpd/gpu-telemetry-gateway:dev
```

So you can skip the local build + `kind load` steps entirely. Kubernetes pulls them on first pod creation.

```powershell
# 1) Spin up a local cluster
kind create cluster --name gpu-telemetry

# 2) Resolve the Bitnami Postgres subchart (one-time)
helm dependency update deploy/helm/gpu-telemetry

# 3) Install the chart — images pulled from Docker Hub automatically
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry `
    --namespace gpu-telemetry --create-namespace `
    --values deploy/helm/gpu-telemetry/values.yaml `
    --wait --timeout 5m

# 4) Verify
kubectl --namespace gpu-telemetry get pods

# 5) Smoke test the API
kubectl --namespace gpu-telemetry port-forward svc/gpu-telemetry-gateway 8080:8080
# (in another terminal)
curl http://localhost:8080/api/v1/gpus
```

If all of that works, jump to [§5 — smoke tests](#5-smoke-tests) for the full verification suite.

> **Building locally instead?** Skip ahead to [§3.2 — build images](#32-build-images-optional-only-if-you-want-to-rebuild-from-source). The local-build images get the same `aravindgpd/...:dev` tags so the chart picks them up either way.

---

## 3. Detailed Walkthrough

### 3.1 Create the kind cluster

```powershell
kind create cluster --name gpu-telemetry --wait 60s
```

Expected output:
```
Creating cluster "gpu-telemetry" ...
 ✓ Ensuring node image (kindest/node:v1.31.x) 🖼
 ✓ Preparing nodes 📦
 ...
Set kubectl context to "kind-gpu-telemetry"
```

Verify the node is `Ready`:
```powershell
kubectl get nodes
# NAME                          STATUS   ROLES           AGE
# gpu-telemetry-control-plane   Ready    control-plane   30s
```

If you'd rather use `minikube`, `Docker Desktop`'s embedded Kubernetes, or a real cluster, just skip this step and ensure your `kubectl` context points where you want.

### 3.2 Build images (optional — only if you want to rebuild from source)

The published images are sufficient for the smoke test. Build locally only when you've changed code. The Dockerfiles assume the **repo root** as build context (so the proto module is available alongside each service module):

```powershell
# If make is available
make images

# Otherwise, the four explicit builds:
docker build -f services/messagequeue/Dockerfile -t aravindgpd/gpu-telemetry-messagequeue:dev .
docker build -f services/streamer/Dockerfile     -t aravindgpd/gpu-telemetry-streamer:dev     .
docker build -f services/collector/Dockerfile    -t aravindgpd/gpu-telemetry-collector:dev    .
docker build -f services/gateway/Dockerfile      -t aravindgpd/gpu-telemetry-gateway:dev      .
```

Each image is built with multi-stage `golang:1.23-alpine` → `gcr.io/distroless/static-debian12:nonroot`. Final image sizes are ~30 MB each.

Verify:
```powershell
docker images | Select-String aravindgpd/gpu-telemetry
# aravindgpd/gpu-telemetry-gateway        dev   abc...   30 seconds ago   28MB
# aravindgpd/gpu-telemetry-collector      dev   def...   1 minute ago     32MB
# aravindgpd/gpu-telemetry-streamer       dev   ghi...   1 minute ago     30MB
# aravindgpd/gpu-telemetry-messagequeue   dev   jkl...   2 minutes ago    29MB
```

### 3.3 Load locally-built images into kind (only if you built in §3.2)

If you're using the **published** images, skip this — Kubernetes pulls from Docker Hub on its own.

If you **built locally**, kind cannot see your host's Docker image cache directly. `kind load docker-image` copies the image into the kind node:

```powershell
kind load docker-image aravindgpd/gpu-telemetry-messagequeue:dev `
                       aravindgpd/gpu-telemetry-streamer:dev `
                       aravindgpd/gpu-telemetry-collector:dev `
                       aravindgpd/gpu-telemetry-gateway:dev `
                       --name gpu-telemetry
```

After loading, set `image.pullPolicy=Never` for the install so kind doesn't try to pull from Docker Hub and ignore your local copy:

```powershell
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry `
    --namespace gpu-telemetry --create-namespace `
    --values deploy/helm/gpu-telemetry/values.yaml `
    --set messagequeue.image.pullPolicy=Never `
    --set streamer.image.pullPolicy=Never `
    --set collector.image.pullPolicy=Never `
    --set gateway.image.pullPolicy=Never `
    --wait --timeout 5m
```

> **For a real cluster** instead of kind: the published images on Docker Hub work as-is. For a private registry, push under your own namespace and override `messagequeue.image.repository`, `streamer.image.repository`, etc. at install time.

### 3.4 Resolve chart dependencies

The chart depends on the Bitnami Postgres subchart, declared in [Chart.yaml](../deploy/helm/gpu-telemetry/Chart.yaml). Helm caches it under `charts/`:

```powershell
helm dependency update deploy/helm/gpu-telemetry
```

Expected output:
```
Hang tight while we grab the latest from your chart repositories...
...Successfully got an update from the "bitnami" chart repository
Saving 1 charts
Downloading postgresql from repo https://charts.bitnami.com/bitnami
Deleting outdated charts
```

If you see *"no repository definition for ..."*, run:
```powershell
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

then retry `helm dependency update`.

### 3.5 Install the chart

```powershell
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry `
    --namespace gpu-telemetry --create-namespace `
    --values deploy/helm/gpu-telemetry/values.yaml `
    --wait --timeout 5m
```

Critical flags:

| Flag | Why |
|---|---|
| `--namespace gpu-telemetry --create-namespace` | All resources land in their own namespace |
| `--wait --timeout 5m` | Helm blocks until every Pod is `Ready` or 5 minutes elapse |

The streamer image has the sample DCGM CSV baked in at `/data/sample_data.csv` (see [services/streamer/Dockerfile](../services/streamer/Dockerfile)) — no ConfigMap or `--set-file` plumbing required. To use a different dataset, rebuild the streamer image with your CSV at that path.

Expected output:
```
Release "gpu-telemetry" has been upgraded. Happy Helming!
NAME: gpu-telemetry
LAST DEPLOYED: ...
NAMESPACE: gpu-telemetry
STATUS: deployed
REVISION: 1

NOTES:
────────────────────────────────────────────────────────────────────────────
GPU Telemetry Pipeline — release "gpu-telemetry" installed in
namespace "gpu-telemetry".
...
```

The `NOTES.txt` ([NOTES.txt](../deploy/helm/gpu-telemetry/templates/NOTES.txt)) tells you the next-step port-forward command.

If the install fails, see [§7 — troubleshooting](#7-troubleshooting).

---

## 4. Verifying the rollout

### 4.1 Pod inventory

```powershell
kubectl --namespace gpu-telemetry get pods
```

Expected output (eight pods total — most via the Bitnami subchart and the four services):

```
NAME                                            READY   STATUS    RESTARTS   AGE
gpu-telemetry-postgresql-0                      1/1     Running   0          90s
gpu-telemetry-messagequeue-0                    1/1     Running   0          90s
gpu-telemetry-streamer-0                        1/1     Running   0          90s
gpu-telemetry-streamer-1                        1/1     Running   0          90s
gpu-telemetry-collector-7c49df6c84-abcde        1/1     Running   0          90s
gpu-telemetry-collector-7c49df6c84-fghij        1/1     Running   0          90s
gpu-telemetry-gateway-66dd7b6c84-klmno          1/1     Running   0          90s
gpu-telemetry-gateway-66dd7b6c84-pqrst          1/1     Running   0          90s
```

### 4.2 Service endpoints

```powershell
kubectl --namespace gpu-telemetry get svc
```

Expected (annotated for what's connected to what):
```
NAME                          TYPE        CLUSTER-IP      PORT(S)            AGE
gpu-telemetry-postgresql      ClusterIP   10.96.x.x       5432/TCP           # ← Postgres
gpu-telemetry-postgresql-hl   ClusterIP   None            5432/TCP           # ← Postgres headless
gpu-telemetry-messagequeue    ClusterIP   None            9090/TCP,9091/TCP  # ← MQ headless
gpu-telemetry-streamer        ClusterIP   None            9091/TCP           # ← Streamer headless
gpu-telemetry-gateway         ClusterIP   10.96.x.x       8080/TCP,9091/TCP  # ← Gateway
```

### 4.3 Resource summary

```powershell
kubectl --namespace gpu-telemetry get all
```

Should report:
- 1 StatefulSet `gpu-telemetry-postgresql` (replicas 1)
- 1 StatefulSet `gpu-telemetry-messagequeue` (replicas 1)
- 1 StatefulSet `gpu-telemetry-streamer` (replicas 2)
- 2 Deployments: `collector`, `gateway`
- 3 HPAs: `streamer`, `collector`, `gateway`

---

## 5. Smoke Tests

Each test verifies one layer of the stack. Run them top-down — failures upstream often mask successes downstream.

### 5.1 Streamer is reading the CSV

```powershell
kubectl --namespace gpu-telemetry logs gpu-telemetry-streamer-0 --tail=30
```

Expected log lines (in JSON from zap):
```json
{"level":"info","msg":"telemetry streamer starting","index":0,"total":2,"csv_path":"/data/sample_data.csv","mq_address":"gpu-telemetry-messagequeue:9090"}
{"level":"info","msg":"obs server listening","port":9091}
```

If you see `level: "warn"` repeating with `"skipping unparseable CSV row"`, the baked-in CSV at `/data/sample_data.csv` is malformed — rebuild the streamer image after fixing `data/sample_data.csv` in the repo.

### 5.2 MQ broker has the topic

```powershell
kubectl --namespace gpu-telemetry logs gpu-telemetry-messagequeue-0 | Select-String "topic created"
```

Expected:
```json
{"level":"info","msg":"topic created","topic":"gpu-telemetry","partitions":10,"ring_buffer_size":65536}
```

### 5.3 Collector is consuming and writing

```powershell
kubectl --namespace gpu-telemetry logs deployment/gpu-telemetry-collector --tail=30
```

Expected:
```json
{"level":"info","msg":"telemetry collector starting","consumer_id":"gpu-telemetry-collector-7c49df6c84-abcde","topic":"gpu-telemetry"}
{"level":"info","msg":"subscribed to topic","topic":"gpu-telemetry","group":"collector-group"}
{"level":"info","msg":"database migrations up to date","known":1}
```

If you see *"Acknowledge failed"* repeating, the broker was restarted while the collector was mid-flight — usually self-heals on the next reconnect.

### 5.4 Postgres has GPU rows

```powershell
kubectl --namespace gpu-telemetry exec -it gpu-telemetry-postgresql-0 -- `
    psql -U gpu_user -d gpu_telemetry -c "SELECT COUNT(*) FROM gpus; SELECT COUNT(*) FROM telemetry_samples;"
```

Expected (numbers will grow as the streamer loops):
```
 count
-------
     2
(1 row)

 count
-------
   240
(1 row)
```

The 2 GPUs come from the unique UUIDs in [data/sample_data.csv](../data/sample_data.csv); the sample count keeps climbing until you stop the cluster.

### 5.5 Gateway is reachable

In one terminal, port-forward the Gateway:
```powershell
kubectl --namespace gpu-telemetry port-forward svc/gpu-telemetry-gateway 8080:8080
```

In another terminal, hit each endpoint:

```powershell
# Liveness — always 200
curl http://localhost:8080/healthz
# {"status":"ok"}

# Readiness — should be 200 (DB is reachable)
curl http://localhost:8080/readyz
# {"status":"ok"}

# List GPUs — expect a JSON array of 2 items
curl http://localhost:8080/api/v1/gpus | python -m json.tool

# Telemetry for one GPU — expect samples streaming back
curl "http://localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?limit=5" | python -m json.tool

# Discover GPU models the gateway has seen (use the spelling that suits you)
curl http://localhost:8080/api/v1/models | python -m json.tool

# Cross-GPU search by metric and model — model_name is a case-insensitive
# substring, so "h100" is enough; no need to URL-encode "NVIDIA H100 80GB HBM3"
curl "http://localhost:8080/api/v1/telemetry?metric_name=DCGM_FI_DEV_GPU_UTIL&model_name=h100&limit=20" | python -m json.tool
```

Or open the **interactive Swagger UI** in a browser — every endpoint has a "Try it out" button:

```
http://localhost:8080/swagger/index.html
```

The raw OpenAPI spec is served at `http://localhost:8080/swagger/doc.json`. A bare `http://localhost:8080/swagger` redirects to the index for convenience.

Expected response shape for `/api/v1/gpus`:
```json
[
    {
        "uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
        "gpu_index": "0",
        "device": "nvidia0",
        "model_name": "NVIDIA H100 80GB HBM3",
        "hostname": "mtv5-dgx1-hgpu-031",
        "created_at": "2026-05-08T11:47:23Z",
        "updated_at": "2026-05-08T11:48:01Z"
    },
    ...
]
```

### 5.6 Metrics scrape works

```powershell
kubectl --namespace gpu-telemetry port-forward svc/gpu-telemetry-messagequeue 9091:9091
# In another terminal:
curl http://localhost:9091/metrics | Select-String "go_goroutines"
# go_goroutines 12
```

The default Prometheus collectors (Go runtime, process) are exposed on every service's metrics port. Domain counters (`mq_publish_total`, `collector_writes_total`, etc.) are deliberately not implemented yet — see [SYSTEM_WALKTHROUGH.md §9](SYSTEM_WALKTHROUGH.md).

---

## 6. Scaling Tests

Verify the elasticity properties the spec calls for.

### 6.1 Scale streamers up

```powershell
helm upgrade gpu-telemetry deploy/helm/gpu-telemetry --reuse-values --set streamer.replicaCount=4

# Watch new pods come up
kubectl --namespace gpu-telemetry get pods -l app.kubernetes.io/component=streamer -w
```

The new streamers should start at ordinal 2 and 3 (`gpu-telemetry-streamer-2`, `-3`). Each picks up its own slice of CSV rows via `STREAMER_INDEX % STREAMER_TOTAL`.

### 6.2 Scale collectors up

```powershell
kubectl --namespace gpu-telemetry scale deployment/gpu-telemetry-collector --replicas=4
kubectl --namespace gpu-telemetry get pods -l app.kubernetes.io/component=collector -w
```

When the new collectors join, the broker's group rebalances. Watch in the broker logs:

```powershell
kubectl --namespace gpu-telemetry logs gpu-telemetry-messagequeue-0 -f | Select-String -Pattern "rebalance|subscribed"
```

You should see:
```
{"msg":"group member subscribed","topic":"gpu-telemetry","group":"collector-group","member_id":"...","assigned_partitions":[0,1,2]}
{"msg":"group member evicted by rebalance","evicted_id":"..."}
{"msg":"group member subscribed",...,"assigned_partitions":[3,4,5]}
```

### 6.3 Pod resilience

Kill the broker pod and confirm the streamer/collector reconnect:

```powershell
kubectl --namespace gpu-telemetry delete pod gpu-telemetry-messagequeue-0
kubectl --namespace gpu-telemetry get pods -w
```

The pod is recreated by the StatefulSet within ~5 seconds. The streamer's publisher and collector's consumer both have reconnect loops ([publisher.go](../services/streamer/internal/publisher/publisher.go), [consumer.go:71-83](../services/collector/internal/consumer/consumer.go#L71)) that re-establish gRPC streams automatically. Postgres rows continue to grow throughout.

The WAL on the broker's persistent volume guarantees no committed offset is lost across the restart — see [partition.go:46-65](../services/messagequeue/internal/broker/partition.go#L46).

---

## 7. Troubleshooting

### "ImagePullBackOff" on every pod
You're using kind but didn't run `kind load docker-image`. Either load the images, or push them to a registry your cluster can reach and update `image.repository` / `image.registry` in values.

### `"docker.io/bitnami/postgresql:<tag>" image not found` (or similar `bitnami/...` 404)
As of August 2025, Bitnami moved their public versioned image catalog out of `docker.io/bitnami/*` and into a frozen archive at `docker.io/bitnamilegacy/*`. The main `bitnami` namespace now serves only `latest` tags for paid subscribers, so pinned chart tags (like `postgresql:16.4.0-debian-12-r14`) return "not found".

The chart's [values.yaml](../deploy/helm/gpu-telemetry/values.yaml) already redirects to `bitnamilegacy/*` and sets `global.security.allowInsecureImages: true` (required because the Bitnami subchart refuses non-`bitnami` repos by default). If you see this error, you've likely overridden one of those values — re-check that the `postgresql.image`, `postgresql.volumePermissions.image`, and `postgresql.metrics.image` blocks still point at `bitnamilegacy/*`.

If a specific legacy tag itself 404s (the archive is frozen but not guaranteed stable), find a working one:

```powershell
curl -s "https://registry.hub.docker.com/v2/repositories/bitnamilegacy/postgresql/tags?page_size=20" | python -m json.tool
```

Long-term, consider switching to [CloudNativePG](https://cloudnative-pg.io/) to drop the Bitnami dependency entirely.

### "PostgreSQL pod stuck in CrashLoopBackOff"
The Bitnami chart's password validation rejects values shorter than 8 characters. The default `changeme` works but feel free to override via `--set postgresql.auth.password=<longer-string>`.

### "Streamer is logging 'skipping unparseable CSV row' constantly"
The CSV baked into the streamer image is malformed. Fix [data/sample_data.csv](../data/sample_data.csv) in the repo, rebuild the streamer image (`docker build -f services/streamer/Dockerfile -t aravindgpd/gpu-telemetry-streamer:dev .`), reload it into the cluster, and roll the StatefulSet.

### "Collector logs 'subscription error, reconnecting'"
Normal during pod restarts. If it persists, check `kubectl logs gpu-telemetry-messagequeue-0` — the broker may be unhealthy.

### "/readyz returns 503"
The Gateway's readiness probe pings Postgres. Check the Postgres pod status. If Postgres is up, run `kubectl exec -it gpu-telemetry-postgresql-0 -- psql -U gpu_user -d gpu_telemetry -c '\\dt'` to verify the connection from inside the cluster.

### "API returns empty array for /api/v1/gpus"
Either the streamer hasn't published yet (give it 30 seconds), or the collector isn't writing. Check both logs in order — streamer first, then collector, then Postgres.

### "helm upgrade hangs at 'waiting for resources'"
One pod failed to become Ready. Open another terminal and run:
```powershell
kubectl --namespace gpu-telemetry describe pod <stuck-pod>
```
The `Events:` section at the bottom usually shows why (image pull failure, probe failing, OOM, etc.).

---

## 8. Production / Real-Cluster Notes

When moving from `kind` to a managed cluster (EKS / GKE / AKS / on-prem), three things change:

1. **Image distribution**: push images to a registry the cluster can reach. Set `--set image.registry=registry.example.com/`. The Helm templates already have `{{ .Values.global.imageRegistry }}` prefixed onto every image reference.

2. **Postgres credentials**: never use the `changeme` default. Either:
   - Inline override: `--set postgresql.auth.password=$(openssl rand -hex 16)`
   - External Secret: create a `Secret` first, then `--set postgresql.auth.existingSecret=<secret-name>`

3. **Ingress**: turn on `gateway.ingress.enabled=true`, set `gateway.ingress.host`, and provide TLS. The chart already has the [ingress template](../deploy/helm/gpu-telemetry/templates/gateway/ingress.yaml).

For a production-ready manifest, also consider:
- Set explicit `resources.requests` and `resources.limits` to match your nodes
- Reduce `messagequeue.replicaCount` only after you've validated WAL durability on the underlying storage class
- Set `collector.autoscaling.maxReplicas` ≤ MQ partition count (10 by default) — extra collectors sit idle (see [SYSTEM_WALKTHROUGH.md §3](SYSTEM_WALKTHROUGH.md))

---

## 9. Teardown

```powershell
# Remove the chart and all resources
helm uninstall gpu-telemetry --namespace gpu-telemetry

# Remove the persistent volumes (else data lingers)
kubectl --namespace gpu-telemetry delete pvc --all

# Remove the namespace
kubectl delete namespace gpu-telemetry

# If using kind: nuke the whole cluster
kind delete cluster --name gpu-telemetry
```

The `make helm-uninstall` target wraps the first command. Cleaning the kind cluster is typically what you want during iterative development since it's the fastest way to reset everything.

---

## 10. Expected Timing

On a 2024-era developer laptop with a warm Docker cache:

| Step | Duration |
|---|---|
| `kind create cluster` | 30–60 s |
| Build all 4 images | 90–180 s (cold) / 10–30 s (warm) |
| `kind load docker-image` × 4 | 30–60 s |
| `helm dep update` | 5–10 s |
| `helm install --wait` | 60–120 s |
| **Total cold start** | **3–6 minutes** |
| **Total warm reinstall** | **30–60 seconds** |

If your run is significantly slower, check (a) Docker has enough memory (~6 GB recommended), (b) you're on the same network as the Bitnami chart repo, and (c) the kind node hasn't been throttled by Docker's CPU limits.

---

## 11. What This Validates

A clean run of all sections proves:
- ✅ All four service images build with `go 1.23.0` and the embedded migrations + proto bindings
- ✅ The custom MQ broker provisions topics, persists to disk, and serves Subscribe streams
- ✅ Streamer's row-partitioning correctly fans out across replicas
- ✅ Collector's idempotent INSERTs (the `ON CONFLICT DO NOTHING` natural-key) handle retries
- ✅ Gateway returns valid JSON conforming to [api/swagger.yaml](../api/swagger.yaml)
- ✅ Helm chart templates render correctly under non-default values (replicas, scaling)
- ✅ Pod-level resilience: kill a broker pod, the system self-heals via WAL replay + reconnect loops
- ✅ HPAs are wired in (visible via `kubectl get hpa`)

This is the most thorough end-to-end check available without writing dedicated integration tests.

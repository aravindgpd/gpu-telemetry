# Multi-Cluster Deployment Guide

The default Helm install puts all four services + Postgres into one cluster, finding each other via in-cluster DNS. This document covers the **distributed** topology: each service in its own cluster (or on its own VM), endpoints exposed across cluster boundaries, services configured to talk to those endpoints.

---

## What's supported

Every component has an `enabled` flag in [values.yaml](../deploy/helm/gpu-telemetry/values.yaml). The chart can render zero or more of:

- `messagequeue` (the broker)
- `streamer` (the CSV reader)
- `collector` (the consumer + DB writer)
- `gateway` (the REST API)
- `postgresql` (Bitnami subchart) — already had its own enabled flag

Plus three escape hatches for cross-cluster wiring:

| Override | Used when | Effect |
|---|---|---|
| `messagequeue.externalAddress` | This cluster does NOT run the broker | Streamer + Collector point at `host:port` instead of the in-cluster service name |
| `messagequeue.service.type` | This cluster DOES run the broker AND others must reach it | `NodePort` or `LoadBalancer` provisions an extra `*-external` Service alongside the headless one |
| `postgresql.enabled: false` + `externalDatabase.*` | Postgres lives elsewhere | Collector + Gateway use the supplied DSN; no in-chart DB |

Plus the **startup-order retry** ([streamer publisher](../services/streamer/internal/publisher/retry.go), [collector/gateway store](../services/collector/internal/store/retry.go)) — so services come up before their cross-cluster dependencies appear and just wait politely with exponential backoff.

---

## The four canonical topologies

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Topology A:  One cluster — the default                                      │
│   helm install ... (no flags)                                               │
│   Everything inside cluster boundary; uses internal DNS.                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ Topology B:  Split MQ                                                       │
│   Cluster A:  messagequeue (LoadBalancer)                                   │
│   Cluster B:  streamer + collector + gateway + postgres                     │
│                  ↑                                                          │
│             messagequeue.externalAddress points at Cluster A                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ Topology C:  Read/write split                                               │
│   Cluster A:  messagequeue (LoadBalancer)                                   │
│   Cluster B:  streamer (talks to A)                                         │
│   Cluster C:  collector + postgres (talks to A's MQ, writes locally)        │
│   Cluster D:  gateway (talks to C's Postgres via externalDatabase)          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ Topology D:  Fully fanned out                                               │
│   Cluster A:  messagequeue                                                  │
│   Cluster B:  streamer                                                      │
│   Cluster C:  collector                                                     │
│   Cluster D:  gateway                                                       │
│   External:   managed Postgres (RDS / Cloud SQL / Crunchy / ...)            │
│                                                                             │
│   B → A   via messagequeue.externalAddress                                  │
│   C → A   via messagequeue.externalAddress                                  │
│   C → DB  via externalDatabase.host                                         │
│   D → DB  via externalDatabase.host                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Walkthrough: Topology D (fully fanned out)

This is the most demanding case. Four installs, each on a different cluster, all pointing at the same external Postgres.

### Step 1 — Cluster A (broker)

```bash
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
    --namespace gpu-telemetry --create-namespace \
    --values deploy/helm/gpu-telemetry/examples/messagequeue-only.values.yaml
```

Wait for the LoadBalancer to get an IP/hostname:

```bash
kubectl --namespace gpu-telemetry get svc gpu-telemetry-messagequeue-external -w
# NAME                                     TYPE           EXTERNAL-IP        PORT(S)
# gpu-telemetry-messagequeue-external      LoadBalancer   34.120.20.45       9090:32341/TCP
```

Save that endpoint — e.g. `34.120.20.45:9090` — you'll use it as the `messagequeue.externalAddress` in the other clusters.

### Step 2 — Cluster B (streamer)

```bash
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
    --namespace gpu-telemetry --create-namespace \
    --values deploy/helm/gpu-telemetry/examples/streamer-only.values.yaml \
    --set messagequeue.externalAddress=34.120.20.45:9090
```

The streamer pods come up and immediately log:
```
{"level":"info","msg":"connecting to MQ broker","address":"34.120.20.45:9090"}
```
followed by either successful CreateTopic / Publish-stream open, or `waiting for dependency` retries if the network path isn't healthy yet.

### Step 3 — Cluster C (collector + DB)

Either run a local Postgres (default in the example values) or point at an external one:

```bash
# Local Postgres in this cluster:
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
    --namespace gpu-telemetry --create-namespace \
    --values deploy/helm/gpu-telemetry/examples/collector-only.values.yaml \
    --set messagequeue.externalAddress=34.120.20.45:9090

# Or with an external managed Postgres:
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
    --namespace gpu-telemetry --create-namespace \
    --values deploy/helm/gpu-telemetry/examples/collector-only.values.yaml \
    --set messagequeue.externalAddress=34.120.20.45:9090 \
    --set postgresql.enabled=false \
    --set externalDatabase.host=pg.example.com \
    --set externalDatabase.password=$PG_PASSWORD
```

### Step 4 — Cluster D (gateway)

```bash
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
    --namespace gpu-telemetry --create-namespace \
    --values deploy/helm/gpu-telemetry/examples/gateway-only.values.yaml \
    --set externalDatabase.host=pg.example.com \
    --set externalDatabase.password=$PG_PASSWORD \
    --set gateway.ingress.host=gpu-telemetry.example.com
```

The Ingress is on by default in this example — users hit `https://gpu-telemetry.example.com/api/v1/gpus` and the gateway returns whatever Cluster C has persisted.

---

## What about network security?

The chart **does not** add TLS to the gRPC traffic between Streamer/Collector and the MQ broker. Two ways to harden:

1. **Service mesh (Istio / Linkerd / Cilium)** — mTLS handled at the sidecar level, app code unchanged.
2. **Application-level TLS** — set the gRPC client's `TransportCredentials` to `credentials.NewTLS(...)` and the server to listen with cert/key. Adds a Helm value + Secret per cluster. Not implemented; documented as follow-up.

For a take-home demo this is acceptable — the trade-off is documented but the implementation is intentionally simple.

---

## Verifying which resources get rendered

After modifying values, render the chart locally before installing:

```bash
helm template gpu-telemetry deploy/helm/gpu-telemetry \
    --values deploy/helm/gpu-telemetry/examples/streamer-only.values.yaml \
    --set messagequeue.externalAddress=test:9090 \
    | grep "^kind:"
# Expected output:
#   kind: ServiceAccount       # Postgres subchart still loaded its CRDs unless disabled
#   kind: StatefulSet          # ← streamer (the only thing we want)
#   kind: Service              # ← streamer headless service
```

If you see unexpected `kind:` lines for components that should be disabled, the `enabled` flag on that component isn't being honoured — check that you applied the latest chart version that has the gates.

---

## Quick sanity checks per cluster

| Cluster | Run this to verify | Expected |
|---|---|---|
| MQ | `kubectl logs -l app.kubernetes.io/component=messagequeue` | `topic created` log line |
| MQ | `kubectl get svc -l app.kubernetes.io/component=messagequeue` | TWO services if `service.type != ClusterIP`: headless + external |
| Streamer | `kubectl logs -l app.kubernetes.io/component=streamer` | `connecting to MQ broker address=<your-external-endpoint>` then `published row` debug lines |
| Collector | `kubectl logs -l app.kubernetes.io/component=collector` | `subscribed to topic` log; query Postgres for row count |
| Gateway | `curl https://<ingress-host>/api/v1/gpus` | JSON array |

If any service is stuck in the `waiting for dependency` retry loop, the network path between this cluster and the upstream cluster isn't open. Try `kubectl exec` into the pod and `nc -zv <external-host> 9090` to validate the route directly.

---

## Limitations + follow-ups

| Limitation | Mitigation |
|---|---|
| No TLS on cross-cluster gRPC | Use a service mesh or implement app-level TLS (config knob already exists in publisher.New for `TransportCredentials`) |
| `externalAddress` is a single host:port | For HA the broker would need a static-IP LB or DNS-based round-robin; the streamer's grpc.NewClient supports `dns:///host:port,host:port` |
| MQ WAL is single-replica | The chart sets `messagequeue.replicaCount: 1`; multi-replica broker would need a leader election protocol that's out of scope for this project |
| Postgres password is a plaintext value | Use `externalDatabase.existingSecret` + `existingSecretPasswordKey` for any non-dev deploy |

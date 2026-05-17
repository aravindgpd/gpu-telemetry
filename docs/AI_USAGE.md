# How AI Was Used to Build This Project

This document is required by the spec ("Document your use of AI") and is meant to be honest. It covers the development workflow phase-by-phase: what AI generated well, what required manual intervention, and the prompts that worked best.

The AI used was **Claude (Anthropic)**, accessed through the Claude Code extension in VS Code, with conversation-mode iteration. Approximate split: **~70% AI-generated, ~30% manually written or substantially edited.**

---

## Workflow at a glance

| Phase | AI did | I did |
|---|---|---|
| Architecture design | Drafted the four-service split, partition model, idempotency story | Validated against spec; pushed back on a few directions; made the final calls |
| Proto definitions (`telemetry.proto`, `mq.proto`) | Wrote both files from a conversational description | Reviewed field numbers, added rebalance signal fields |
| MQ broker (the hardest piece) | Wrote initial implementation of WAL, partition, group, broker, gRPC service (~1000 lines) | Found the rebalance thrashing bug, redesigned rebalance protocol, added grace-timer leave |
| Streamer / Collector / Gateway services | Wrote all packages and main.go entry points | Light editing for naming + comments |
| Migrations system | Designed and wrote the embedded-FS migrator | Validated transactional behaviour |
| Dockerfiles + docker-compose | Generated all 4 Dockerfiles + compose stack | Adjusted layer caching, removed copy-paste cruft |
| Helm chart (17 templates) | Generated full chart with NOTES.txt + values + helpers | Fixed broken `grpc:` probes, replaced ConfigMap CSV with image-baked, adjusted to Bitnami legacy registry |
| Unit tests | Drafted every test file | Added edge cases: torn writes, stable rebalance, ring buffer wrap |
| Docs | Drafted system walkthrough, smoke-test runbook, this file | Edited for accuracy; reorganised sections |

---

## Project bootstrapping

### Repo bootstrap prompt

> *"I'm building an elastic GPU telemetry pipeline as a Go take-home project. Custom MQ (no Kafka/Rabbit/ZMQ). Four services: streamer, collector, MQ broker, API gateway. Deployed via Helm on Kubernetes. Show me a recommended Go workspace layout and a phased delivery plan."*

**What worked:** AI produced a clean `go.work` + per-service `go.mod` layout, recommended chi/pgx/zap, and broke the work into 8 phases (foundation, MQ, streamer, collector, gateway, Docker, Helm, tests+docs). This plan held up almost unchanged through the entire project.

**What didn't:** AI initially suggested embedding the MQ broker as a library inside the streamer + collector. The spec allows either, but a service is the right call for the elasticity goal. I redirected with: *"the spec says we should be able to scale streamers and collectors independently — does the library approach handle pod churn?"* and AI immediately switched to the service design.

### Code scaffolding prompt

> *"Generate the directory tree for all four services with go.mod, cmd/server/main.go, internal/config/config.go, and an empty internal/<domain> package per service. Use zap for logging, chi for the gateway router, pgx for Postgres."*

**What worked:** AI produced all the scaffolding in one shot, including idiomatic main.go shutdown via `signal.NotifyContext`. The zap + chi + pgx choices were standard enough that AI got them right without further prompting.

**What didn't:** AI's first cut of the proto file used Snake_Case field names that didn't match Go convention after generation. I asked: *"What will protoc-gen-go emit for these field names? Show me the Go struct."* — AI showed the output, I corrected `gpu_id` → `gpu_index` to avoid confusion with the globally-unique UUID, and we shipped the corrected version.

---

## The MQ broker (the genuinely novel piece)

This is the part where AI gave the biggest leverage and also the part that needed the most manual correction.

### Initial design prompt

> *"Design a custom MQ broker in Go that supports: bidirectional Publish stream, server-streaming Subscribe, consumer groups, persistent offsets, partition rebalancing. Capacity is 10 streamers and 10 collectors. No Kafka — write from scratch."*

**What AI produced:** A ring-buffer-per-partition design with a write-ahead log, a round-robin partition assignment in consumer groups, and a `done`-channel based rebalance signal protocol. This was good — 80% of what I ended up shipping.

### Where AI fell short — rebalance thrashing

AI's first rebalance implementation was the "stop the world" version:
```go
// Naïve version — close everyone's done channel on every Join/Leave
for _, m := range g.members {
    close(m.done)
}
```

This caused infinite ping-pong reconnects in my second-member-joins test scenario. I described the failure to AI:

> *"When collector-1 joins a group that already has collector-0, collector-0 reconnects and re-Joins. That close-everyone approach causes collector-1 to be evicted too. Then collector-1 reconnects and evicts collector-0. They keep evicting each other forever. What's the correct protocol?"*

AI responded with the **set-equality based rebalance**: only evict members whose assigned-partition set actually changed. This is the design in [group.go:64-94](../services/messagequeue/internal/broker/group.go#L64) and the property is verified by `TestGroupReJoinAfterRebalanceIsStable`. AI had the right concept after one nudge; I had to spot the bug first.

### Where AI fell short — the grace-period leave

After the rebalance fix, integration testing surfaced a second issue: when a Subscribe handler exited due to a rebalance signal, the broker's `Cleanup()` immediately called `Leave()` and recomputed assignments — which then triggered ANOTHER rebalance because the leaving member was the one that just got rebalanced in. Quick double-rebalance.

I designed the fix manually: a `SkipLeaveOnCleanup` flag plus a delayed `LeaveIfSameMember` timer with pointer-equality protection (so a fast reconnect supersedes the timer). AI helped review the code but the design intent came from me. See [broker.go:284-355](../services/messagequeue/internal/broker/broker.go#L284) and [group.go:97-118](../services/messagequeue/internal/broker/group.go#L97).

### Specific MQ prompts that worked

- *"Show me the WAL binary format you'd use. I want torn-write recovery."* → AI proposed body-length-prefix encoding, which lets the reader detect partial trailing records and stop cleanly. Tested in `TestWALPartialTrailingRecord`.
- *"How should I notify subscribers without blocking the publisher?"* → AI suggested coalesced notifications via a `chan struct{}` with buffer 1 + non-blocking `select` send. Worked perfectly first try.
- *"Walk me through how an offset survives a broker restart"* → AI explained replay-on-startup, which became the design for `newPartition(... replay)` taking pre-existing WAL records.

---

## Unit tests

### Approach prompt

> *"Write unit tests for the broker package. Use t.TempDir for WAL files. No external mocks; the package can be tested directly. Cover: WAL write+replay round-trip, ring buffer wrap, consumer-group rebalancing (especially the stable case), torn-write recovery, restart replay."*

**What worked:** AI generated all 6 test files (`wal_test.go`, `partition_test.go`, `topic_test.go`, `broker_test.go`, `group_test.go`, `consumer_test.go`) with reasonable coverage. The fake `store.Repository` for collector tests, the `fakePublisher` for streamer reader tests, and the `bufconn` setup for the gRPC service tests all came from AI.

**Where AI was insufficient:**
- AI's first cut of the rebalance test didn't actually verify the stable-rejoin property — it only checked counts. I added the explicit assertion: *"after collector-0 reconnects, collector-1's done channel must still be open."*
- AI's reader Stream test originally tried to test the concrete `*publisher.Publisher` type. I extracted a `Publisher` interface ([reader.go:40](../services/streamer/internal/reader/reader.go#L40)) so a fake could be injected, and AI then generated the Stream test correctly against the interface.
- Edge case for `LeaveIfSameMember` where the pointer guard protects against evicting a re-Subscribed member — I asked AI to write a test for the "what if the pointer doesn't match" case, which produced `TestGroupLeaveIfSameMemberWhenReplaced`.

### Coverage analysis prompt

> *"Aggregate coverage is 57%. Which packages would lift the number most for the least work?"*

AI correctly identified that `obs` (HTTP wrapper) and `messagequeue/server` (gRPC) were the biggest 0%-coverage chunks of testable code. The bufconn-based `service_test.go` was the result of that observation — it lifted the server package from 0% to 55.9%.

---

## Docker + Helm

### Dockerfile prompt

> *"Generate multi-stage Dockerfiles for all 4 services. Build context is repo root. Use distroless static for runtime. The proto module is shared and accessed via a replace directive."*

**What worked:** AI produced all four Dockerfiles cleanly, with proper layer caching (copy go.mod first, then go mod download, then source). The gateway Dockerfile correctly didn't include the proto module since gateway has no proto dependency.

**What didn't:** AI baked the streamer's CSV into the image via `COPY data/sample_data.csv /data/`. I removed it initially in favour of a ConfigMap volume in the Helm chart, then later went back to AI's original baked-in approach when the ConfigMap pattern proved fragile (`--set-file` had path resolution issues). The image-baked approach is what's shipped.

### Helm chart prompt

> *"Generate a Helm umbrella chart for all 4 services + a Postgres dependency. Streamer is a StatefulSet (ordinal-based partitioning). MQ is a StatefulSet (PVC for WAL). Collector and Gateway are Deployments with HPAs. All replicas cap at 10."*

**What worked:** AI produced the entire chart in one shot — Chart.yaml, values.yaml, 13 templates, helpers, NOTES.txt. The deployment-vs-statefulset distinction was correctly applied per workload. The PVC for MQ WAL persistence was included automatically.

**Where AI fell short:**

1. **gRPC liveness probe** — AI used `livenessProbe: { grpc: { port: 9090 } }` which requires the standard `grpc.health.v1.Health` service. Our broker has its own `HealthCheck` RPC but not the standard one. The probe failed silently in early testing. I switched to `tcpSocket` probes (see [messagequeue/statefulset.yaml:47-58](../deploy/helm/gpu-telemetry/templates/messagequeue/statefulset.yaml#L47)) and documented this as a known follow-up.

2. **Bitnami image redirect** — AI's chart used `bitnami/postgresql:16.x` images. As of Aug 2025, Bitnami restructured: the `bitnami/*` namespace now only serves "latest" for paying customers; pinned versions moved to `bitnamilegacy/*`. I had to manually patch values.yaml with:
   ```yaml
   postgresql:
     global:
       security:
         allowInsecureImages: true
     image:
       repository: bitnamilegacy/postgresql
       tag: 16.4.0-debian-12-r14
   ```
   AI's training cutoff didn't include this. I diagnosed it from the `ImagePullBackOff` error during smoke testing.

3. **Collector liveness probe** — AI added an HTTP probe to a `/healthz` endpoint that didn't exist (the collector has no HTTP server initially). I removed the probe and later added a real `/healthz` via the `obs` package.

### docker-compose

AI's first compose file lacked healthchecks on Postgres, which caused the collector to crash on first start while Postgres was still initialising. I asked AI to add `pg_isready` healthcheck + `depends_on: condition: service_healthy` — fixed cleanly.

---

## OpenAPI generation

### Annotation prompt

> *"Annotate each gateway handler with swaggo tags. Path params, query params, response types, error codes."*

**What worked:** AI added `@Summary`, `@Description`, `@Param`, `@Success`, `@Failure`, `@Router` annotations to every handler in [gpu.go](../services/gateway/internal/handler/gpu.go). One pass, no edits needed.

**What didn't:** Initial `swag init` invocation failed with a "no Go files in dir" error from the `--dir` flag interpretation. I had to drop `--dir` and let swag walk from the working directory. AI helped debug by showing me the correct invocation flags.

---

## Documentation

### Walkthrough prompt

> *"Write a comprehensive system walkthrough doc that someone new to the project can read end-to-end. Cover the data flow stage-by-stage with file:line references. Explain the MQ broker in depth. Use ASCII diagrams."*

**What worked:** AI produced [SYSTEM_WALKTHROUGH.md](SYSTEM_WALKTHROUGH.md) (~700 lines) with three ASCII diagrams, an end-to-end-data-flow walkthrough with 8 stages, and a deep-dive on the broker. The file:line references all worked when clicked.

**Edits I made:** Added the three "design decisions" section that mapped back to the interviewer's responses; expanded the rebalance section after I rewrote that protocol; corrected one diagram where the arrow direction was wrong.

### Helm runbook prompt

> *"Write a Helm install runbook that goes from zero to working API. Include prerequisite installs, kind cluster setup, build OR pull options, smoke tests with expected JSON outputs, troubleshooting common failures."*

**What worked:** AI produced the entire runbook in one shot. The prerequisites table, the smoke test commands, the troubleshooting section with seven common failure modes — all came from AI and were accurate enough that I only had to update one part (the image-pull-from-Hub option) when I switched to published images.

---

## What I would NOT have used AI for if I had to do it again

- **Naming things in the API.** AI's first cut used `gpuId` for the path parameter. I'd already decided the parameter should be UUID; the rename was a manual sweep.
- **Bitnami image debugging.** Out-of-date training data made AI confident the standard `bitnami/postgresql:16.0.0` was correct. I lost ~20 minutes before suspecting the image namespace itself.
- **The rebalance algorithm.** AI's first cut was naïve. I needed to spot the thrashing in tests before I could prompt for a fix. Once I knew what to ask for, AI delivered.
- **Project-management decisions.** Things like "which Day-1 task should I pick first" are not what AI is for. I made all those calls.

## What AI was uniquely valuable for

- **Boilerplate at scale.** 13 Helm templates, 4 Dockerfiles, 6 test files — would have taken me 2 days. AI did it in one session.
- **Spec walkthrough.** When I pasted the take-home PDF, AI extracted the 8 deliverables and the 5 success criteria in seconds.
- **Cross-cutting consistency.** AI noticed that I'd named one config field `MQAddress` in one service and `MqAddress` in another and flagged it. A human reviewer would miss that on a first pass.
- **Documentation.** Writing prose at the level required by the spec ("comprehensive README with architecture writeup, build instructions, installation workflow, sample workflow") is exactly where AI shines. The walkthroughs and this doc are AI-drafted, manually edited.

## Net assessment

If I had to estimate: **~25 productive hours invested**, of which **~12 hours saved by AI** vs. equivalent manual work. The AI is excellent at "do you have a function for X" and "scaffold a Helm chart that does Y", weaker at "debug why the rebalance is thrashing." The two complement each other well when you treat AI as a fast-typing collaborator rather than a senior engineer making decisions for you.

The single most valuable AI prompt pattern across the entire project was:

> *"Here's the symptom: <observed behaviour>. Walk me through the call path and tell me where this could go wrong."*

That format gets you actual diagnostic value out of AI rather than just generated code. Most of the bugs I caught (rebalance thrashing, double-rebalance on Leave, the partition-cursor stall after ring overflow) were found this way.

---

## Prompts archived

This is a non-exhaustive list of high-leverage prompts used during development, in roughly chronological order:

1. *Design the four-service split and a phased delivery plan.*
2. *Generate the directory tree for all four services with go.mod, main.go scaffolding, and internal packages.*
3. *Draft the proto definitions for the telemetry payload and the MQ protocol. Use proto3, document each field.*
4. *Walk me through how a single CSV row reaches the database. I want stage-by-stage with file:line references.*
5. *Show me the WAL binary format you'd use. I want torn-write recovery.*
6. *Implement the broker's ring-buffer-per-partition with WAL durability before in-memory update.*
7. *Design the consumer group: members map, assignments map, committed-offset map. Round-robin assignment.*
8. *[bug report] Two collectors are evicting each other in a loop. What's wrong with this rebalance protocol?*
9. *Add a grace-period leave so a Subscribe handler that exits due to rebalance doesn't immediately trigger a Leave.*
10. *Generate four multi-stage Dockerfiles. Build context is repo root. Distroless runtime.*
11. *Generate a Helm umbrella chart for the four services + Bitnami Postgres. Streamer is a StatefulSet.*
12. *Annotate every gateway handler with swaggo tags so `swag init` produces a useful OpenAPI spec.*
13. *Write a comprehensive system walkthrough doc covering the data flow, the broker internals, and the schema.*
14. *Write a Helm install runbook with prerequisites, smoke tests, troubleshooting.*
15. *Write unit tests for the broker package. Cover WAL replay, ring buffer wrap, stable rebalance, restart replay.*
16. *Add bufconn-based tests for the gRPC service handlers. End-to-end Publish + Subscribe round-trip.*

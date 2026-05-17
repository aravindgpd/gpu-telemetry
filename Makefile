## =============================================================================
## GPU Telemetry Pipeline — Makefile
## =============================================================================
## Usage: make <target>
## Run `make help` to list all available targets.
## =============================================================================

SHELL   := /bin/bash
BINDIR  := bin
GOFLAGS :=

# Service binary names
SERVICES := messagequeue streamer collector gateway

## ── Code Generation ──────────────────────────────────────────────────────────

.PHONY: proto
proto: ## Generate Go code from proto/mq/mq.proto and proto/telemetry/telemetry.proto via buf
	@which buf > /dev/null || (echo "ERROR: buf not found. Install via 'go install github.com/bufbuild/buf/cmd/buf@latest'"; exit 1)
	buf generate
	@echo "Proto generation complete."

.PHONY: proto-lint
proto-lint: ## Lint .proto files
	@which buf > /dev/null || (echo "ERROR: buf not found"; exit 1)
	buf lint

.PHONY: openapi
openapi: ## Generate OpenAPI spec from swaggo annotations → api/swagger.{yaml,json}
	@which swag > /dev/null || go install github.com/swaggo/swag/cmd/swag@latest
	mkdir -p api
	cd services/gateway && swag init \
		-g main.go \
		-d ./cmd/server,./internal/handler,./internal/store \
		-o ../../api \
		--outputTypes yaml,json
	@echo "OpenAPI spec written to api/swagger.yaml + api/swagger.json"

## ── Build ─────────────────────────────────────────────────────────────────────

.PHONY: build
build: $(addprefix build-,$(SERVICES)) ## Build all service binaries

.PHONY: build-messagequeue
build-messagequeue: ## Build the message queue broker binary
	mkdir -p $(BINDIR)
	go build $(GOFLAGS) -o $(BINDIR)/messagequeue ./services/messagequeue/cmd/server

.PHONY: build-streamer
build-streamer: ## Build the telemetry streamer binary
	mkdir -p $(BINDIR)
	go build $(GOFLAGS) -o $(BINDIR)/streamer ./services/streamer/cmd/server

.PHONY: build-collector
build-collector: ## Build the telemetry collector binary
	mkdir -p $(BINDIR)
	go build $(GOFLAGS) -o $(BINDIR)/collector ./services/collector/cmd/server

.PHONY: build-gateway
build-gateway: ## Build the API gateway binary
	mkdir -p $(BINDIR)
	go build $(GOFLAGS) -o $(BINDIR)/gateway ./services/gateway/cmd/server

## ── Docker ───────────────────────────────────────────────────────────────────

.PHONY: docker-build
docker-build: ## Build all Docker images (tagged :dev)
	docker build -t gpu-telemetry/messagequeue:dev -f services/messagequeue/Dockerfile .
	docker build -t gpu-telemetry/streamer:dev     -f services/streamer/Dockerfile .
	docker build -t gpu-telemetry/collector:dev    -f services/collector/Dockerfile .
	docker build -t gpu-telemetry/gateway:dev      -f services/gateway/Dockerfile .

.PHONY: docker-push
docker-push: ## Push Docker images (set REGISTRY env var)
	docker push $(REGISTRY)/gpu-telemetry/messagequeue:$(TAG)
	docker push $(REGISTRY)/gpu-telemetry/streamer:$(TAG)
	docker push $(REGISTRY)/gpu-telemetry/collector:$(TAG)
	docker push $(REGISTRY)/gpu-telemetry/gateway:$(TAG)

## ── Testing ──────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run all unit tests
	go test ./services/messagequeue/... ./services/streamer/... ./services/collector/... ./services/gateway/... -v -timeout 120s

.PHONY: test-mq
test-mq: ## Run message queue unit tests
	go test ./services/messagequeue/... -v -timeout 60s -count=1

.PHONY: test-streamer
test-streamer: ## Run streamer unit tests
	go test ./services/streamer/... -v -timeout 30s -count=1

.PHONY: test-collector
test-collector: ## Run collector unit tests (requires Docker for PostgreSQL)
	go test ./services/collector/... -v -timeout 60s -count=1

.PHONY: test-gateway
test-gateway: ## Run gateway unit tests
	go test ./services/gateway/... -v -timeout 30s -count=1

.PHONY: test-system
test-system: ## Run system/integration tests (requires Docker)
	go test ./tests/system/... -v -timeout 300s -tags=integration -count=1

## ── Coverage ─────────────────────────────────────────────────────────────────

.PHONY: coverage
coverage: ## Generate HTML coverage report and print total coverage
	go test \
		./services/messagequeue/... \
		./services/streamer/... \
		./services/collector/... \
		./services/gateway/... \
		-coverprofile=coverage.out \
		-covermode=atomic \
		-timeout 120s
	go tool cover -html=coverage.out -o coverage.html
	@echo ""
	@echo "── Coverage Summary ──────────────────────────────────────────"
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report: coverage.html"

.PHONY: coverage-check
coverage-check: coverage ## Fail the build if total coverage is below 80%
	@TOTAL=$$(go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | tr -d '%'); \
	echo "Total coverage: $$TOTAL%"; \
	if [ $$(echo "$$TOTAL < 80" | bc -l) -eq 1 ]; then \
		echo "FAIL: coverage $$TOTAL% is below the 80% threshold"; exit 1; \
	fi
	@echo "PASS: coverage meets the 80% threshold"

## ── Local Dev Stack ──────────────────────────────────────────────────────────

.PHONY: up
up: ## Start the full stack with Docker Compose
	docker compose -f docker-compose.yml up --build -d
	@echo "Stack is up. Swagger UI: http://localhost:8080/swagger/"

.PHONY: down
down: ## Stop and remove all Docker Compose resources
	docker compose -f docker-compose.yml down -v

.PHONY: logs
logs: ## Tail logs from all services
	docker compose -f docker-compose.yml logs -f

.PHONY: smoke-test
smoke-test: ## Quick API sanity check against the local stack
	@echo "── Health check ──────────────────────────────────────────────"
	@curl -sf http://localhost:8080/healthz | python3 -m json.tool
	@echo ""
	@echo "── GPU list ──────────────────────────────────────────────────"
	@curl -sf http://localhost:8080/api/v1/gpus | python3 -m json.tool

## ── Kubernetes / Helm ────────────────────────────────────────────────────────

.PHONY: helm-deps
helm-deps: ## Update Helm chart dependencies
	helm dependency update deploy/helm/gpu-telemetry

.PHONY: helm-lint
helm-lint: ## Lint the Helm chart
	helm lint deploy/helm/gpu-telemetry

.PHONY: helm-template
helm-template: ## Dry-run: render all Helm templates to stdout
	helm template gpu-telemetry deploy/helm/gpu-telemetry

.PHONY: helm-install
helm-install: helm-deps ## Install (or upgrade) the chart to the current kubectl context
	helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
		--namespace gpu-telemetry \
		--create-namespace \
		--values deploy/helm/gpu-telemetry/values.yaml \
		--wait \
		--timeout 5m

.PHONY: helm-test
helm-test: ## Run Helm chart tests (connection test pod)
	helm test gpu-telemetry --namespace gpu-telemetry

.PHONY: helm-uninstall
helm-uninstall: ## Uninstall the Helm release
	helm uninstall gpu-telemetry --namespace gpu-telemetry

## ── Code Quality ─────────────────────────────────────────────────────────────

.PHONY: lint
lint: ## Run golangci-lint (install if missing)
	@which golangci-lint > /dev/null || (curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin)
	golangci-lint run ./...

.PHONY: vet
vet: ## Run go vet on all packages
	go vet ./...

.PHONY: tidy
tidy: ## Tidy go.sum files for all modules
	go work sync
	cd proto                  && go mod tidy
	cd services/messagequeue  && go mod tidy
	cd services/streamer      && go mod tidy
	cd services/collector     && go mod tidy
	cd services/gateway       && go mod tidy

## ── Help ─────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "GPU Telemetry Pipeline — available make targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""

.DEFAULT_GOAL := help

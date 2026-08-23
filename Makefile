.DEFAULT_GOAL := build
.PHONY: build test test-race benchmark perf-test csharp csharp-pack python python-pack image image-tools image-python-probe compose-up compose-down smoke validate-local helm-lint helm-render helm-test helm-install binary-curve clean

BIN := bin
GOCACHE ?= $(CURDIR)/.cache/go-build
DOTNET_CLI_HOME ?= $(CURDIR)/.cache/dotnet
NUGET_PACKAGES ?= $(CURDIR)/.cache/nuget
export GOCACHE DOTNET_CLI_HOME NUGET_PACKAGES

build:
	mkdir -p $(BIN)
	CGO_ENABLED=0 go build -trimpath -o $(BIN)/spruce ./cmd/spruce
	go build -trimpath -o $(BIN)/spruce-producer ./cmd/spruce-producer
	go build -trimpath -o $(BIN)/spruce-consumer ./cmd/spruce-consumer
	go build -trimpath -o $(BIN)/spruce-bench ./cmd/spruce-bench
	go build -trimpath -o $(BIN)/spruce-integration ./cmd/spruce-integration
	go build -trimpath -o $(BIN)/spruce-binary-soak ./cmd/spruce-binary-soak

binary-curve: build
	sh scripts/k3s-binary-curve.sh

test:
	go test ./...

test-race:
	go test -race ./...

benchmark:
	go run ./cmd/spruce-bench -server "$${SPRUCE_URL:-http://localhost:8080}"
	go test -run '^$$' -bench '^(BenchmarkPublishBatch|BenchmarkConcurrentPublishBatch64|BenchmarkCachePutBatch512|BenchmarkWriteFrame|BenchmarkDeliverConsumerGroup|BenchmarkPublishDeliverAck256|BenchmarkWritePeerBatch512|BenchmarkWritePeerBatch512Cold|BenchmarkNextID)$$' -benchmem ./internal/broker

perf-test:
	go test -run '^TestPerformance' -count=1 ./internal/broker
	go test -run '^$$' -bench '^(BenchmarkPublishBatch|BenchmarkConcurrentPublishBatch64|BenchmarkCachePutBatch512|BenchmarkWriteFrame|BenchmarkDeliverConsumerGroup|BenchmarkPublishDeliverAck256|BenchmarkWritePeerBatch512|BenchmarkWritePeerBatch512Cold|BenchmarkNextID)$$' -benchtime=10x ./internal/broker

csharp:
	dotnet build clients/csharp/Spruce.Example/Spruce.Example.csproj --ignore-failed-sources
	dotnet build clients/csharp/Spruce.Conformance/Spruce.Conformance.csproj --ignore-failed-sources
	DOTNET_ROLL_FORWARD=Major dotnet run --project clients/csharp/Spruce.Conformance/Spruce.Conformance.csproj --no-build
	DOTNET_ROLL_FORWARD=Major dotnet run --project clients/csharp/Spruce.BatcherConformance/Spruce.BatcherConformance.csproj

csharp-pack:
	dotnet pack clients/csharp/Spruce/Spruce.csproj -c Release -o $(BIN)

python:
	PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -p 'test_*.py' -v

python-pack:
	python3 -m build --outdir $(BIN) clients/python

image:
	docker build -t spruce:dev .

image-tools:
	docker build --target tools -t spruce:tools .

image-python-probe:
	docker build --target python-probe -t spruce:python-probe .

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down --remove-orphans

smoke:
	sh scripts/smoke.sh

validate-local: build csharp python
	sh scripts/validate-local.sh

helm-lint:
	helm lint deploy/helm/spruce --set image.repository=spruce --set image.tag=dev --set image.pullPolicy=Never --set tls.allowInsecureTransport=true --set gateway.allowInsecureClientTransport=true

helm-render:
	helm template spruce deploy/helm/spruce --set image.repository=spruce --set image.tag=dev --set image.pullPolicy=Never --set tls.allowInsecureTransport=true --set gateway.allowInsecureClientTransport=true

helm-test:
	sh scripts/test-helm-validation.sh

helm-install:
	helm upgrade --install spruce deploy/helm/spruce --namespace spruce --create-namespace --set image.repository=spruce --set image.tag=dev --set image.pullPolicy=Never --set tls.allowInsecureTransport=true --set gateway.allowInsecureClientTransport=true

clean:
	rm -rf $(BIN) .cache clients/csharp/*/bin clients/csharp/*/obj clients/python/build clients/python/src/*.egg-info

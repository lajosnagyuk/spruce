.DEFAULT_GOAL := build
.PHONY: build test test-race benchmark csharp csharp-pack image compose-up compose-down smoke validate-local k8s-render k8s-apply clean

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

test:
	go test ./...

test-race:
	go test -race ./...

benchmark:
	go run ./cmd/spruce-bench -server "$${SPRUCE_URL:-http://localhost:8080}"
	go test -run '^$$' -bench BenchmarkPublishBatch -benchmem ./internal/broker

csharp:
	dotnet build clients/csharp/Spruce.Example/Spruce.Example.csproj --ignore-failed-sources
	dotnet build clients/csharp/Spruce.Conformance/Spruce.Conformance.csproj --ignore-failed-sources
	dotnet run --project clients/csharp/Spruce.Conformance/Spruce.Conformance.csproj --no-build

csharp-pack:
	dotnet pack clients/csharp/Spruce/Spruce.csproj -c Release -o $(BIN)

image:
	docker build -t spruce:dev .

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down --remove-orphans

smoke:
	sh scripts/smoke.sh

validate-local: build csharp
	sh scripts/validate-local.sh

k8s-render:
	kubectl kustomize deploy/kubernetes

k8s-apply:
	kubectl apply -k deploy/kubernetes

clean:
	rm -rf $(BIN) .cache clients/csharp/*/bin clients/csharp/*/obj

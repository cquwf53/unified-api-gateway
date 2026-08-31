SHELL := /bin/sh

BINARY ?= unified-api-gateway
CONFIG ?= configs/gateway.yaml
GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
ADMIN_URL ?= http://127.0.0.1:8781/readyz
LDFLAGS := -s -w -X unified-api-gateway/internal/server.BuildVersion=$(VERSION)

.PHONY: all build run validate test test-race coverage fuzz integration fmt vet clean docker docker-smoke health

all: test build

build:
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BINARY) ./cmd/gateway

run:
	$(GO) run ./cmd/gateway --config $(CONFIG)

validate:
	$(GO) run ./cmd/gateway validate --config $(CONFIG)

health:
	$(GO) run ./cmd/gateway health --admin-url $(ADMIN_URL)

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

coverage:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out

fuzz:
	$(GO) test ./internal/adapter -run=^$$ -fuzz=FuzzSSEParser -fuzztime=5s
	$(GO) test ./internal/config -run=^$$ -fuzz=FuzzParseConfig -fuzztime=5s

integration:
	$(GO) test ./internal/server ./cmd/gateway -count=1

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

docker:
	docker build --build-arg VERSION=$(VERSION) -t $(BINARY):$(VERSION) .

docker-smoke: docker
	docker run --rm --name $(BINARY)-smoke -d -p 127.0.0.1:18780:8780 -p 127.0.0.1:18781:8781 $(BINARY):$(VERSION)
	@trap 'docker rm -f $(BINARY)-smoke >/dev/null 2>&1 || true' EXIT; sleep 3; curl -fsS http://127.0.0.1:18781/readyz

clean:
	rm -f $(BINARY) $(BINARY).exe

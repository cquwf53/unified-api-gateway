SHELL := /bin/sh

BINARY ?= unified-api-gateway
CONFIG ?= configs/gateway.yaml
GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
ADMIN_URL ?= http://127.0.0.1:8781/readyz
LDFLAGS := -s -w -X unified-api-gateway/internal/server.BuildVersion=$(VERSION)

.PHONY: all build run validate test test-race fmt vet clean docker health

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

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

docker:
	docker build --build-arg VERSION=$(VERSION) -t $(BINARY):$(VERSION) .

clean:
	rm -f $(BINARY) $(BINARY).exe

SHELL := /bin/sh

BINARY ?= unified-api-gateway
CONFIG ?= configs/gateway.yaml
GO ?= go

.PHONY: all build run validate test fmt vet clean docker

all: test build

build:
	$(GO) build -trimpath -o $(BINARY) ./cmd/gateway

run:
	$(GO) run ./cmd/gateway --config $(CONFIG)

validate:
	$(GO) run ./cmd/gateway validate --config $(CONFIG)

test:
	$(GO) test ./...

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

docker:
	docker build -t $(BINARY):dev .

clean:
	rm -f $(BINARY) $(BINARY).exe

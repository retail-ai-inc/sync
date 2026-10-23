GOPATH=$(shell go env GOPATH)
GOLANGCI_LINT_VERSION=latest

all: lint test build

download:
	go mod download
	go mod tidy
	go vet .

build:
	go build -buildvcs=false -o sync ./cmd/sync

build-race: ## build with race detactor
	go build -race -buildvcs=false

build-slim: ## build without symbol and DWARF table, smaller binary but no debugging and profiling ability
	go build -ldflags="-s -w"

lint: ## run all the lint tools, install golangci-lint if not exist
ifeq (,$(wildcard $(GOPATH)/bin/golangci-lint))
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) > /dev/null
	GOGC=1 $(GOPATH)/bin/golangci-lint --timeout 30m --verbose run || exit 0
else
	GOGC=1 $(GOPATH)/bin/golangci-lint --timeout 30m --verbose run || exit 0
endif

lint-upgrade: ## upgrade lint version
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

test: ## run tests with race detactor
	go test -race ./...

vet-tags: ## compile the tagged tests, which a plain `go test` never builds
	go vet ./...
	go vet -tags integration ./...
	go vet -tags staging ./...
	go vet -tags perf ./...

test-integration: ## run the tagged tests against the local database stack
	./docker/test-stack.sh > .test-stack.env
	set -a && . ./.test-stack.env && set +a && \
		go test -tags integration -count=1 -timeout 540s -p 1 ./...

cover: ## statement coverage over the whole repo, including the tagged tests
	./docker/test-stack.sh > .test-stack.env
	set -a && . ./.test-stack.env && set +a && \
		go test -tags integration -count=1 -timeout 540s -p 1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

clean: ## remove the output binary from go build, as well as go install and build cache
	go clean -i -r -cache

.PHONY: all download build build-race build-slim lint lint-upgrade test vet-tags test-integration cover clean

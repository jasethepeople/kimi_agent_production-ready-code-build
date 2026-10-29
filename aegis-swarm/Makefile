# aegis-swarm — authorized security research only.
GO ?= go

.PHONY: build test vet run-demo clean

build:
	$(GO) build -o bin/orchestrator ./cmd/orchestrator
	$(GO) build -o bin/worker ./cmd/worker

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

# Full in-memory demo: orchestrator + 4 simulated workers against a synthetic
# WPA fixture with a known password.
run-demo:
	$(GO) test -race -run Integration -v ./internal/integration/

clean:
	rm -rf bin

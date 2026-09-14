GO ?= go

.PHONY: test test-fast test-race test-race-mesh soak memory-gate build-linux build-windows build-darwin site scope gateway signal clean

test:
	$(GO) test ./...

# Fast leaf packages only: seconds instead of minutes, for the inner loop.
test-fast:
	$(GO) test -count=1 ./internal/gossip ./internal/transport ./internal/crypto ./internal/bootstrap ./internal/nat

# Same packages CI runs under -race on every push (see .github/workflows/ci-dev.yml).
test-race:
	$(GO) test -race -count=1 ./internal/gossip ./internal/transport ./internal/crypto ./internal/bootstrap ./internal/nat

# The mesh package under -race: minutes, matches the nightly CI job.
test-race-mesh:
	$(GO) test -race -count=1 -timeout 3600s ./internal/mesh

# Sustained 25-node load soak. WINDOW_SEC sets how long the scenario runs
# (default 30s; CI uses minutes). Longer windows catch convergence
# degradation that short passes cannot see.
soak:
	MOSS_SOAK_WINDOW_SEC=$${SOAK_WINDOW_SEC:-180} $(GO) test -count=1 -timeout 900s -run 'TestTwentyFiveNodeLoadSoakSustainsPublishing' ./internal/mesh

# Steady-state heap regression gate: fails when the 201-node benchmark's
# heap_mb exceeds baseline + 20% (see internal/mesh/memory_benchmark_test.go).
memory-gate:
	$(GO) test -count=1 -run '^$' -bench 'BenchmarkTwoHundredPeerSteadyStateMemory' -benchtime=1x -timeout 900s ./internal/mesh

# Build the full static site (landing + explorer + showcase + docs) with Vite.
# Stages the wasm verifier and its loader into site/public, then bundles to
# site/dist.
site:
	npm install
	GOOS=js GOARCH=wasm $(GO) build -o site/public/moss.wasm ./cmd/moss-wasm
	cp "$$($(GO) env GOROOT)/lib/wasm/wasm_exec.js" site/public/wasm_exec.js
	npm run build

# Build MossScope: the site bundle goes INTO the binary, so deployment is one
# file. Order matters — go:embed reads internal/webui/dist at compile time, so
# the Vite output has to be staged there first.
scope: site
	find internal/webui/dist -mindepth 1 -maxdepth 1 ! -name '.gitignore' ! -name '.gitkeep' -exec rm -rf {} +
	cp -r site/dist/. internal/webui/dist/
	$(GO) build -o bin/moss-scope ./cmd/moss-scope

# Build the read-only telemetry gateway binary.
# DEPRECATED: superseded by `moss-scope serve`, which does the same relaying and
# telemetry and also serves the interface. Kept until deployments move over.
gateway:
	$(GO) build -o bin/moss-gateway ./cmd/moss-gateway

# Build the WebRTC signaling relay binary.
signal:
	$(GO) build -o bin/moss-signal ./cmd/moss-signal

build-linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GO) build -buildmode=c-shared -o libmoss.so ./cmd/moss-ffi

build-windows:
	CGO_ENABLED=1 GOOS=windows GOARCH=amd64 $(GO) build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi

build-darwin:
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GO) build -buildmode=c-shared -o libmoss.dylib ./cmd/moss-ffi

clean:
	rm -f libmoss.so libmoss.h libmoss.dylib moss.dll moss.h

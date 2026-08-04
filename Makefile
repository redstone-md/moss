GO ?= go

.PHONY: test build-linux build-windows build-darwin site scope gateway signal clean

test:
	$(GO) test ./...

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

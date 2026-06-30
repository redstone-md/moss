GO ?= go

.PHONY: test build-linux build-windows build-darwin site gateway signal clean

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

# Build the read-only telemetry gateway binary.
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

GO ?= go

.PHONY: test build-linux build-windows build-darwin explorer gateway signal mosh-web clean

test:
	$(GO) test ./...

# Build the WebAssembly verifier and stage the explorer's runtime support file.
explorer:
	GOOS=js GOARCH=wasm $(GO) build -o explorer/moss.wasm ./cmd/moss-wasm
	cp "$$($(GO) env GOROOT)/lib/wasm/wasm_exec.js" explorer/wasm_exec.js

# Build the read-only telemetry gateway binary.
gateway:
	$(GO) build -o bin/moss-gateway ./cmd/moss-gateway

# Build the WebRTC signaling relay binary.
signal:
	$(GO) build -o bin/moss-signal ./cmd/moss-signal

# Build the full browser node wasm + stage wasm_exec.js for mosh-web.
mosh-web:
	GOOS=js GOARCH=wasm $(GO) build -o web/mosh/moss-node.wasm ./cmd/moss-node-wasm
	cp "$$($(GO) env GOROOT)/lib/wasm/wasm_exec.js" web/mosh/wasm_exec.js

build-linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GO) build -buildmode=c-shared -o libmoss.so ./cmd/moss-ffi

build-windows:
	CGO_ENABLED=1 GOOS=windows GOARCH=amd64 $(GO) build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi

build-darwin:
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GO) build -buildmode=c-shared -o libmoss.dylib ./cmd/moss-ffi

clean:
	rm -f libmoss.so libmoss.h libmoss.dylib moss.dll moss.h

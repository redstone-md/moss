GO ?= go

.PHONY: test build-linux build-windows build-darwin clean

test:
	$(GO) test ./...

build-linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GO) build -buildmode=c-shared -o libmoss.so ./cmd/moss-ffi

build-windows:
	CGO_ENABLED=1 GOOS=windows GOARCH=amd64 $(GO) build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi

build-darwin:
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GO) build -buildmode=c-shared -o libmoss.dylib ./cmd/moss-ffi

clean:
	rm -f libmoss.so libmoss.h libmoss.dylib moss.dll moss.h

//go:build !(js && wasm)

// This package targets WebAssembly (GOOS=js GOARCH=wasm). This stub keeps a
// normal `go build ./...` from failing on other platforms.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "moss-node-wasm must be built with GOOS=js GOARCH=wasm")
	os.Exit(1)
}

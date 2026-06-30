//go:build !(js && wasm)

// This package targets WebAssembly (GOOS=js GOARCH=wasm). This stub exists so a
// normal `go build ./...` does not fail with "build constraints exclude all Go
// files" on other platforms.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "moss-wasm must be built with GOOS=js GOARCH=wasm")
	os.Exit(1)
}

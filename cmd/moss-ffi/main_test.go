package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestBuildSharedLibrary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping shared build in short mode")
	}
	outDir := t.TempDir()
	var libraryName string
	switch runtime.GOOS {
	case "windows":
		libraryName = "moss.dll"
	case "darwin":
		libraryName = "libmoss.dylib"
	default:
		libraryName = "libmoss.so"
	}
	output := filepath.Join(outDir, libraryName)
	cacheDir := filepath.Join(outDir, "gocache")
	tmpDir := filepath.Join(outDir, "gotmp")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatalf("mkdir cache failed: %v", err)
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatalf("mkdir tmp failed: %v", err)
	}
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", output, ".")
	cmd.Dir = "."
	cmd.Env = append(os.Environ(),
		"GOCACHE="+cacheDir,
		"GOTMPDIR="+tmpDir,
	)
	if outputBytes, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("shared build failed: %v\n%s", err, string(outputBytes))
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatalf("shared library missing: %v", err)
	}
	header := output[:len(output)-len(filepath.Ext(output))] + ".h"
	if _, err := os.Stat(header); err != nil {
		t.Fatalf("generated header missing: %v", err)
	}
	headerBytes, err := os.ReadFile(header)
	if err != nil {
		t.Fatalf("read generated header failed: %v", err)
	}
	if !strings.Contains(string(headerBytes), "Moss_SetScoringCallback") {
		t.Fatal("generated header is missing Moss_SetScoringCallback")
	}
}

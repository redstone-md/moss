package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"moss/internal/mesh"
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
	if !strings.Contains(string(headerBytes), "Moss_SetKeyStore") {
		t.Fatal("generated header is missing Moss_SetKeyStore")
	}
}

func TestInitNodeUsesPersistentIdentityFromKeyStore(t *testing.T) {
	previousLoad := loadIdentityBytes
	previousSave := saveIdentityBytes
	previousRegistry := registry
	previousCounter := handleCounter.Load()
	t.Cleanup(func() {
		loadIdentityBytes = previousLoad
		saveIdentityBytes = previousSave
		registry = previousRegistry
		handleCounter.Store(previousCounter)
	})
	registry = make(map[int64]*mesh.Node)
	handleCounter.Store(0)

	var stored []byte
	saveCalls := 0
	loadIdentityBytes = func() ([]byte, error) {
		if len(stored) == 0 {
			return nil, nil
		}
		return append([]byte(nil), stored...), nil
	}
	saveIdentityBytes = func(raw []byte) error {
		saveCalls++
		stored = append([]byte(nil), raw...)
		return nil
	}

	handle1 := initNode("mesh-keystore", nil, "")
	if handle1 <= 0 {
		t.Fatalf("first initNode failed: %d", handle1)
	}
	node1 := registry[handle1]
	if node1 == nil {
		t.Fatal("first node missing from registry")
	}
	pub1 := node1.PublicKey()

	handle2 := initNode("mesh-keystore", nil, "")
	if handle2 <= 0 {
		t.Fatalf("second initNode failed: %d", handle2)
	}
	node2 := registry[handle2]
	if node2 == nil {
		t.Fatal("second node missing from registry")
	}
	pub2 := node2.PublicKey()

	if pub1 != pub2 {
		t.Fatal("expected persisted keystore identity to be reused")
	}
	if saveCalls != 1 {
		t.Fatalf("expected single keystore save, got %d", saveCalls)
	}
}

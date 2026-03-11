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

func repoRoot() string {
	return filepath.Clean(filepath.Join("..", ".."))
}

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
	if !strings.Contains(string(headerBytes), "Moss_Connect") {
		t.Fatal("generated header is missing Moss_Connect")
	}
}

func TestFFIExamplesRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping example smoke tests in short mode")
	}
	if runtime.GOOS != "windows" {
		t.Skip("example smoke tests are only wired for windows in this suite")
	}

	root := repoRoot()
	buildSharedLibraryAtRoot(t, root)

	t.Run("c", func(t *testing.T) {
		runExampleCommand(t, filepath.Join(root, "examples", "c_example"), []string{
			"gcc", "-I../..", "-o", "moss_c_test.exe", "main.c", "-L../..", "-lmoss",
		})
		runExampleCommand(t, filepath.Join(root, "examples", "c_example"), []string{"cmd", "/c", "moss_c_test.exe"})
	})

	t.Run("cpp", func(t *testing.T) {
		runExampleCommand(t, filepath.Join(root, "examples", "cpp_example"), []string{
			"g++", "-I../..", "-o", "moss_cpp_test.exe", "main.cpp", "-L../..", "-lmoss",
		})
		runExampleCommand(t, filepath.Join(root, "examples", "cpp_example"), []string{"cmd", "/c", "moss_cpp_test.exe"})
	})

	t.Run("python", func(t *testing.T) {
		runExampleCommand(t, filepath.Join(root, "examples", "python_example"), []string{"python", "moss_demo.py"})
	})

	t.Run("csharp", func(t *testing.T) {
		runExampleCommand(t, filepath.Join(root, "examples", "csharp_example"), []string{"dotnet", "run", "--project", "MossDemo.csproj"})
	})

	t.Run("rust", func(t *testing.T) {
		if !hasActiveRustToolchain() {
			t.Skip("rust toolchain is not configured")
		}
		runExampleCommand(t, filepath.Join(root, "examples", "rust_example"), []string{"cargo", "run"})
	})
}

func buildSharedLibraryAtRoot(t *testing.T, root string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", "moss.dll", "./cmd/moss-ffi")
	cmd.Dir = root
	if outputBytes, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("root shared build failed: %v\n%s", err, string(outputBytes))
	}
}

func runExampleCommand(t *testing.T, dir string, args []string) {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	cmd.Env = exampleEnv(dir)
	if outputBytes, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s failed: %v\n%s", strings.Join(args, " "), err, string(outputBytes))
	}
}

func exampleEnv(dir string) []string {
	root := repoRoot()
	path := root + string(os.PathListSeparator) + os.Getenv("PATH")
	env := append([]string(nil), os.Environ()...)
	for i, entry := range env {
		if strings.HasPrefix(entry, "PATH=") {
			env[i] = "PATH=" + path
			return env
		}
	}
	return append(env, "PATH="+path)
}

func hasActiveRustToolchain() bool {
	cmd := exec.Command("rustup", "show", "active-toolchain")
	outputBytes, err := cmd.CombinedOutput()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(outputBytes)) != ""
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

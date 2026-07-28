package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// An unstamped build must say "dev" rather than claim a version it cannot know.
// The release workflow is the only thing that knows the tag, so this is the
// value a hand-built library carries — and a host can tell the two apart.
func TestUnstampedBuildReportsDev(t *testing.T) {
	if buildVersion != "dev" {
		t.Fatalf("an unstamped build reports %q; the default must stay %q so a "+
			"library built outside the release pipeline never claims a release",
			buildVersion, "dev")
	}
}

// The stamp has to survive the linker flag the release workflow passes, in a
// real c-shared build rather than a plain test binary. Without it Moss_Version
// reports "dev" for every published release and a host cannot tell which
// library it loaded — which is the whole reason the symbol exists.
func TestVersionStampReachesTheSharedLibrary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping shared build in short mode")
	}
	const stamp = "v9.9.9-stamp-probe"

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

	cmd := exec.Command("go", "build", "-buildmode=c-shared",
		"-ldflags", "-X main.buildVersion="+stamp, "-o", output, ".")
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "GOCACHE="+cacheDir, "GOTMPDIR="+tmpDir)
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("c-shared build failed: %v\n%s", err, combined)
	}

	built, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("reading the built library failed: %v", err)
	}
	if !bytes.Contains(built, []byte(stamp)) {
		t.Fatal("the linker stamp did not reach the shared library — " +
			"Moss_Version would report \"dev\" for every release")
	}
}

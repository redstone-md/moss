package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkMossPublishFFIOverhead(b *testing.B) {
	if _, err := exec.LookPath("gcc"); err != nil {
		b.Skip("gcc is required for ffi benchmark harness")
	}

	tmp := b.TempDir()
	libName, exeName := sharedLibrarySpec()
	libPath := filepath.Join(tmp, libName)
	headerPath := libPath[:len(libPath)-len(filepath.Ext(libPath))] + ".h"
	harnessPath := filepath.Join(tmp, "ffi_publish_bench.c")
	exePath := filepath.Join(tmp, exeName)

	b.StopTimer()
	buildSharedLibraryForBench(b, libPath, tmp)
	headerName := filepath.Base(headerPath)
	if err := os.WriteFile(harnessPath, []byte(ffiPublishBenchSource(headerName)), 0o644); err != nil {
		b.Fatalf("write ffi harness failed: %v", err)
	}
	buildFFIHarness(b, tmp, exePath, harnessPath)
	b.StartTimer()

	cmd := exec.Command(exePath, strconv.Itoa(b.N))
	cmd.Dir = tmp
	cmd.Env = ffiHarnessEnv(tmp)
	outputBytes, err := cmd.CombinedOutput()
	if err != nil {
		b.Fatalf("ffi harness failed: %v\n%s", err, string(outputBytes))
	}
	nsPerOp := parseBenchMetric(b, string(outputBytes), "ns_per_op=")
	b.ReportMetric(nsPerOp, "ffi_ns/op")
}

func buildSharedLibraryForBench(tb testing.TB, output, tempDir string) {
	tb.Helper()
	cacheDir := filepath.Join(tempDir, "gocache")
	tmpDir := filepath.Join(tempDir, "gotmp")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		tb.Fatalf("mkdir gocache failed: %v", err)
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		tb.Fatalf("mkdir gotmp failed: %v", err)
	}
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", output, ".")
	cmd.Dir = "."
	cmd.Env = append(os.Environ(),
		"GOCACHE="+cacheDir,
		"GOTMPDIR="+tmpDir,
	)
	if outputBytes, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("shared build failed: %v\n%s", err, string(outputBytes))
	}
}

func buildFFIHarness(tb testing.TB, dir, output, source string) {
	tb.Helper()
	args := []string{"-O2", "-I", dir, "-L", dir, "-o", output, source, "-lmoss"}
	cmd := exec.Command("gcc", args...)
	cmd.Dir = dir
	cmd.Env = ffiHarnessEnv(dir)
	if outputBytes, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("ffi harness compile failed: %v\n%s", err, string(outputBytes))
	}
}

func ffiHarnessEnv(dir string) []string {
	env := append([]string(nil), os.Environ()...)
	switch runtime.GOOS {
	case "windows":
		return prependEnv(env, "PATH", dir)
	case "darwin":
		return prependEnv(env, "DYLD_LIBRARY_PATH", dir)
	default:
		return prependEnv(env, "LD_LIBRARY_PATH", dir)
	}
}

func prependEnv(env []string, key, value string) []string {
	for i, entry := range env {
		if strings.HasPrefix(entry, key+"=") {
			env[i] = key + "=" + value + string(os.PathListSeparator) + strings.TrimPrefix(entry, key+"=")
			return env
		}
	}
	return append(env, key+"="+value)
}

func parseBenchMetric(tb testing.TB, output, prefix string) float64 {
	tb.Helper()
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value := strings.TrimPrefix(line, prefix)
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			break
		}
		return parsed
	}
	tb.Fatalf("benchmark output missing %s: %s", prefix, output)
	return 0
}

func sharedLibrarySpec() (libraryName, executableName string) {
	switch runtime.GOOS {
	case "windows":
		return "moss.dll", "ffi_publish_bench.exe"
	case "darwin":
		return "libmoss.dylib", "ffi_publish_bench"
	default:
		return "libmoss.so", "ffi_publish_bench"
	}
}

func ffiPublishBenchSource(headerName string) string {
	return fmt.Sprintf(`#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef _WIN32
#include <windows.h>
static double now_ns(void) {
  LARGE_INTEGER freq;
  LARGE_INTEGER counter;
  QueryPerformanceFrequency(&freq);
  QueryPerformanceCounter(&counter);
  return (double)counter.QuadPart * 1000000000.0 / (double)freq.QuadPart;
}
#else
#include <time.h>
static double now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1000000000.0 + (double)ts.tv_nsec;
}
#endif

#include "%s"

int main(int argc, char** argv) {
  long iterations = 1000;
  if (argc > 1) {
    iterations = strtol(argv[1], NULL, 10);
  }
  const char* config = "{\"trackers\":[],\"gossipsub\":{\"heartbeat_ms\":250}}";
  MossHandle handle = Moss_Init("ffi-bench", NULL, config);
  if (handle <= 0) {
    fprintf(stderr, "Moss_Init failed: %%lld\n", (long long)handle);
    return 2;
  }
  int32_t code = Moss_Start(handle);
  if (code != 0) {
    fprintf(stderr, "Moss_Start failed: %%d\n", (int)code);
    return 3;
  }
  code = Moss_Subscribe(handle, "alpha");
  if (code != 0) {
    fprintf(stderr, "Moss_Subscribe failed: %%d\n", (int)code);
    return 4;
  }
  double started = now_ns();
  for (long i = 0; i < iterations; ++i) {
    code = Moss_Publish(handle, "alpha", NULL, 0);
    if (code != 0 && code != -6) {
      fprintf(stderr, "Moss_Publish failed: %%d\n", (int)code);
      return 5;
    }
  }
  double elapsed = now_ns() - started;
  code = Moss_Stop(handle);
  if (code != 0) {
    fprintf(stderr, "Moss_Stop failed: %%d\n", (int)code);
    return 6;
  }
  printf("ns_per_op=%%.2f\n", elapsed / (double)iterations);
  return 0;
}
`, headerName)
}

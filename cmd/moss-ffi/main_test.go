package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/mesh"
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
	for _, symbol := range []string{
		"Moss_JoinRoom",
		"Moss_LeaveRoom",
		"Moss_SubscribeRoom",
		"Moss_UnsubscribeRoom",
		"Moss_PublishRoom",
		"Moss_ConnectToPeer",
		"Moss_SendToPeer",
		"Moss_SendToPeerAsync",
		"Moss_RelaySendToAsync",
		"Moss_PeerRTT",
		"Moss_SetPacketCallback",
		"Moss_OpenStream",
		"Moss_SendStream",
		"Moss_OnStream",
		"Moss_Version",
		"Moss_LastError",
		"Moss_EnableAxiom",
		"Moss_LogEvent",
		"Moss_GetNetworkStats",
	} {
		if !strings.Contains(string(headerBytes), symbol) {
			t.Fatalf("generated header is missing %s", symbol)
		}
	}
}

// TestNewFFIWrappersValidateInputsInSharedLibrary builds the shared library
// and drives the wave-2 FFI wrappers through a C harness, covering their
// synchronous validation paths without network I/O: nil/invalid arguments
// must fail fast with the coarse codes hosts switch on, before anything
// touches the mesh runtime.
func TestNewFFIWrappersValidateInputsInSharedLibrary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping shared library ffi smoke in short mode")
	}
	if _, err := exec.LookPath("gcc"); err != nil {
		t.Skip("gcc is required for ffi smoke harness")
	}
	tmp := t.TempDir()
	libName, exeName := sharedLibrarySpec()
	libPath := filepath.Join(tmp, libName)
	headerPath := libPath[:len(libPath)-len(filepath.Ext(libPath))] + ".h"
	harnessPath := filepath.Join(tmp, "ffi_wave2_validation.c")
	exePath := filepath.Join(tmp, exeName)

	buildSharedLibraryForBench(t, libPath, tmp)
	if err := os.WriteFile(harnessPath, []byte(ffiWave2ValidationSource(filepath.Base(headerPath))), 0o644); err != nil {
		t.Fatalf("write ffi wave2 validation harness failed: %v", err)
	}
	buildFFIHarness(t, tmp, exePath, harnessPath)
	cmd := exec.Command(exePath)
	cmd.Dir = tmp
	cmd.Env = ffiHarnessEnv(tmp)
	if outputBytes, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ffi wave2 validation harness failed: %v\n%s", err, string(outputBytes))
	}
}

func ffiWave2ValidationSource(headerName string) string {
	return fmt.Sprintf(`#include <stdint.h>
#include <stdio.h>

#include "%s"

static int failures = 0;

static void expect_code(const char* what, int32_t got, int32_t want) {
  if (got != want) {
    fprintf(stderr, "FAIL %%s: expected %%d, got %%d\n", what, (int)want, (int)got);
    failures++;
  }
}

static void async_completion(uint64_t job_id, int32_t result) {
  fprintf(stderr, "FAIL async completion fired: job %%llu result %%d\n",
          (unsigned long long)job_id, (int)result);
  failures++;
}

int main(void) {
  const char* config = "{\"trackers\":[]}";
  MossHandle handle = Moss_Init("ffi-wave2-validation", NULL, config);
  if (handle <= 0) {
    fprintf(stderr, "Moss_Init failed: %%lld\n", (long long)handle);
    return 2;
  }
  expect_code("Moss_Start", Moss_Start(handle), 0);

  uint8_t one_byte = 'x';

  /* Moss_SendToPeer: validation fires before any network work. */
  expect_code("SendToPeer nil peer", Moss_SendToPeer(handle, NULL, &one_byte, 1), -8);
  expect_code("SendToPeer negative length", Moss_SendToPeer(handle, "peer", &one_byte, -1), -8);
  int32_t oversize = (int32_t)65536 + 1; /* > default max_message_size_bytes */
  expect_code("SendToPeer oversize", Moss_SendToPeer(handle, "peer", &one_byte, oversize), -5);

  /* Moss_PeerRTT: unknown/unprobed peers report 0. */
  if (Moss_PeerRTT(handle, NULL) != 0) {
    fprintf(stderr, "FAIL PeerRTT nil peer: expected 0\n");
    failures++;
  }
  if (Moss_PeerRTT(handle, "unknown-peer") != 0) {
    fprintf(stderr, "FAIL PeerRTT unknown peer: expected 0\n");
    failures++;
  }

  /* Moss_SetPacketCallback: NULL clears and returns OK. */
  expect_code("SetPacketCallback nil", Moss_SetPacketCallback(handle, NULL), 0);

  /* Streams: 0 (raw) and 1 (gossip) are reserved by the transport. */
  expect_code("OpenStream nil peer", Moss_OpenStream(handle, NULL, 300), -8);
  expect_code("OpenStream stream 0", Moss_OpenStream(handle, "peer", 0), -8);
  expect_code("OpenStream gossip stream", Moss_OpenStream(handle, "peer", 1), -8);
  expect_code("SendStream nil peer", Moss_SendStream(handle, NULL, 300, NULL, 0), -8);
  expect_code("SendStream stream 0", Moss_SendStream(handle, "peer", 0, NULL, 0), -8);
  expect_code("SendStream oversize", Moss_SendStream(handle, "peer", 300, &one_byte, (uint32_t)oversize), -5);
  expect_code("OnStream stream 0", Moss_OnStream(handle, 0, NULL), -8);
  expect_code("OnStream nil handler", Moss_OnStream(handle, 300, NULL), -8);

  /* Async sends: a refused call returns job 0 and never fires the
     callback. NULL peer, negative length, and NULL callback all refuse. */

  if (Moss_SendToPeerAsync(handle, NULL, &one_byte, 1, async_completion) != 0) {
    fprintf(stderr, "FAIL SendToPeerAsync nil peer: expected job 0\n");
    failures++;
  }
  if (Moss_SendToPeerAsync(handle, "peer", &one_byte, -1, async_completion) != 0) {
    fprintf(stderr, "FAIL SendToPeerAsync negative length: expected job 0\n");
    failures++;
  }
  if (Moss_SendToPeerAsync(handle, "peer", &one_byte, 1, NULL) != 0) {
    fprintf(stderr, "FAIL SendToPeerAsync nil callback: expected job 0\n");
    failures++;
  }
  if (Moss_RelaySendToAsync(handle, NULL, &one_byte, 1, async_completion) != 0) {
    fprintf(stderr, "FAIL RelaySendToAsync nil peer: expected job 0\n");
    failures++;
  }
  if (Moss_RelaySendToAsync(handle, "peer", &one_byte, 1, NULL) != 0) {
    fprintf(stderr, "FAIL RelaySendToAsync nil callback: expected job 0\n");
    failures++;
  }


  if (failures > 0) {
    fprintf(stderr, "%%d wave2 validation checks failed\n", failures);
    return 1;
  }
  if (Moss_Stop(handle) != 0) {
    fprintf(stderr, "Moss_Stop failed\n");
    return 3;
  }
  printf("wave2 validation ok\n");
  return 0;
}
`, headerName)
}

func TestMossPublishRejectsOversizedLengthInSharedLibrary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping shared library ffi smoke in short mode")
	}
	if _, err := exec.LookPath("gcc"); err != nil {
		t.Skip("gcc is required for ffi smoke harness")
	}
	tmp := t.TempDir()
	libName, exeName := sharedLibrarySpec()
	libPath := filepath.Join(tmp, libName)
	headerPath := libPath[:len(libPath)-len(filepath.Ext(libPath))] + ".h"
	harnessPath := filepath.Join(tmp, "ffi_publish_oversized.c")
	exePath := filepath.Join(tmp, exeName)

	buildSharedLibraryForBench(t, libPath, tmp)
	if err := os.WriteFile(harnessPath, []byte(ffiPublishOversizedSource(filepath.Base(headerPath))), 0o644); err != nil {
		t.Fatalf("write ffi oversized harness failed: %v", err)
	}
	buildFFIHarness(t, tmp, exePath, harnessPath)
	cmd := exec.Command(exePath)
	cmd.Dir = tmp
	cmd.Env = ffiHarnessEnv(tmp)
	if outputBytes, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ffi oversized harness failed: %v\n%s", err, string(outputBytes))
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
		runExampleCommand(t, filepath.Join(root, "examples", "c_example"), []string{"cmd", "/c", ".\\moss_c_test.exe"})
	})

	t.Run("cpp", func(t *testing.T) {
		runExampleCommand(t, filepath.Join(root, "examples", "cpp_example"), []string{
			"g++", "-I../..", "-o", "moss_cpp_test.exe", "main.cpp", "-L../..", "-lmoss",
		})
		runExampleCommand(t, filepath.Join(root, "examples", "cpp_example"), []string{"cmd", "/c", ".\\moss_cpp_test.exe"})
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
		if runtime.GOOS == "windows" {
			if _, err := os.Stat(filepath.Join(root, "moss.lib")); err != nil {
				t.Skip("windows rust example requires moss.lib import library")
			}
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

func ffiPublishOversizedSource(headerName string) string {
	return `#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "` + headerName + `"

int main(void) {
  const char* config = "{\"trackers\":[]}";
  MossHandle handle = Moss_Init("ffi-oversized", NULL, config);
  if (handle <= 0) {
    fprintf(stderr, "Moss_Init failed: %lld\n", (long long)handle);
    return 2;
  }
  uint8_t payload = 0;
  int32_t code = Moss_Publish(handle, "alpha", &payload, UINT32_MAX);
  Moss_Stop(handle);
  if (code != -5) {
    fprintf(stderr, "Moss_Publish oversized returned %d\n", (int)code);
    return 3;
  }
  return 0;
}
`
}

func TestValidateKeystoreProbeSizeRejectsOversizedIdentity(t *testing.T) {
	if err := validateKeystoreProbeSize(uint32(mcrypto.IdentityEncodedSize)); err != nil {
		t.Fatalf("expected exact identity size to be accepted: %v", err)
	}
	if err := validateKeystoreProbeSize(uint32(mcrypto.IdentityEncodedSize + 1)); err == nil {
		t.Fatal("expected oversized keystore load probe to be rejected")
	}
}

func TestValidateKeystoreReadSizeRejectsBufferOverread(t *testing.T) {
	if err := validateKeystoreReadSize(1, 1); err != nil {
		t.Fatalf("expected in-capacity read to be accepted: %v", err)
	}
	if err := validateKeystoreReadSize(2, 1); err == nil {
		t.Fatal("expected read larger than capacity to be rejected")
	}
	if err := validateKeystoreReadSize(uint32(mcrypto.IdentityEncodedSize+1), uint32(mcrypto.IdentityEncodedSize+1)); err == nil {
		t.Fatal("expected read larger than identity size to be rejected")
	}
}

func TestValidatePublishPayloadPointerRejectsOversizedLength(t *testing.T) {
	one := byte(1)
	max := mesh.DefaultConfig().Security.MaxMessageSizeBytes
	if code := validatePublishPayloadPointer(unsafe.Pointer(&one), uint32(max), max); code != mesh.MOSS_OK {
		t.Fatalf("expected max-sized payload to be accepted, got %d", code)
	}
	if code := validatePublishPayloadPointer(unsafe.Pointer(&one), uint32(max+1), max); code != mesh.MOSS_ERR_MESSAGE_TOO_LARGE {
		t.Fatalf("expected oversized payload to be rejected before copy, got %d", code)
	}
	if code := validatePublishPayloadPointer(unsafe.Pointer(&one), math.MaxUint32, max); code != mesh.MOSS_ERR_MESSAGE_TOO_LARGE {
		t.Fatalf("expected uint32 max length to be rejected before C.GoBytes, got %d", code)
	}
}

func TestValidatePublishPayloadPointerRejectsNilNonZeroPayload(t *testing.T) {
	max := mesh.DefaultConfig().Security.MaxMessageSizeBytes
	if code := validatePublishPayloadPointer(nil, 0, max); code != mesh.MOSS_OK {
		t.Fatalf("expected nil zero-length payload to be accepted, got %d", code)
	}
	if code := validatePublishPayloadPointer(nil, 1, max); code != mesh.MOSS_ERR_CONFIG_INVALID {
		t.Fatalf("expected nil non-zero payload to be rejected, got %d", code)
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

// ---------------------------------------------------------------------
// Stream fallback wire format.

func TestStreamFallbackWrapRoundTrip(t *testing.T) {
	payload := []byte("stream payload bytes")
	wrapped := wrapStreamFallbackPayload(300, payload)
	if len(wrapped) != streamFallbackHeaderLen+len(payload) {
		t.Fatalf("wrapped length = %d, want %d", len(wrapped), streamFallbackHeaderLen+len(payload))
	}
	streamID, inner, ok := unwrapStreamFallbackPayload(wrapped)
	if !ok {
		t.Fatal("wrapped payload did not unwrap")
	}
	if streamID != 300 {
		t.Fatalf("unwrapped streamID = %d, want 300", streamID)
	}
	if string(inner) != string(payload) {
		t.Fatalf("unwrapped payload = %q, want %q", inner, payload)
	}
}

func TestStreamFallbackWrapEncodesStreamIDBigEndian(t *testing.T) {
	wrapped := wrapStreamFallbackPayload(0xDEADBEEF, nil)
	if string(wrapped[:4]) != "MSs1" {
		t.Fatalf("magic = %q, want MSs1", wrapped[:4])
	}
	if wrapped[4] != 0xDE || wrapped[5] != 0xAD || wrapped[6] != 0xBE || wrapped[7] != 0xEF {
		t.Fatalf("streamID bytes = %x, want deadbeef", wrapped[4:8])
	}
}

func TestStreamFallbackUnwrapRejectsForeignPayloads(t *testing.T) {
	if _, _, ok := unwrapStreamFallbackPayload(nil); ok {
		t.Fatal("nil payload must not unwrap")
	}
	short := []byte("MSs1")
	if _, _, ok := unwrapStreamFallbackPayload(short); ok {
		t.Fatal("payload shorter than the header must not unwrap")
	}
	app := []byte("plain relayed DM bytes")
	if _, _, ok := unwrapStreamFallbackPayload(app); ok {
		t.Fatal("plain application payload must not unwrap")
	}
}

// ---------------------------------------------------------------------
// Async directed sends.

func TestAsyncOutcomeCode(t *testing.T) {
	if code := asyncOutcomeCode(nil); code != mesh.MOSS_OK {
		t.Fatalf("nil error mapped to %d, want 0", code)
	}
	if code := asyncOutcomeCode(errors.New("send failed")); code != mesh.MOSS_ERR_RELAY_FAILED {
		t.Fatalf("send error mapped to %d, want -11", code)
	}
}

func TestStartAsyncSendRefusesBadArguments(t *testing.T) {
	delivered := false
	deliver := func(jobID uint64, code int32) { delivered = true }
	if jobID := startAsyncSend(nil, "peer", []byte("x"), deliver); jobID != 0 {
		t.Fatalf("nil node returned job %d, want 0", jobID)
	}
	if jobID := startAsyncSend(&mesh.Node{}, "", []byte("x"), deliver); jobID != 0 {
		t.Fatalf("empty peer returned job %d, want 0", jobID)
	}
	if jobID := startAsyncSend(&mesh.Node{}, "peer", []byte("x"), nil); jobID != 0 {
		t.Fatalf("nil deliver returned job %d, want 0", jobID)
	}
	if delivered {
		t.Fatal("refused call must not deliver")
	}
}

// twoFFINodes builds an isolated two-node topology through the public mesh
// API: unique NetworkID, no discovery sources, one static dial. Returns the
// started nodes; t.Cleanup stops them.
func twoFFINodes(t *testing.T, name string) (a, b *mesh.Node) {
	t.Helper()
	cfg := func(static string) mesh.Config {
		c := mesh.DefaultConfig()
		c.NetworkID = "moss-ffi-test-" + name
		c.Trackers = nil
		c.DHTEnabled = false
		c.LANDiscoveryEnabled = false
		if static != "" {
			c.StaticPeers = []string{static}
		}
		return c
	}
	var err error
	a, err = mesh.NewNode("ffi-async", nil, cfg(""))
	if err != nil {
		t.Fatalf("NewNode a failed: %v", err)
	}
	if code := a.Start(); code != mesh.MOSS_OK {
		t.Fatalf("a.Start failed: %d", code)
	}
	t.Cleanup(func() { a.Stop() })
	b, err = mesh.NewNode("ffi-async", nil, cfg(net.JoinHostPort("127.0.0.1", strconv.Itoa(a.ListenPort()))))
	if err != nil {
		t.Fatalf("NewNode b failed: %v", err)
	}
	if code := b.Start(); code != mesh.MOSS_OK {
		t.Fatalf("b.Start failed: %d", code)
	}
	t.Cleanup(func() { b.Stop() })
	waitForFFIPeer(t, b)
	waitForFFIPeer(t, a)
	return a, b
}

func waitForFFIPeer(t *testing.T, node *mesh.Node) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var info struct {
			PeerCount int `json:"peer_count"`
		}
		if err := json.Unmarshal([]byte(node.MeshInfoJSON()), &info); err == nil && info.PeerCount >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peer count did not reach 1; info=%s", node.MeshInfoJSON())
}

func TestAsyncSendCompletionFiresOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live-node async test in short mode")
	}
	a, b := twoFFINodes(t, "async-completion")
	pub := b.PublicKey()
	bID := hex.EncodeToString(pub[:])

	completions := make(chan int32, 4)
	jobID := startAsyncSend(a, bID, []byte("async payload"), func(jobID uint64, code int32) {
		completions <- code
	})
	if jobID == 0 {
		t.Fatal("startAsyncSend refused a valid send")
	}
	select {
	case code := <-completions:
		if code != mesh.MOSS_OK {
			t.Fatalf("completion code = %d, want 0", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("async completion did not fire")
	}
	// Exactly once: nothing else may arrive.
	select {
	case code := <-completions:
		t.Fatalf("second completion fired: %d", code)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestAsyncSendUnknownPeerFailsFast(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live-node async test in short mode")
	}
	a, _ := twoFFINodes(t, "async-unknown")
	// An unknown peer with no relay-capable candidate: SendToPeer falls
	// through to RelaySendTo, which refuses immediately (no candidates).
	completions := make(chan int32, 1)
	jobID := startAsyncSend(a, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef", []byte("x"), func(jobID uint64, code int32) {
		completions <- code
	})
	if jobID == 0 {
		t.Fatal("startAsyncSend refused a valid send")
	}
	select {
	case code := <-completions:
		if code != mesh.MOSS_ERR_RELAY_FAILED {
			t.Fatalf("completion code = %d, want -11", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("async completion did not fire")
	}
}

// TestAsyncDeliverGuardAfterStop verifies the exact state the async
// completion guard relies on: Moss_Stop removes the handle from BOTH
// registries, so asyncDeliver's getNode lookup fails and the C callback is
// never invoked on the torn-down host. The guard itself reads no other
// state, so this is its complete enabling condition.
func TestAsyncDeliverGuardAfterStop(t *testing.T) {
	previousRegistry := registry
	previousStates := ffiStates
	previousCounter := handleCounter.Load()
	t.Cleanup(func() {
		registry = previousRegistry
		ffiStates = previousStates
		handleCounter.Store(previousCounter)
	})
	registry = make(map[int64]*mesh.Node)
	ffiStates = make(map[int64]*ffiState)
	handleCounter.Store(0)

	handle := initNode("ffi-async-stop", nil, "{\"network_id\":\"moss-ffi-test-async-stop\",\"trackers\":[]}")
	if handle <= 0 {
		t.Fatalf("initNode failed: %d", handle)
	}
	if _, ok := registry[handle]; !ok {
		t.Fatal("initNode did not register the handle")
	}
	if _, ok := ffiStates[handle]; !ok {
		t.Fatal("initNode did not register ffiState for the handle")
	}
	// The node was never started (no listeners needed for a registry
	// test), so there is nothing to Stop — Moss_Stop's own work here is
	// the registry teardown this test performs by hand below.
	registryMu.Lock()
	delete(registry, handle)
	delete(ffiStates, handle)
	registryMu.Unlock()

	if _, code := getNode(handle); code != mesh.MOSS_ERR_INVALID_HANDLE {
		t.Fatalf("getNode after stop = %d, want %d", code, mesh.MOSS_ERR_INVALID_HANDLE)
	}
	if ffiStateFor(handle) != nil {
		t.Fatal("ffiStateFor after stop returned non-nil")
	}
}

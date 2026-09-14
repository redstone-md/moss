//go:build windows

package meshlan

import (
	"errors"
	"strings"
	"testing"
)

// TestWintunRingCapacityContract pins the two constraints wintun.h puts on
// the session ring capacity: within [128 KiB, 64 MiB] and a power of two.
// WintunStartSession rejects anything else, and this must fail at home in
// a unit test, not as a mystery adapter-open failure on real hardware.
func TestWintunRingCapacityContract(t *testing.T) {
	const (
		min = 0x20000   // WINTUN_MIN_RING_CAPACITY
		max = 0x4000000 // WINTUN_MAX_RING_CAPACITY
	)
	if wintunRingCapacity < min || wintunRingCapacity > max {
		t.Fatalf("ring capacity %#x outside wintun bounds [%#x, %#x]",
			wintunRingCapacity, min, max)
	}
	if wintunRingCapacity&(wintunRingCapacity-1) != 0 {
		t.Fatalf("ring capacity %#x is not a power of two", wintunRingCapacity)
	}
}

// TestWintunDllPathResolution pins the DLL path resolution order:
// MOSS_WINTUN_DLL wins; without it the bare name goes to the loader,
// whose search order puts the process directory first — the documented
// DLL location. The path is read per OpenTun call, so the env var needs
// no restart to take effect.
func TestWintunDllPathResolution(t *testing.T) {
	t.Setenv("MOSS_WINTUN_DLL", `C:\drivers\wintun-custom.dll`)
	if got := wintunDllPath(); !strings.HasSuffix(got, "wintun-custom.dll") {
		t.Fatalf("MOSS_WINTUN_DLL must win, got %q", got)
	}
}

// TestWintunLoadWithoutDllFailsClean pins the fast-fail shape every Windows
// host without the driver hits (CI's windows-latest runner included):
// a descriptive error naming the fix (place the DLL beside the binary /
// set MOSS_WINTUN_DLL), never a panic, and never ErrNotImplemented —
// Windows IS implemented, the driver just is not installed. Callers
// branch on this difference (ErrNotImplemented means "no fallback to
// hardware edge exists on this OS", which would be a lie here).
func TestWintunLoadWithoutDllFailsClean(t *testing.T) {
	// Point the load at a path that cannot resolve, so the failure
	// shape is exercised deterministically even on a host that HAS the
	// DLL on its search path.
	t.Setenv("MOSS_WINTUN_DLL", t.TempDir()+`\does-not-exist\wintun.dll`)

	procs, err := loadWintunCached()
	if err == nil {
		// Unreachable with the env var forced above; kept as a guard.
		t.Fatalf("load against a nonexistent path must fail, got procs=%v", procs)
	}
	if procs != nil {
		t.Fatal("loadWintunCached returned procs with an error")
	}
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("missing DLL must not be ErrNotImplemented (Windows is implemented): %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "wintun.dll") {
		t.Fatalf("error must name the DLL: %v", err)
	}
	if !strings.Contains(msg, "MOSS_WINTUN_DLL") {
		t.Fatalf("error must name the env override: %v", err)
	}
}

// TestWintunOpenTunWithoutDllFailsClean pins the OpenTun surface of the
// same host state: OpenTun("") must return the load error (not a device,
// not ErrNotImplemented, not a hang) so a moss-lan binary on a
// driver-less Windows reports the actionable message at startup.
func TestWintunOpenTunWithoutDllFailsClean(t *testing.T) {
	t.Setenv("MOSS_WINTUN_DLL", t.TempDir()+`\does-not-exist\wintun.dll`)

	iface, err := OpenTun("")
	if err == nil {
		// Unreachable with the env var forced above; kept as a guard.
		t.Fatalf("OpenTun against a missing DLL must fail, got %T", iface)
	}
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("OpenTun without the DLL must not report ErrNotImplemented: %v", err)
	}
	if iface != nil {
		t.Fatal("OpenTun returned a non-nil iface with an error")
	}
}

// TestWintunOpenTunErrorsAreBranchable re-pins, on the Windows file's own
// surface, the branchable error contract TestOpenTunErrorsAreBranchable
// pins for every OS: an over-length name is a validation error, never
// ErrNotImplemented — and unlike the no-DLL errors above, it must not
// depend on the DLL being loadable (validation precedes the OS layer).
func TestWintunOpenTunErrorsAreBranchable(t *testing.T) {
	// An impossible path keeps the DLL out of the picture entirely.
	t.Setenv("MOSS_WINTUN_DLL", t.TempDir()+`\does-not-exist\wintun.dll`)

	long := strings.Repeat("x", tunNameMax+2)
	_, err := OpenTun(long)
	if err == nil {
		t.Fatal("over-length name must be rejected")
	}
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("over-length name must be a validation error, not ErrNotImplemented: %v", err)
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("over-length name error should quote the bound: %v", err)
	}
}

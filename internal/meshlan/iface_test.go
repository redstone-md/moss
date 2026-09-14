package meshlan

import (
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/tun"
)

// OpenTun's surface against the tun.PacketIface contract. The loopback
// tests are the everywhere-portable anchor (they run on every GOOS the
// package builds for); the device tests run only where a kernel TUN is
// actually reachable — a CI container without /dev/net/tun or without
// CAP_NET_ADMIN skips them rather than failing. Darwin's utun
// implementation and Windows's wintun.dll adapter are hardware-untested
// (no mac / no Windows box in the project), so on those OSes the device
// tests skip: darwin returns its permission error, windows without
// wintun.dll returns the driver-missing error — neither reaches the
// lifecycle asserts until real hardware proves the files.

// TestLoopbackPacketContract drives tun.Loopback — the MVP edge the
// LanNode wires when no kernel TUN exists — through the PacketIface
// contract the mesh binding enforces: ReadPacket blocks until a packet
// lands (no sample-and-return-empty), packets arrive in write order,
// and Close fails both directions with net.ErrClosed while staying
// idempotent.
func TestLoopbackPacketContract(t *testing.T) {
	lb := tun.NewLoopback()
	defer lb.Close()

	go func() {
		if err := lb.WritePacket([]byte("one")); err != nil {
			t.Errorf("write 1: %v", err)
		}
		if err := lb.WritePacket([]byte("two")); err != nil {
			t.Errorf("write 2: %v", err)
		}
	}()

	got, err := lb.ReadPacket()
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if string(got) != "one" {
		t.Fatalf("packet 1 mismatch: %q", got)
	}
	got, err = lb.ReadPacket()
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if string(got) != "two" {
		t.Fatalf("packet 2 mismatch: %q", got)
	}

	if err := lb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := lb.ReadPacket(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after close: want net.ErrClosed, got %v", err)
	}
	if err := lb.WritePacket([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close: want net.ErrClosed, got %v", err)
	}
	if err := lb.Close(); err != nil {
		t.Fatalf("second close must be idempotent: %v", err)
	}
}

// TestLoopbackCloseUnblocksParkedRead pins the binding-critical property
// of the edge: a reader parked in ReadPacket when Close lands must wake
// with net.ErrClosed, not hang. The mesh binding's pump goroutine relies
// on exactly this to exit on DetachTun (mesh.AttachTun documents Close as
// the only thing that ends a parked read).
func TestLoopbackCloseUnblocksParkedRead(t *testing.T) {
	lb := tun.NewLoopback()
	defer lb.Close()

	readDone := make(chan error, 1)
	go func() {
		_, err := lb.ReadPacket()
		readDone <- err
	}()

	// Let the reader park on the waiters list.
	time.Sleep(50 * time.Millisecond)

	if err := lb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-readDone:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("parked reader woke with %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock a parked ReadPacket within 2s")
	}
}

// TestAsNetClosedMapping pins the sentinel mapping the OS files lean on:
// an *os.File reports os.ErrClosed ("file already closed") after Close,
// while the PacketIface contract (and tun.Loopback) speak net.ErrClosed.
// A closing-shaped error must map to net.ErrClosed; any other error must
// pass through unwrapped so real I/O failures stay diagnosable.
func TestAsNetClosedMapping(t *testing.T) {
	// A real closed-file error, the exact shape tun_linux/darwin hand in.
	f, err := os.CreateTemp(t.TempDir(), "moss-lan-closed")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	f.Close()
	_, readErr := f.Read(make([]byte, 4))
	if !errors.Is(readErr, os.ErrClosed) {
		t.Fatalf("setup: reading a closed file should report os.ErrClosed, got %v", readErr)
	}
	if got := asNetClosed(readErr); !errors.Is(got, net.ErrClosed) {
		t.Fatalf("closed-file error must map to net.ErrClosed, got %v", got)
	}

	// A non-closing error passes through unwrapped.
	ioErr := fmt.Errorf("write tun0: %w", os.ErrDeadlineExceeded)
	if got := asNetClosed(ioErr); !errors.Is(got, os.ErrDeadlineExceeded) {
		t.Fatalf("non-closing error must pass through, got %v", got)
	}

	// Nil stays nil.
	if got := asNetClosed(nil); got != nil {
		t.Fatalf("nil must stay nil, got %v", got)
	}
}

// tunAvailable opens a kernel TUN device, skipping the device tests when
// this host cannot provide one: a linux host without /dev/net/tun or
// CAP_NET_ADMIN, a darwin host whose utun open fails, or a windows host
// without wintun.dll beside the binary (the driver-missing error names
// the fix). Absence is an environment property, not a code defect.
func tunAvailable(t *testing.T) (tun.PacketIface, func()) {
	t.Helper()

	iface, err := OpenTun("")
	if err != nil {
		if errors.Is(err, ErrNotImplemented) {
			t.Skipf("%s: %v", runtime.GOOS, err)
		}
		t.Skipf("no kernel TUN available on this host: %v", err)
	}
	return iface, func() { _ = iface.Close() }
}

// TestOpenTunDeviceLifecycle opens a real TUN device (skipped where the
// host has none) and drives the lifecycle the binding counts on: a
// kernel interface name is reported, Close unblocks a parked reader with
// net.ErrClosed, post-close reads and writes return net.ErrClosed, and
// a second Close is a nil no-op.
func TestOpenTunDeviceLifecycle(t *testing.T) {
	iface, stop := tunAvailable(t)
	defer stop()

	ni, ok := iface.(TunIface)
	if !ok {
		t.Fatalf("OpenTun returned %T, which does not implement TunIface (no Name)", iface)
	}
	if ni.Name() == "" {
		t.Fatal("interface reported an empty kernel name")
	}
	t.Logf("opened kernel TUN interface %q", ni.Name())

	// Park a reader, then Close: the parked reader must wake with
	// net.ErrClosed. This is the exact contract DetachTun exercises
	// (Close is what ends a pump's blocked ReadPacket).
	readDone := make(chan error, 1)
	go func() {
		_, err := ni.ReadPacket()
		readDone <- err
	}()
	// Let the reader reach the kernel (poller-registered read).
	time.Sleep(50 * time.Millisecond)

	if err := ni.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-readDone:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("parked reader woke with %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock a parked ReadPacket within 2s")
	}

	// Post-close: both directions fail with net.ErrClosed.
	if _, err := ni.ReadPacket(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after close: want net.ErrClosed, got %v", err)
	}
	if err := ni.WritePacket([]byte{0x45}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close: want net.ErrClosed, got %v", err)
	}

	// Idempotent Close: the second call is a nil no-op.
	if err := ni.Close(); err != nil {
		t.Fatalf("second close must be idempotent (nil), got %v", err)
	}
}

// TestOpenTunErrorsAreBranchable pins the error shapes callers branch on.
// The over-length name must be rejected as a validation error on every
// OS — never as the "no TUN here" sentinel, which would make a caller
// wrongly conclude the OS is unsupported instead of retrying with a
// shorter name.
func TestOpenTunErrorsAreBranchable(t *testing.T) {
	longName := strings.Repeat("x", tunNameMax+2)
	_, err := OpenTun(longName)
	if err == nil {
		t.Fatal("over-length name must be rejected")
	}
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("over-length name must be a validation error, not ErrNotImplemented: %v", err)
	}
}

// TestOpenTunNoHang pins that OpenTun always completes promptly —
// success, sentinel, or fast failure — never a wedged open. A hang here
// would wedge a moss-lan binary's startup forever.
func TestOpenTunNoHang(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		iface, err := OpenTun("")
		if err == nil {
			_ = iface.Close()
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("OpenTun did not complete within 5s")
	}
}

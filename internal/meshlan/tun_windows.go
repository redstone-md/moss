//go:build windows

// meshlan tun_windows.go: the wintun.dll ring-buffer adapter, cgo-free.
//
// Windows has no kernel TUN char device. The de-facto standard — wintun.dll
// — is a userspace DLL that creates a transient L3 adapter ("Wintun") and
// hands the process a lock-free ring buffer to exchange raw IP packets with
// the network stack: receive returns a pointer straight into the ring (the
// caller must release the slot), send allocates a slot and queues it. No cgo
// anywhere: the DLL loads via LoadLibrary (windows.LoadDLL) and the procs
// are plain WinAPI calls through golang.org/x/sys/windows — the same
// calls wireguard-go makes through its wintun binding.
//
// The DLL is NOT embedded: it ships beside the moss-lan binary (the DLL is
// redistributable under MIT; wireguard's own Go tools do the same) and is
// found via the standard DLL search path, where the process directory
// comes first. An explicit MOSS_WINTUN_DLL env var overrides the path, so
// a Windows admin can point at a specific file. A missing DLL fails with a
// descriptive ERROR_MOD_NOT_FOUND error — never ErrNotImplemented: the OS
// IS implemented, the driver merely is not installed.
//
// Hardware-untested, like the darwin file before it was proven on a mac:
// the ring-session lifecycle follows wireguard-go and the wintun.h
// contract exactly, but moss has no Windows hardware to drive it on. The
// CI windows-latest runner will exercise the no-DLL failure path.
package meshlan

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/redstone-md/moss/internal/tun"
)

// wintunTunnelType is the adapter's tunnel type — an arbitrary label that
// groups Wintun adapters in some management UIs (wireguard-go passes
// "WireGuard").
const wintunTunnelType = "Moss"

// wintunRingCapacity is the send+receive ring size: 4 MiB, within the
// wintun bounds [128 KiB, 64 MiB] (must be a power of two). wireguard-go
// starts at 8 MiB; moss-lan's packets are capped at tun.DefaultMTU (1500),
// so 4 MiB holds ~2700 of them — ample burst headroom for a LAN edge at
// half the locked capacity.
const wintunRingCapacity = 0x400000

// wintunProcs is the resolved proc set of the loaded wintun.dll. Field per
// call, no map indirection on the packet path.
type wintunProcs struct {
	createAdapter  *windows.Proc
	startSession   *windows.Proc
	endSession     *windows.Proc
	readWaitEvent  *windows.Proc
	receivePacket  *windows.Proc
	releaseReceive *windows.Proc
	allocateSend   *windows.Proc
	sendPacket     *windows.Proc
	closeAdapter   *windows.Proc
}

// wintunDllPath picks the wintun.dll path for this process: MOSS_WINTUN_DLL
// wins, else the bare name goes to the loader, whose search order puts the
// process directory first — the documented location for the DLL. Read per
// OpenTun call (a getenv is nothing next to a LoadDLL), so the env var
// takes effect without restart gymnastics.
func wintunDllPath() string {
	if p := os.Getenv("MOSS_WINTUN_DLL"); p != "" {
		return p
	}
	return "wintun.dll"
}

// loadWintun loads wintun.dll and resolves all nine procs up front — before
// any adapter exists — so openTunOS fails fast with a NAMED proc if the
// DLL is an incompatible build (a missing proc would otherwise panic
// mid-lifecycle on a live adapter). The DLL stays loaded for the process
// lifetime (adapters share it); never FreeLibrary it — the procs below are
// captured in every open adapter.
func loadWintun() (*wintunProcs, error) {
	dll, err := windows.LoadDLL(wintunDllPath())
	if err != nil {
		// ERROR_ACCESS_DENIED gets the permission sentinel so callers
		// can branch on it; every other failure (most commonly the DLL
		// simply not being there) carries the how-to-fix hint.
		if d, ok := err.(*windows.DLLError); ok && d.Err == windows.ERROR_ACCESS_DENIED {
			return nil, fmt.Errorf("moss-lan: load wintun.dll: %w", os.ErrPermission)
		}
		return nil, fmt.Errorf("moss-lan: load wintun.dll (place it beside the binary or set MOSS_WINTUN_DLL; is the Wintun driver installed?): %w", err)
	}
	// Resolve every proc before returning: openTunOS then fails fast
	// with a NAMED proc on an incompatible DLL build, instead of a
	// mid-lifecycle panic on a live adapter.
	p := &wintunProcs{}
	find := func(dst **windows.Proc, name string) error {
		proc, err := dll.FindProc(name)
		if err != nil {
			return fmt.Errorf("moss-lan: wintun.dll lacks %s (incompatible DLL build?): %w", name, err)
		}
		*dst = proc
		return nil
	}
	for _, b := range []struct {
		dst  **windows.Proc
		name string
	}{
		{&p.createAdapter, "WintunCreateAdapter"},
		{&p.startSession, "WintunStartSession"},
		{&p.endSession, "WintunEndSession"},
		{&p.readWaitEvent, "WintunGetReadWaitEvent"},
		{&p.receivePacket, "WintunReceivePacket"},
		{&p.releaseReceive, "WintunReleaseReceivePacket"},
		{&p.allocateSend, "WintunAllocateSendPacket"},
		{&p.sendPacket, "WintunSendPacket"},
		{&p.closeAdapter, "WintunCloseAdapter"},
	} {
		if err := find(b.dst, b.name); err != nil {
			_ = dll.Release()
			return nil, err
		}
	}
	return p, nil
}

// wintunCache guards the process-wide proc cache. A SUCCESSFUL load is
// cached for the process lifetime; a failed load is NOT cached, so a
// user who drops wintun.dll in place (or sets MOSS_WINTUN_DLL) needs no
// process restart for the next OpenTun to pick it up.
var wintunCache struct {
	sync.Mutex
	loaded bool
	procs  *wintunProcs
}

// loadWintunCached is the entry point openTunOS uses: the first
// SUCCESSFUL load is remembered for the process; a failure is returned
// to the caller and the next call retries from scratch (env or file
// layout may have been fixed in between — moss-lan must not demand a
// restart for that).
func loadWintunCached() (*wintunProcs, error) {
	wintunCache.Lock()
	defer wintunCache.Unlock()
	if wintunCache.loaded {
		return wintunCache.procs, nil
	}
	procs, err := loadWintun()
	if err != nil {
		return nil, err
	}
	wintunCache.loaded = true
	wintunCache.procs = procs
	return procs, nil
}

// wintunTun is a wintun.dll ring-buffer adapter as a tun.PacketIface.
type wintunTun struct {
	name string
	// procs is the shared, process-lifetime proc set.
	procs *wintunProcs
	// adapter is the opaque WINTUN_ADAPTER_HANDLE.
	adapter uintptr
	// session is the opaque WINTUN_SESSION_HANDLE (the ring).
	session uintptr
	// readEvent is the session's read-wait event: signaled when ring data
	// arrives, or manually by Close to wake a parked reader. Owned by the
	// session — never CloseHandle it.
	readEvent windows.Handle
	// closed flips to true in Close BEFORE the session dies, so the
	// packet methods can check it without racing the teardown: atomic,
	// monotonically false→true.
	closed atomic.Bool
	// mu guards the teardown transition: exactly one Close performs the
	// real teardown; later Close calls are nil no-ops (idempotency per
	// the PacketIface contract).
	mu sync.Mutex
}

// openTunOS creates a wintun adapter and starts its ring session. An empty
// name requests the default "moss0" (wintun has no query-API for the
// created name, so the file assigns the name itself and keeps the request
// idempotent within the process: repeated OpenTun("") calls reuse
// "moss0", a second adapter created while "moss0" is alive lands on the
// WintunCreateAdapter failure — same-name adapters do not coexist).
// Creating an adapter requires the Wintun driver package installed; the
// load errors above distinguish "driver missing" from real failures.
func openTunOS(name string) (tun.PacketIface, string, error) {
	procs, err := loadWintunCached()
	if err != nil {
		return nil, "", err
	}
	if name == "" {
		name = "moss0"
	}
	name16, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, "", fmt.Errorf("moss-lan: interface name %q: %w", name, err)
	}
	tunnelType16, err := windows.UTF16PtrFromString(wintunTunnelType)
	if err != nil {
		return nil, "", fmt.Errorf("moss-lan: tunnel type %q: %w", wintunTunnelType, err)
	}

	// WINTUN_ADAPTER_HANDLE WintunCreateAdapter(LPCWSTR Name, LPCWSTR
	// TunnelType, const GUID *RequestedGUID); NULL + GetLastError on
	// failure. RequestedGUID nil: a fresh GUID per adapter avoids the
	// undocumented-guid complications wintun.h itself warns about.
	adapter, _, callErr := procs.createAdapter.Call(
		uintptr(unsafe.Pointer(name16)),
		uintptr(unsafe.Pointer(tunnelType16)),
		0,
	)
	if adapter == 0 {
		return nil, "", fmt.Errorf("moss-lan: WintunCreateAdapter %q (is the Wintun driver installed?): %v", name, callErr)
	}

	// WINTUN_SESSION_HANDLE WintunStartSession(Adapter, Capacity); NULL +
	// GetLastError on failure. The session is ended before the adapter is
	// closed on teardown, per the wintun.h ordering.
	session, _, callErr := procs.startSession.Call(adapter, uintptr(wintunRingCapacity))
	if session == 0 {
		_, _, _ = procs.closeAdapter.Call(adapter)
		return nil, "", fmt.Errorf("moss-lan: WintunStartSession (capacity %d): %v", wintunRingCapacity, callErr)
	}

	// HANDLE WintunGetReadWaitEvent(Session): the event a blocked reader
	// parks on. The event is created and stored by WintunStartSession,
	// so the call cannot fail (wireguard-go and the wintun bindings
	// treat it as infallible too); its result is trusted as-is.
	readEvent, _, _ := procs.readWaitEvent.Call(session)

	return &wintunTun{
		name:      name,
		procs:     procs,
		adapter:   adapter,
		session:   session,
		readEvent: windows.Handle(readEvent),
	}, name, nil
}

// Name returns the adapter name as passed to WintunCreateAdapter ("moss0"
// when the caller asked for the default).
func (t *wintunTun) Name() string { return t.name }

// ReadPacket blocks until one IP packet arrives from the ring, copies it
// out, and releases the ring slot. The returned slice is freshly
// allocated per read: ownership transfers with the return, the same
// contract tun.Loopback's copy-on-queue provides.
//
// The blocking shape: receive returns ERROR_NO_MORE_ITEMS when the ring is
// empty; the reader then parks on the session's read-wait event — signaled
// on packet arrival, or manually by Close, whose closed flag makes the
// woken reader return net.ErrClosed instead of retrying a dying session.
// That is how Close unblocks a parked ReadPacket — the property the mesh
// binding's pump goroutine relies on to exit on DetachTun.
func (t *wintunTun) ReadPacket() ([]byte, error) {
	for {
		if t.closed.Load() {
			return nil, net.ErrClosed
		}
		var size uint32
		// BYTE *WintunReceivePacket(Session, DWORD *PacketSize): NULL +
		// GetLastError on empty ring / terminating session.
		p, _, callErr := t.procs.receivePacket.Call(t.session, uintptr(unsafe.Pointer(&size)))
		if p != 0 {
			// View the ring slot as a byte slice over the DLL's memory.
			// Go's unsafeptr analyzer has no clean way to express
			// "this uintptr came from a C-style call and is a real
			// pointer": a direct unsafe.Pointer(p) conversion is
			// flagged as possible misuse (a false positive, but one
			// the analyzer cannot prove away), and the reflect-header
			// route (setting Data on a header over a real slice) is
			// the analyzer's own sanctioned pattern for exactly this
			// shape. The slot is valid from WintunReceivePacket until
			// WintunReleaseReceivePacket below — precisely the window
			// of the copy. (wireguard-go's binding does the identical
			// conversion behind its own API.)
			var ring []byte = make([]byte, size)
			sh := (*reflect.SliceHeader)(unsafe.Pointer(&ring))
			sh.Data = p
			// Copy out BEFORE releasing the slot: the slice handed to
			// the router must not alias the ring (wireguard-go has the
			// same single-copy shape).
			packet := make([]byte, size)
			copy(packet, ring)
			// VOID WintunReleaseReceivePacket(Session, const BYTE
			// *Packet): the slot is back in the ring immediately.
			t.procs.releaseReceive.Call(t.session, p)
			return packet, nil
		}
		switch callErr {
		case windows.ERROR_NO_MORE_ITEMS:
			// Ring empty: park on the read-wait event. INFINITE is safe
			// because Close signals the event before ending the
			// session, so the wait always returns eventually.
			_, _ = windows.WaitForSingleObject(t.readEvent, windows.INFINITE)
			continue
		case windows.ERROR_HANDLE_EOF:
			// Session/adapter terminating — after Close this is exactly
			// net.ErrClosed, the PacketIface contract's shape.
			return nil, net.ErrClosed
		case windows.ERROR_INVALID_DATA:
			return nil, fmt.Errorf("moss-lan: wintun receive ring corrupt")
		default:
			return nil, asNetClosed(callErr)
		}
	}
}

// WritePacket delivers one IP packet to the ring: allocate a send slot,
// copy the packet in, queue it. A full ring (ERROR_BUFFER_OVERFLOW) drops
// the packet rather than blocking — the same loss a kernel TUN queue
// applies on overflow, counted by the router as a failed delivery.
func (t *wintunTun) WritePacket(packet []byte) error {
	if t.closed.Load() {
		return net.ErrClosed
	}
	if len(packet) == 0 {
		return nil
	}
	// BYTE *WintunAllocateSendPacket(Session, DWORD PacketSize): NULL +
	// GetLastError on full ring / terminating session.
	p, _, callErr := t.procs.allocateSend.Call(t.session, uintptr(len(packet)))
	if p == 0 {
		switch callErr {
		case windows.ERROR_BUFFER_OVERFLOW:
			return fmt.Errorf("moss-lan: wintun send ring full (packet dropped)")
		case windows.ERROR_HANDLE_EOF:
			return net.ErrClosed
		default:
			return asNetClosed(callErr)
		}
	}
	// Ring-slot view, the same reflect-header route as ReadPacket: p is
	// the slot WintunAllocateSendPacket returned, valid until
	// WintunSendPacket releases it below.
	var ring []byte = make([]byte, len(packet))
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&ring))
	sh.Data = p
	copy(ring, packet)
	// VOID WintunSendPacket(Session, const BYTE *Packet): queues the slot
	// for the ring's consumer.
	t.procs.sendPacket.Call(t.session, p)
	return nil
}

// Close shuts the adapter down and unblocks any parked reader. The flag
// flips FIRST — a reader woken by the SetEvent below (or racing this call)
// sees it at the loop head and returns net.ErrClosed without touching the
// dying session — then the reader is released, the session ended, the
// adapter closed (a created adapter is REMOVED, per wintun.h). Idempotent:
// after the first call every later Close returns nil.
func (t *wintunTun) Close() error {
	t.mu.Lock()
	if t.closed.Load() {
		t.mu.Unlock()
		return nil
	}
	t.closed.Store(true)
	t.mu.Unlock()

	// Wake a parked reader; it returns net.ErrClosed at its loop head.
	if t.readEvent != 0 {
		_ = windows.SetEvent(t.readEvent)
	}
	// VOID WintunEndSession(Session), then WintunCloseAdapter(Adapter).
	_, _, _ = t.procs.endSession.Call(t.session)
	_, _, _ = t.procs.closeAdapter.Call(t.adapter)
	return nil
}

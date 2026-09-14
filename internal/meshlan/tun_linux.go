//go:build linux

// meshlan tun_linux.go: the real /dev/net/tun device, cgo-free.
//
// The kernel's TUN/TAP driver multiplexes IP packets between userspace and
// the network stack through one char device. Opening it yields an fd whose
// TUNSETIFF ioctl binds it to a transient network interface; reads and
// writes on that fd are then raw IP packet frames — exactly the shape
// tun.PacketIface wants (IFF_NO_PI: no 4-byte packet-info preamble, so the
// fd speaks plain IP; IFF_TUN excludes the ethernet header of TAP).
//
// The fd is raw and made nonblocking BEFORE os.NewFile, so the file is
// registered with Go's netpoller: Read parks in epoll (no spinning), and —
// crucially for the PacketIface contract — Close() wakes a parked Read
// with os.ErrClosed (verified against Go 1.25 and a live device).
package meshlan

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/redstone-md/moss/internal/tun"
)

// tunNameMax (in tun.go) matches unix.IFNAMSIZ-1; NewIfreq enforces the
// same bound kernel-side, so the OS file needs no second check.

// linuxTun is a kernel TUN device as a tun.PacketIface.
type linuxTun struct {
	f    *os.File
	name string
	// mu guards the closed transition only: exactly one caller performs
	// the real Close and consumes its error; late Close callers get nil
	// (idempotency per the PacketIface contract).
	mu     sync.Mutex
	closed bool
}

// openTunOS opens /dev/net/tun and attaches it to interface name. "" picks
// the kernel's next free "tunN"; a taken name yields EBUSY from TUNSETIFF.
func openTunOS(name string) (tun.PacketIface, string, error) {
	ifr, err := unix.NewIfreq(name)
	if err != nil {
		return nil, "", fmt.Errorf("moss-lan: interface name %q: %w", name, err)
	}

	// O_CLOEXEC: the device must not leak into a spawned process's fd
	// table — the interface lives exactly as long as this process.
	fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || err == unix.ENOENT {
			return nil, "", fmt.Errorf("moss-lan: /dev/net/tun does not exist (load the tun module or enable it in the container spec): %w", err)
		}
		if errors.Is(err, os.ErrPermission) || err == unix.EACCES {
			return nil, "", fmt.Errorf("moss-lan: no permission to open /dev/net/tun (needs CAP_NET_ADMIN): %w", err)
		}
		return nil, "", fmt.Errorf("moss-lan: open /dev/net/tun: %w", err)
	}

	ifr.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)
	if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, ifr); err != nil {
		unix.Close(fd)
		if errors.Is(err, os.ErrPermission) || err == unix.EPERM {
			return nil, "", fmt.Errorf("moss-lan: TUNSETIFF on %q (needs CAP_NET_ADMIN): %w", name, err)
		}
		return nil, "", fmt.Errorf("moss-lan: TUNSETIFF on %q: %w", name, err)
	}

	// Nonblocking before NewFile: this is what makes the fd pollable
	// and Close-able-with-wakeup instead of a blocking read that ties
	// up a thread forever.
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return nil, "", fmt.Errorf("moss-lan: set O_NONBLOCK on %q: %w", ifr.Name(), err)
	}

	name = ifr.Name()
	return &linuxTun{
		f:    os.NewFile(uintptr(fd), name),
		name: name,
	}, name, nil
}

// Name returns the kernel-side interface name ("tun0"...) as reported by
// TUNSETIFF — authoritative even when the caller asked for "".
func (t *linuxTun) Name() string { return t.name }

// ReadPacket blocks until one IP packet arrives from the kernel, then
// returns it. The returned slice is freshly allocated per read: the
// router does not retain it past the call, but the mesh binding's pump
// channel hands it across goroutines, so ownership transfers with the
// return (the same contract tun.Loopback's copy-on-queue provides).
func (t *linuxTun) ReadPacket() ([]byte, error) {
	buf := make([]byte, tun.DefaultMTU)
	n, err := t.f.Read(buf)
	if err != nil {
		return nil, asNetClosed(err)
	}
	return buf[:n], nil
}

// WritePacket delivers one IP packet to the kernel stack. The kernel may
// reject a packet targeting an address with no route, or when the
// interface is administratively down; both surface as errors here (EIO
// for the down state) and the router counts them as failed deliveries.
func (t *linuxTun) WritePacket(packet []byte) error {
	_, err := t.f.Write(packet)
	return asNetClosed(err)
}

// Close shuts the interface down and unblocks any parked ReadPacket. The
// kernel interface disappears with the fd (TUN devices are transient
// unless flagged IFF_PERSIST, which we do not use). Idempotent: after the
// first call every later Close returns nil.
func (t *linuxTun) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		// Swallow os.ErrClosed from the second Close: contract says
		// idempotent, the caller did nothing wrong.
		return nil
	}
	t.closed = true
	return t.f.Close()
}

//go:build windows

// meshlan tun_windows.go: TUN on Windows is a wintun.dll adapter, not a
// kernel device, and loading it means DLL loading (windows.LoadLibrary,
// ring-buffer session) — a deliberate next step, not silently skipped.
//
// Plan for the next pass (kept here so the seam is explicit):
//
//  1. embed wintun.dll (or LoadLibrary it from beside the binary) and
//     call WintunCreateAdapter via golang.org/x/sys/windows syscall
//     wrappers — cgo still not required, but the DLL must ship;
//  2. WintunStartSession returns a ring-buffer handle; ReadPacket/
//     WritePacket wrap WintunReceivePacket/WintunSendPacket (each
//     returns a pointer the caller must release);
//  3. Close is WintunEndSession + WintunCloseAdapter (idempotent).
//
// Until then OpenTun fails with ErrNotImplemented — a caller can detect
// it with errors.Is and fall back to a tun.Loopback edge or surface an
// actionable message.
package meshlan

import (
	"fmt"

	"github.com/redstone-md/moss/internal/tun"
)

// openTunOS reports the missing wintun implementation. name is echoed in
// the error text for log clarity on the only OS that reaches this file.
func openTunOS(name string) (tun.PacketIface, string, error) {
	return nil, "", fmt.Errorf("%w: Windows needs the wintun.dll adapter for %q (planned: LoadLibrary wintun.dll, ring-buffer session over WintunCreateAdapter/StartSession)", ErrNotImplemented, name)
}

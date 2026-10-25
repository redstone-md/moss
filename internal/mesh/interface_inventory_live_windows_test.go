package mesh

import (
	"net"
	"reflect"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsInventoryMatchesStandardLibrary(t *testing.T) {
	old, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	view, err := readInterfaceView()
	if err != nil || !reflect.DeepEqual(old, view.interfaces) {
		t.Fatalf("native metadata differs from net.Interfaces: %v", err)
	}
	for _, iface := range old {
		want, wantErr := iface.Addrs()
		got, gotErr := view.addresses(iface)
		if (wantErr != nil) != (gotErr != nil) || !reflect.DeepEqual(want, got) {
			t.Fatalf("interface %q addresses differ: want=%v err=%v got=%v err=%v", iface.Name, want, wantErr, got, gotErr)
		}
	}
}

func TestWindowsInventoryPreservesMediaFlags(t *testing.T) {
	for kind, want := range map[uint32]net.Flags{
		windows.IF_TYPE_ETHERNET_CSMACD:    net.FlagBroadcast | net.FlagMulticast,
		windows.IF_TYPE_ISO88025_TOKENRING: net.FlagBroadcast | net.FlagMulticast,
		windows.IF_TYPE_IEEE80211:          net.FlagBroadcast | net.FlagMulticast,
		windows.IF_TYPE_IEEE1394:           net.FlagBroadcast | net.FlagMulticast,
		windows.IF_TYPE_PPP:                net.FlagPointToPoint | net.FlagMulticast,
		windows.IF_TYPE_TUNNEL:             net.FlagPointToPoint | net.FlagMulticast,
		windows.IF_TYPE_SOFTWARE_LOOPBACK:  net.FlagLoopback | net.FlagMulticast,
		windows.IF_TYPE_ATM:                net.FlagBroadcast | net.FlagPointToPoint | net.FlagMulticast,
		windows.IF_TYPE_OTHER:              0,
	} {
		for _, up := range []bool{false, true} {
			adapter := &windows.IpAdapterAddresses{IfType: kind, Mtu: 0xffffffff}
			flags := want
			if up {
				adapter.OperStatus = windows.IfOperStatusUp
				flags |= net.FlagUp | net.FlagRunning
			}
			iface := windowsInterface(adapter)
			if iface.Flags != flags || iface.MTU != -1 || iface.HardwareAddr != nil {
				t.Fatalf("media %d up=%v: %+v", kind, up, iface)
			}
		}
	}
}

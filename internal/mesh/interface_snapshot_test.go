package mesh

import (
	"net"
	"testing"
)

func TestLANBeaconReadsOneFreshInterfaceSnapshot(t *testing.T) {
	fakeDefaultRouteEgress(t, "", false)
	node, err := NewNode("snapshot-room", nil, DefaultOfflineConfig())
	if err != nil {
		t.Fatal(err)
	}
	reads, addressReads := 0, 0
	host := "192.168.1.42"
	read := func() (interfaceView, error) {
		reads++
		return interfaceView{
			interfaces: []net.Interface{{Index: 1, Name: "Ethernet", Flags: net.FlagUp | net.FlagMulticast}},
			addresses: func(net.Interface) ([]net.Addr, error) {
				addressReads++
				return []net.Addr{&net.IPNet{IP: net.ParseIP(host), Mask: net.CIDRMask(24, 32)}}, nil
			},
		}, nil
	}
	for operation, nextHost := range []string{"192.168.1.42", "192.168.2.42"} {
		host = nextHost
		targets, err := node.prepareLANBeacon(&interfaceSnapshot{read: read})
		if err != nil {
			t.Fatal(err)
		}
		if reads != operation+1 || addressReads != operation+1 {
			t.Fatalf("operation %d: inventory reads=%d, address reads=%d; want one fresh read per operation", operation, reads, addressReads)
		}
		if len(targets.interfaces) != 1 || len(targets.broadcasts) != 1 {
			t.Fatalf("incomplete beacon targets: %+v", targets)
		}
		want := "192.168.1.255"
		if operation == 1 {
			want = "192.168.2.255"
		}
		if got := targets.broadcasts[0].IP.String(); got != want {
			t.Fatalf("broadcast=%s, want fresh %s", got, want)
		}
	}
}

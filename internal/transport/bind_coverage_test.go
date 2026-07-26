package transport

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// rawSocketCall matches the socket constructors that pick their local address
// from the routing table unless something binds them afterwards.
var rawSocketCall = regexp.MustCompile(`net\.(Dial|DialTimeout|DialUDP|DialTCP|ListenPacket|Listen|ListenUDP)\(|net\.Dialer\{`)

// alreadyBound matches the helpers that pin a socket to bindIfIndex. A line
// that hands its socket to one of these is doing the right thing, however raw
// the constructor next to it looks.
var alreadyBound = regexp.MustCompile(`DialerWithBind|ApplyBindTo(UDP|Packet)`)

// boundElsewhere lists the files allowed to construct a socket without an
// immediately visible bind, each for a reason that has to stay true.
var boundElsewhere = map[string]string{
	// This file is the bind.
	filepath.Join("transport", "bind.go"): "applies the bind",
	// The mesh UDP listener is bound a few lines below, via ApplyBindToUDP.
	filepath.Join("transport", "udp.go"): "binds the listener it creates",
	// Inbound TCP stays on 0.0.0.0 on purpose: SO_BINDTODEVICE on a listening
	// socket would refuse connections arriving over the tunnel, and outbound
	// TCP gets its own bound dialer in mesh/node_accept.go.
	filepath.Join("transport", "listener.go"): "inbound only, deliberately unbound",
	// Send socket is bound at the call site; the receive socket joins a
	// link-local multicast group per interface and never leaves the LAN.
	filepath.Join("mesh", "lan_discovery.go"): "binds its send socket, receive is link-local",
	// Bound immediately after ListenPacket.
	filepath.Join("mesh", "dht.go"): "binds the socket it creates",
	// PCP/NAT-PMP talks to on-link gateways over their own chain, which does
	// not carry bindIfIndex yet. Tracked separately; a mapping obtained from a
	// VPN gateway is the remaining way to advertise a tunnel-side address.
	filepath.Join("nat", "pmp.go"): "on-link gateway, separate plumbing",
}

// TestNoUnboundSocketCallSites fails when a new socket appears somewhere that
// cannot honour bind_interface. Splitting the node across two NICs is invisible
// at runtime — it shows up as a peer advertising two addresses and the mesh
// arguing about which is real — so the invariant is guarded at the source.
func TestNoUnboundSocketCallSites(t *testing.T) {
	root := ".."
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		if _, ok := boundElsewhere[rel]; ok {
			return nil
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for i, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "//") || alreadyBound.MatchString(line) {
				continue
			}
			if rawSocketCall.MatchString(line) {
				t.Errorf("%s:%d creates a socket the routing table controls:\n\t%s\n"+
					"use transport.DialerWithBind / ApplyBindToUDP / ApplyBindToPacket with the node's "+
					"bindIfIndex, or add the file to boundElsewhere with the reason it is exempt",
					rel, i+1, strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/: %v", err)
	}
}

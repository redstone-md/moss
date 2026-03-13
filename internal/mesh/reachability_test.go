package mesh

import "testing"

func TestReachabilityProbePeerIDsExcludeFreshPrivatePeer(t *testing.T) {
	node := &Node{
		peers: map[string]*peerConn{
			"private": {id: "private", addr: "10.0.0.8:41030"},
		},
		knownPeers: map[string]knownPeer{},
	}

	peerIDs := node.reachabilityProbePeerIDsLocked()
	if len(peerIDs) != 0 {
		t.Fatalf("expected no eligible reachability probers, got %#v", peerIDs)
	}
}

func TestReachabilityProbePeerIDsIncludeBootstrapAndPublicPeers(t *testing.T) {
	node := &Node{
		peers: map[string]*peerConn{
			"bootstrap": {id: "bootstrap", addr: "10.0.0.8:41030", bootstrap: true},
			"public":    {id: "public", addr: "198.51.100.10:41030"},
		},
		knownPeers: map[string]knownPeer{
			"public": {id: "public", publicReachable: true},
		},
	}

	peerIDs := node.reachabilityProbePeerIDsLocked()
	if len(peerIDs) != 2 || peerIDs[0] != "bootstrap" || peerIDs[1] != "public" {
		t.Fatalf("unexpected eligible reachability probers: %#v", peerIDs)
	}
}

func TestReachabilityProbePeerIDsIncludePublicTransportAddr(t *testing.T) {
	node := &Node{
		peers: map[string]*peerConn{
			"public-addr": {id: "public-addr", addr: "203.0.113.20:41030"},
		},
		knownPeers: map[string]knownPeer{},
	}

	peerIDs := node.reachabilityProbePeerIDsLocked()
	if len(peerIDs) != 1 || peerIDs[0] != "public-addr" {
		t.Fatalf("expected public peer address to be eligible, got %#v", peerIDs)
	}
}

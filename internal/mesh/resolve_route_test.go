package mesh

import (
	"testing"
)

// ResolveRoute must answer from the peer table first: a live direct session
// is the answer, no overlay round-trip needed.
func TestResolveRouteDirectPeer(t *testing.T) {
	node := &Node{peers: map[string]*peerConn{
		"peer": {id: "peer"},
	}}
	route, err := node.ResolveRoute("peer")
	if err != nil {
		t.Fatalf("ResolveRoute(direct): %v", err)
	}
	if route.Relayed || route.ViaPeerID != "" {
		t.Fatalf("direct peer resolved to %+v, want zero Route", route)
	}
}

// A relayed peerConn resolves to a relay Route naming the relay hop — the
// caller takes it from there with RelaySendTo.
func TestResolveRouteRelayedPeer(t *testing.T) {
	node := &Node{peers: map[string]*peerConn{
		"peer": {id: "peer", relayed: true, viaPeerID: "via"},
	}}
	route, err := node.ResolveRoute("peer")
	if err != nil {
		t.Fatalf("ResolveRoute(relayed): %v", err)
	}
	if !route.Relayed || route.ViaPeerID != "via" {
		t.Fatalf("relayed peer resolved to %+v, want {Relayed: true, ViaPeerID: via}", route)
	}
}

// A peer nobody knows must fail cleanly, not hang: with no overlay table the
// lookup returns nothing and the dial loop is skipped. The literal node has a
// nil rootCtx, which is exactly the unstarted-node case the context fallback
// guards against (WithTimeout on a nil parent would panic).
func TestResolveRouteUnknownPeerFails(t *testing.T) {
	node := &Node{}
	_, err := node.ResolveRoute("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	if err == nil {
		t.Fatal("unknown peer resolved with no overlay and no peers, want an error")
	}
}

// Malformed peer IDs must fail before any lookup: IDFromHex demands the
// canonical 64-hex form, and a non-hex string never matches a provider.
func TestResolveRouteInvalidPeerIDFails(t *testing.T) {
	node := &Node{}
	if _, err := node.ResolveRoute("not-hex"); err == nil {
		t.Fatal("non-hex peer id resolved, want an error")
	}
}

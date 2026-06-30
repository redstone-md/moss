package main

import (
	"sort"
	"testing"
)

func TestHubJoinReturnsExistingPeers(t *testing.T) {
	h := newHub()
	a := &client{id: "a", room: "mesh", out: make(chan []byte, 1)}
	b := &client{id: "b", room: "mesh", out: make(chan []byte, 1)}

	if got := h.join(a); len(got) != 0 {
		t.Fatalf("first joiner should see no peers, got %v", got)
	}
	got := h.join(b)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("second joiner should see [a], got %v", got)
	}
}

func TestHubRouteDeliversToTarget(t *testing.T) {
	h := newHub()
	a := &client{id: "a", room: "mesh", out: make(chan []byte, 1)}
	b := &client{id: "b", room: "mesh", out: make(chan []byte, 1)}
	h.join(a)
	h.join(b)

	if !h.route("mesh", "b", []byte("hi")) {
		t.Fatal("route to present peer should succeed")
	}
	select {
	case msg := <-b.out:
		if string(msg) != "hi" {
			t.Fatalf("wrong payload %q", msg)
		}
	default:
		t.Fatal("b did not receive routed message")
	}
	if h.route("mesh", "ghost", []byte("x")) {
		t.Fatal("route to absent peer should fail")
	}
}

func TestHubBroadcastExcludesSender(t *testing.T) {
	h := newHub()
	a := &client{id: "a", room: "mesh", out: make(chan []byte, 1)}
	b := &client{id: "b", room: "mesh", out: make(chan []byte, 1)}
	c := &client{id: "c", room: "mesh", out: make(chan []byte, 1)}
	h.join(a)
	h.join(b)
	h.join(c)

	h.broadcast("mesh", "a", []byte("ping"))
	if len(a.out) != 0 {
		t.Fatal("sender should not receive its own broadcast")
	}
	if len(b.out) != 1 || len(c.out) != 1 {
		t.Fatal("other peers should receive broadcast")
	}
}

func TestHubLeaveCleansUpRoom(t *testing.T) {
	h := newHub()
	a := &client{id: "a", room: "mesh", out: make(chan []byte, 1)}
	h.join(a)
	h.leave(a)
	if got := h.peers("mesh"); len(got) != 0 {
		t.Fatalf("room should be empty after leave, got %v", got)
	}
	// Re-join works and room is fresh.
	b := &client{id: "b", room: "mesh", out: make(chan []byte, 1)}
	if got := h.join(b); len(got) != 0 {
		sort.Strings(got)
		t.Fatalf("fresh room expected, got %v", got)
	}
}

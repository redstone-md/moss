package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestNormalizeRoom(t *testing.T) {
	got, err := normalizeRoom("  #Lobby ")
	if err != nil {
		t.Fatalf("normalizeRoom returned error: %v", err)
	}
	if got != "lobby" {
		t.Fatalf("expected lobby, got %q", got)
	}
}

func TestNormalizeRoomRejectsEmpty(t *testing.T) {
	if _, err := normalizeRoom("   "); err == nil {
		t.Fatal("expected empty room to fail")
	}
}

func TestFormatPeer(t *testing.T) {
	got := formatPeer("1234567890abcdef")
	if got != "12345678..cdef" {
		t.Fatalf("unexpected peer format %q", got)
	}
}

func TestSanitizeName(t *testing.T) {
	got := sanitizeName(" Andy / Admin ")
	if got != "andyadmin" {
		t.Fatalf("unexpected sanitized name %q", got)
	}
}

func TestFinalizeOptionsNormalizesRoomsAndPeers(t *testing.T) {
	opts, err := finalizeOptions(options{
		nickname: "Andrii",
		rooms:    []string{"#Lobby", " lobby ", "Ops"},
		peers:    []string{" 127.0.0.1:41030 ", "127.0.0.1:41030"},
	})
	if err != nil {
		t.Fatalf("finalizeOptions returned error: %v", err)
	}
	if len(opts.rooms) != 2 || opts.rooms[0] != "lobby" || opts.rooms[1] != "ops" {
		t.Fatalf("unexpected rooms: %#v", opts.rooms)
	}
	if len(opts.peers) != 1 || opts.peers[0] != "127.0.0.1:41030" {
		t.Fatalf("unexpected peers: %#v", opts.peers)
	}
}

func TestPromptMissingOptionsUsesDefaults(t *testing.T) {
	input := strings.NewReader("Andrii\n\n\n\n\n\n")
	var output bytes.Buffer
	opts, err := promptMissingOptions(options{
		meshID:     defaultMesh,
		listenPort: 0,
		rooms:      []string{defaultRoom},
	}, input, &output)
	if err != nil {
		t.Fatalf("promptMissingOptions returned error: %v", err)
	}
	if opts.nickname != "Andrii" {
		t.Fatalf("unexpected nickname %q", opts.nickname)
	}
	if opts.meshID != defaultMesh {
		t.Fatalf("unexpected mesh id %q", opts.meshID)
	}
	if len(opts.rooms) != 1 || opts.rooms[0] != defaultRoom {
		t.Fatalf("unexpected rooms %#v", opts.rooms)
	}
	if opts.noTrackers {
		t.Fatal("expected trackers to remain enabled")
	}
}

func TestSplitTargetAndText(t *testing.T) {
	target, text := splitTargetAndText("alice hello there")
	if target != "alice" || text != "hello there" {
		t.Fatalf("unexpected split result target=%q text=%q", target, text)
	}
}

func TestDirectRoomNameStableOrdering(t *testing.T) {
	a := directRoomName("bbbb2222", "aaaa1111")
	b := directRoomName("aaaa1111", "bbbb2222")
	if a != b {
		t.Fatalf("expected stable room ordering, got %q and %q", a, b)
	}
}

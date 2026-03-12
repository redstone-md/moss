package main

import "testing"

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

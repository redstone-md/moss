package transport

import (
	"net"
	"testing"
)

func TestSTUNBindingRequestHasValidFingerprint(t *testing.T) {
	packet, _, err := buildSTUNBindingRequest()
	if err != nil {
		t.Fatalf("buildSTUNBindingRequest failed: %v", err)
	}
	if !isSTUNMessage(packet) {
		t.Fatal("binding request is not recognized as STUN")
	}
	if !validSTUNFingerprint(packet) {
		t.Fatal("binding request fingerprint is invalid")
	}
	packet[len(packet)-1] ^= 0xff
	if validSTUNFingerprint(packet) {
		t.Fatal("corrupted binding request fingerprint validated")
	}
}

func TestSTUNBindingSuccessRoundTripIPv4(t *testing.T) {
	_, txID, err := buildSTUNBindingRequest()
	if err != nil {
		t.Fatalf("buildSTUNBindingRequest failed: %v", err)
	}
	packet := buildSTUNBindingSuccess(txID, &net.UDPAddr{IP: net.ParseIP("203.0.113.7"), Port: 3478})
	if !validSTUNFingerprint(packet) {
		t.Fatal("binding success fingerprint is invalid")
	}
	gotTxID, observed, ok := parseSTUNBindingSuccess(packet)
	if !ok {
		t.Fatal("binding success did not parse")
	}
	if gotTxID != txID {
		t.Fatalf("txid mismatch: got %x want %x", gotTxID, txID)
	}
	if observed != "203.0.113.7:3478" {
		t.Fatalf("observed endpoint mismatch: got %q", observed)
	}
}

func TestSTUNBindingSuccessRoundTripIPv6(t *testing.T) {
	_, txID, err := buildSTUNBindingRequest()
	if err != nil {
		t.Fatalf("buildSTUNBindingRequest failed: %v", err)
	}
	packet := buildSTUNBindingSuccess(txID, &net.UDPAddr{IP: net.ParseIP("2001:db8::55"), Port: 5000})
	if !validSTUNFingerprint(packet) {
		t.Fatal("binding success fingerprint is invalid")
	}
	_, observed, ok := parseSTUNBindingSuccess(packet)
	if !ok {
		t.Fatal("binding success did not parse")
	}
	if observed != "[2001:db8::55]:5000" {
		t.Fatalf("observed endpoint mismatch: got %q", observed)
	}
}

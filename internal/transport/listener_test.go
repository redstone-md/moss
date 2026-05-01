package transport

import (
	"net"
	"testing"
)

func TestListenPairResolvesHostnameForTCPAndUDP(t *testing.T) {
	t.Setenv("MOSS_LISTEN_HOST", "localhost")

	ln, udpListener, _, err := ListenPair(0, HandshakeConfig{})
	if err != nil {
		t.Fatalf("ListenPair failed: %v", err)
	}
	defer ln.Close()
	defer udpListener.Close()

	tcpAddr := ln.Addr().(*net.TCPAddr)
	udpAddr := udpListener.Addr().(*net.UDPAddr)
	if !tcpAddr.IP.IsLoopback() {
		t.Fatalf("TCP listener bound outside loopback: %s", tcpAddr)
	}
	if !udpAddr.IP.IsLoopback() {
		t.Fatalf("UDP listener bound outside loopback: %s", udpAddr)
	}
	if tcpAddr.Port != udpAddr.Port {
		t.Fatalf("TCP/UDP ports differ: tcp=%d udp=%d", tcpAddr.Port, udpAddr.Port)
	}
}

func TestListenUDPRejectsUnresolvableHost(t *testing.T) {
	t.Setenv("MOSS_LISTEN_HOST", "invalid host")

	udpListener, _, err := ListenUDP(0, HandshakeConfig{})
	if err == nil {
		_ = udpListener.Close()
		t.Fatal("ListenUDP unexpectedly accepted unresolvable host")
	}
}

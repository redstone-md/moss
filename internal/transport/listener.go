package transport

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Listener struct {
	net.Listener
}

func Listen(port int) (*Listener, int, error) {
	addr := listenAddr(port)
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		return nil, 0, err
	}
	actual := ln.Addr().(*net.TCPAddr).Port
	return &Listener{Listener: ln}, actual, nil
}

// ListenPair brings up the substrate transport. UDP is the primary path (Noise
// sessions, hole punching, relay) and is REQUIRED — ListenUDP itself falls back
// to a raw blocking socket on hosts where Go's netpoller cannot bind (older
// Wine/Proton). TCP is a best-effort secondary path on the same port; when it
// cannot bind, the node comes up UDP-only (a nil *Listener) instead of failing
// to start, so Proton users still get a working P2P transport. The returned
// *Listener is nil in that UDP-only case; callers must skip their TCP accept
// loop accordingly.
func ListenPair(port int, cfg HandshakeConfig) (*Listener, *UDPListener, int, error) {
	attempts := 1
	if port == 0 {
		attempts = 8
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		udpListener, actualPort, err := ListenUDP(port, cfg)
		if err != nil {
			lastErr = err
			if port != 0 {
				return nil, nil, 0, err
			}
			time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
			continue
		}
		ln, _, tcpErr := Listen(actualPort)
		if tcpErr == nil {
			return ln, udpListener, actualPort, nil
		}
		lastErr = tcpErr
		if port != 0 {
			// Honour the requested fixed port: come up UDP-only rather than moving
			// the node to a different port merely to also acquire TCP.
			return nil, udpListener, actualPort, nil
		}
		// Auto-port: this pair lost the race for the TCP side; drop it and retry a
		// fresh pairing so we prefer a port where both bind.
		_ = udpListener.Close()
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	// Auto-port never found a matching TCP+UDP pair; prefer UDP-only over failing.
	if udpListener, actualPort, err := ListenUDP(0, cfg); err == nil {
		return nil, udpListener, actualPort, nil
	} else if lastErr == nil {
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("failed to bind udp listener")
	}
	return nil, nil, 0, lastErr
}

// ListenMasqPair brings up the masquerade TCP listener and the UDP listener on
// one port. An auto port (0) retries a fresh pairing when another process
// already holds the UDP side of the port the OS chose for TCP, as ListenPair
// does; a fixed port fails at once.
func ListenMasqPair(port int, coverSNI string, cfg HandshakeConfig) (*MasqListener, *UDPListener, int, error) {
	attempts := 1
	if port == 0 {
		attempts = 8
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		masq, err := MasqListen(net.JoinHostPort("", strconv.Itoa(port)), coverSNI)
		if err == nil {
			udpListener, actualPort, udpErr := ListenUDP(masq.Addr().(*net.TCPAddr).Port, cfg)
			if udpErr == nil {
				return masq, udpListener, actualPort, nil
			}
			_ = masq.Close()
			err = udpErr
		}
		lastErr = err
		if port != 0 {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return nil, nil, 0, lastErr
}

func listenHost() string {
	if host := os.Getenv("MOSS_LISTEN_HOST"); host != "" {
		return host
	}
	if RunningGoTest() {
		return "127.0.0.1"
	}
	return "0.0.0.0"
}

func listenAddr(port int) string {
	return net.JoinHostPort(listenHost(), strconv.Itoa(port))
}

func listenUDPAddr(port int) (*net.UDPAddr, error) {
	return net.ResolveUDPAddr("udp4", listenAddr(port))
}

func RunningGoTest() bool {
	name := strings.ToLower(filepath.Base(os.Args[0]))
	return strings.HasSuffix(name, ".test") || strings.HasSuffix(name, ".test.exe")
}

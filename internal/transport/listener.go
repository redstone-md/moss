package transport

import (
	"errors"
	"net"
	"strconv"
	"time"
)

type Listener struct {
	net.Listener
}

func Listen(port int) (*Listener, int, error) {
	addr := "0.0.0.0:" + strconv.Itoa(port)
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		return nil, 0, err
	}
	actual := ln.Addr().(*net.TCPAddr).Port
	return &Listener{Listener: ln}, actual, nil
}

func ListenPair(port int, cfg HandshakeConfig) (*Listener, *UDPListener, int, error) {
	attempts := 1
	if port == 0 {
		attempts = 64
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		ln, actualPort, err := Listen(port)
		if err != nil {
			lastErr = err
			if port == 0 {
				time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
				continue
			}
			return nil, nil, 0, err
		}
		udpListener, _, err := ListenUDP(actualPort, cfg)
		if err == nil {
			return ln, udpListener, actualPort, nil
		}
		lastErr = err
		_ = ln.Close()
		if port == 0 {
			udpListener, actualPort, err = ListenUDP(0, cfg)
			if err == nil {
				ln, _, err = Listen(actualPort)
				if err == nil {
					return ln, udpListener, actualPort, nil
				}
				lastErr = err
				_ = udpListener.Close()
			}
			time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
		}
		if port != 0 {
			break
		}
	}
	if lastErr == nil {
		lastErr = errors.New("failed to bind tcp/udp listener pair")
	}
	return nil, nil, 0, lastErr
}

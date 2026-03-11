package transport

import (
	"net"
	"strconv"
)

type Listener struct {
	net.Listener
}

func Listen(port int) (*Listener, int, error) {
	addr := ":" + strconv.Itoa(port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, 0, err
	}
	actual := ln.Addr().(*net.TCPAddr).Port
	return &Listener{Listener: ln}, actual, nil
}

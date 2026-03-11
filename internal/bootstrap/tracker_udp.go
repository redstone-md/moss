package bootstrap

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"net/url"
	"time"
)

const (
	trackerConnectAction  = 0
	trackerAnnounceAction = 1
	trackerConnectionID   = 0x41727101980
)

type UDPClient struct{}

func (c *UDPClient) Announce(ctx context.Context, trackerURL string, req AnnounceRequest) ([]string, error) {
	u, err := url.Parse(trackerURL)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "80")
	}
	conn, err := net.DialTimeout("udp", host, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	connectTx := rand.Uint32()
	connectMsg := make([]byte, 16)
	binary.BigEndian.PutUint64(connectMsg[0:8], trackerConnectionID)
	binary.BigEndian.PutUint32(connectMsg[8:12], trackerConnectAction)
	binary.BigEndian.PutUint32(connectMsg[12:16], connectTx)
	if _, err := conn.Write(connectMsg); err != nil {
		return nil, err
	}
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	if n < 16 {
		return nil, errors.New("short connect response")
	}
	if binary.BigEndian.Uint32(buf[0:4]) != trackerConnectAction {
		return nil, errors.New("unexpected tracker action")
	}
	if binary.BigEndian.Uint32(buf[4:8]) != connectTx {
		return nil, errors.New("tracker transaction mismatch")
	}
	connID := binary.BigEndian.Uint64(buf[8:16])
	announceTx := rand.Uint32()
	announceMsg := make([]byte, 98)
	binary.BigEndian.PutUint64(announceMsg[0:8], connID)
	binary.BigEndian.PutUint32(announceMsg[8:12], trackerAnnounceAction)
	binary.BigEndian.PutUint32(announceMsg[12:16], announceTx)
	copy(announceMsg[16:36], req.InfoHash[:])
	copy(announceMsg[36:56], req.PeerID[:])
	binary.BigEndian.PutUint64(announceMsg[64:72], 1)
	binary.BigEndian.PutUint32(announceMsg[80:84], uint32(req.Event))
	binary.BigEndian.PutUint32(announceMsg[88:92], rand.Uint32())
	binary.BigEndian.PutUint32(announceMsg[92:96], uint32(req.NumWant))
	binary.BigEndian.PutUint16(announceMsg[96:98], uint16(req.Port))
	if _, err := conn.Write(announceMsg); err != nil {
		return nil, err
	}
	n, err = conn.Read(buf)
	if err != nil {
		return nil, err
	}
	if n < 20 {
		return nil, errors.New("short announce response")
	}
	if binary.BigEndian.Uint32(buf[0:4]) != trackerAnnounceAction {
		return nil, errors.New("unexpected announce action")
	}
	if binary.BigEndian.Uint32(buf[4:8]) != announceTx {
		return nil, errors.New("announce transaction mismatch")
	}
	return parseCompactPeers(buf[20:n]), nil
}

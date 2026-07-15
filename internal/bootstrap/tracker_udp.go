package bootstrap

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"net/url"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

const (
	trackerConnectAction  = 0
	trackerAnnounceAction = 1
	trackerConnectionID   = 0x41727101980
)

// UDPClient announces over the binary UDP tracker protocol. Setting
// BindIfIndex to a non-zero value forces the underlying socket onto a
// specific NIC (see transport.ResolveBindInterface).
type UDPClient struct {
	BindIfIndex int
}

var (
	udpTrackerDialTimeout    = 3 * time.Second
	udpTrackerResponseWindow = 5 * time.Second
	udpTrackerRetryBase      = 15 * time.Second
	udpTrackerMaxRetries     = 9
)

func (c *UDPClient) Announce(ctx context.Context, trackerURL string, req AnnounceRequest) ([]string, error) {
	u, err := url.Parse(trackerURL)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "80")
	}
	dialer := transport.DialerWithBind(net.Dialer{Timeout: udpTrackerDialTimeout}, c.BindIfIndex)
	dialCtx, cancel := context.WithTimeout(ctx, udpTrackerDialTimeout)
	defer cancel()
	conn, err := dialer.DialContext(dialCtx, "udp", host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	connID, err := c.connect(ctx, conn)
	if err != nil {
		return nil, err
	}
	resp, err := c.announce(ctx, conn, connID, req)
	if err != nil {
		return nil, err
	}
	return parseCompactPeers(resp[20:]), nil
}

func (c *UDPClient) connect(ctx context.Context, conn net.Conn) (uint64, error) {
	buf := make([]byte, 2048)
	var connID uint64
	err := c.retry(ctx, conn, func() error {
		txID := rand.Uint32()
		msg := make([]byte, 16)
		binary.BigEndian.PutUint64(msg[0:8], trackerConnectionID)
		binary.BigEndian.PutUint32(msg[8:12], trackerConnectAction)
		binary.BigEndian.PutUint32(msg[12:16], txID)
		n, err := exchangeUDP(conn, msg, buf)
		if err != nil {
			return err
		}
		if n < 16 {
			return errors.New("short connect response")
		}
		if binary.BigEndian.Uint32(buf[0:4]) != trackerConnectAction {
			return errors.New("unexpected tracker action")
		}
		if binary.BigEndian.Uint32(buf[4:8]) != txID {
			return errors.New("tracker transaction mismatch")
		}
		connID = binary.BigEndian.Uint64(buf[8:16])
		return nil
	})
	return connID, err
}

func (c *UDPClient) announce(ctx context.Context, conn net.Conn, connID uint64, req AnnounceRequest) ([]byte, error) {
	buf := make([]byte, 2048)
	var response []byte
	err := c.retry(ctx, conn, func() error {
		txID := rand.Uint32()
		msg := make([]byte, 98)
		binary.BigEndian.PutUint64(msg[0:8], connID)
		binary.BigEndian.PutUint32(msg[8:12], trackerAnnounceAction)
		binary.BigEndian.PutUint32(msg[12:16], txID)
		copy(msg[16:36], req.InfoHash[:])
		copy(msg[36:56], req.PeerID[:])
		binary.BigEndian.PutUint64(msg[64:72], 1)
		binary.BigEndian.PutUint32(msg[80:84], uint32(req.Event))
		binary.BigEndian.PutUint32(msg[88:92], rand.Uint32())
		binary.BigEndian.PutUint32(msg[92:96], uint32(req.NumWant))
		binary.BigEndian.PutUint16(msg[96:98], uint16(req.Port))
		n, err := exchangeUDP(conn, msg, buf)
		if err != nil {
			return err
		}
		if n < 20 {
			return errors.New("short announce response")
		}
		if binary.BigEndian.Uint32(buf[0:4]) != trackerAnnounceAction {
			return errors.New("unexpected announce action")
		}
		if binary.BigEndian.Uint32(buf[4:8]) != txID {
			return errors.New("announce transaction mismatch")
		}
		response = append([]byte(nil), buf[:n]...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *UDPClient) retry(ctx context.Context, conn net.Conn, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < udpTrackerMaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		if deadline, ok := attemptDeadline(ctx); ok {
			_ = conn.SetDeadline(deadline)
		}
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryableUDPError(err) || attempt == udpTrackerMaxRetries-1 {
			return err
		}
		wait := udpTrackerRetryBase << attempt
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return lastErr
			}
			if wait > remaining {
				wait = remaining
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lastErr
		case <-timer.C:
		}
	}
	return lastErr
}

func exchangeUDP(conn net.Conn, msg []byte, buf []byte) (int, error) {
	if _, err := conn.Write(msg); err != nil {
		return 0, err
	}
	return conn.Read(buf)
}

func attemptDeadline(ctx context.Context) (time.Time, bool) {
	deadline := time.Now().Add(udpTrackerResponseWindow)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline, true
	}
	return deadline, true
}

func retryableUDPError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

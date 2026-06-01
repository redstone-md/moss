package bootstrap

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"moss/internal/transport"
)

type HTTPClient struct {
	Client      *http.Client
	BindIfIndex int
}

func NewHTTPClient(timeout time.Duration) *HTTPClient {
	return NewHTTPClientWithBind(timeout, 0)
}

func NewHTTPClientWithBind(timeout time.Duration, bindIfIndex int) *HTTPClient {
	client := &http.Client{Timeout: timeout}
	if bindIfIndex != 0 {
		dialer := transport.DialerWithBind(net.Dialer{Timeout: timeout}, bindIfIndex)
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.DialContext = dialer.DialContext
		client.Transport = tr
	}
	return &HTTPClient{
		Client:      client,
		BindIfIndex: bindIfIndex,
	}
}

func (c *HTTPClient) Announce(ctx context.Context, trackerURL string, req AnnounceRequest) ([]string, error) {
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 5 * time.Second}
	}
	u, err := url.Parse(trackerURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("compact", "1")
	q.Set("port", strconv.Itoa(req.Port))
	q.Set("uploaded", "0")
	q.Set("downloaded", "0")
	q.Set("left", "1")
	q.Set("event", req.Event.String())
	q.Set("numwant", strconv.Itoa(req.NumWant))
	q.Set("peer_id", string(req.PeerID[:]))
	q.Set("info_hash", string(req.InfoHash[:]))
	u.RawQuery = q.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeBencode(body)
	if err != nil {
		return nil, err
	}
	dict, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("tracker response is not a dictionary")
	}
	if failure, ok := dict["failure reason"].(string); ok && failure != "" {
		return nil, errors.New(failure)
	}
	peersRaw, ok := dict["peers"].(string)
	if !ok {
		return nil, nil
	}
	return parseCompactPeers([]byte(peersRaw)), nil
}

type bencodeParser struct {
	data []byte
	pos  int
}

func decodeBencode(data []byte) (any, error) {
	p := &bencodeParser{data: data}
	return p.value()
}

func (p *bencodeParser) value() (any, error) {
	if p.pos >= len(p.data) {
		return nil, io.ErrUnexpectedEOF
	}
	switch p.data[p.pos] {
	case 'd':
		return p.dict()
	case 'i':
		return p.integer()
	default:
		if p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			return p.string()
		}
		return nil, fmt.Errorf("unsupported bencode token %q", p.data[p.pos])
	}
}

func (p *bencodeParser) dict() (map[string]any, error) {
	p.pos++
	out := make(map[string]any)
	for {
		if p.pos >= len(p.data) {
			return nil, io.ErrUnexpectedEOF
		}
		if p.data[p.pos] == 'e' {
			p.pos++
			return out, nil
		}
		key, err := p.string()
		if err != nil {
			return nil, err
		}
		value, err := p.value()
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
}

func (p *bencodeParser) integer() (int64, error) {
	p.pos++
	start := p.pos
	for p.pos < len(p.data) && p.data[p.pos] != 'e' {
		p.pos++
	}
	if p.pos >= len(p.data) {
		return 0, io.ErrUnexpectedEOF
	}
	value, err := strconv.ParseInt(string(p.data[start:p.pos]), 10, 64)
	if err != nil {
		return 0, err
	}
	p.pos++
	return value, nil
}

func (p *bencodeParser) string() (string, error) {
	start := p.pos
	for p.pos < len(p.data) && p.data[p.pos] != ':' {
		p.pos++
	}
	if p.pos >= len(p.data) {
		return "", io.ErrUnexpectedEOF
	}
	size, err := strconv.Atoi(string(p.data[start:p.pos]))
	if err != nil {
		return "", err
	}
	p.pos++
	end := p.pos + size
	if end > len(p.data) {
		return "", io.ErrUnexpectedEOF
	}
	out := string(p.data[p.pos:end])
	p.pos = end
	return out, nil
}

func parseCompactPeers(raw []byte) []string {
	out := make([]string, 0, len(raw)/6)
	for i := 0; i+6 <= len(raw); i += 6 {
		ip := net.IPv4(raw[i], raw[i+1], raw[i+2], raw[i+3]).String()
		port := binary.BigEndian.Uint16(raw[i+4 : i+6])
		out = append(out, net.JoinHostPort(ip, strconv.Itoa(int(port))))
	}
	return out
}

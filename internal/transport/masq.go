package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	utls "github.com/refraction-networking/utls"
)

// Masquerading bearer for direct peer-to-peer links: a Chrome-shaped uTLS
// ClientHello (HelloChrome_Auto) aimed at CoverSNI on the dial side, and a
// freshly forged self-signed ECDSA P-256 certificate naming that same SNI on
// the listen side, with the Noise session riding inside the TLS 1.3 stream.
//
// This is the p2p story Veil's Reality bearer cannot serve. Reality couples
// its ClientHello masquerade to a relay topology: an HMAC auth tag stuffed
// into the TLS SessionID, and probe traffic spliced to a third-party origin
// so the listener can convincingly pretend to be a real site. A direct
// peer-to-peer link has no relay to splice to and no origin to impersonate —
// so none of that is carried over. Here TLS is camouflage only: every
// connection that completes the handshake reaches Accept, and the Noise
// handshake the caller runs next is the sole authentication gate. The cover
// certificate is self-signed and clients skip chain validation on purpose —
// a Moss peer is identified by its static Noise key, never by a CA.
//
// The client half mirrors veil/core's realitytr dialer (uTLS Chrome preset,
// TLS 1.3, InsecureSkipVerify with Noise as the real anchor) minus the
// auth-bearing SessionID; the listen half mirrors its listener minus the
// verifier and splice path, keeping the forged-cover-certificate shape. Both
// sides must be configured with the same CoverSNI or the ruse mismatches.

// masqHandshakeTimeout bounds the inbound TLS handshake so a silent peer
// cannot pin an accept goroutine forever. Outbound handshakes are bounded by
// the dialer's context (MasqDialer.Timeout / the caller's deadline).
const masqHandshakeTimeout = 20 * time.Second

// MasqDialer opens TCP connections masked as ordinary Chrome-to-cover-site
// TLS 1.3 traffic. The zero value is not usable: CoverSNI must name the same
// cover domain the target peer's listener forges its certificate for.
type MasqDialer struct {
	// CoverSNI is the host name placed in the TLS ClientHello. It must match
	// the MasqListener's CoverSNI on the peer.
	CoverSNI string

	// BindIfIndex optionally pins the TCP socket to an OS interface index
	// (resolved via ResolveBindInterface), so the masqueraded leg leaves
	// through the same NIC as the rest of the node's traffic. Zero keeps
	// routing-table behaviour.
	BindIfIndex int

	// Timeout optionally caps the TCP connect and TLS handshake. Zero means
	// only the passed context bounds them.
	Timeout time.Duration
}

// Dial connects to addr (host:port) and completes a uTLS handshake shaped
// like Chrome's before returning. The returned conn is the raw post-TLS
// stream — the caller runs its own Noise handshake over it (mirroring
// connectPeerOnce: ClientHandshake on the conn Dial returns). A nil context
// is refused rather than silently replaced, matching the mesh dial contract.
func (d *MasqDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	if ctx == nil {
		return nil, errors.New("transport: masq dial requires a non-nil context")
	}
	if d.CoverSNI == "" {
		return nil, errors.New("transport: masq dial requires a cover SNI")
	}

	dialCtx := ctx
	var cancel context.CancelFunc
	if d.Timeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, d.Timeout)
		defer cancel()
	}
	base := net.Dialer{Timeout: d.Timeout}
	// The dial side leaves through the node's NIC when BindIfIndex is set
	// (DialerWithBind pins the fresh socket); zero keeps routing-table
	// behaviour, matching the rest of the mesh's outbound dials.
	tcp, err := DialerWithBind(base, d.BindIfIndex).DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: masq tcp dial: %w", err)
	}

	uConn := utls.UClient(tcp, &utls.Config{
		ServerName: d.CoverSNI,
		// The peer presents a forged cover certificate by design; the Noise
		// handshake layered on top is the real authentication anchor.
		InsecureSkipVerify: true,
		MinVersion:         utls.VersionTLS13,
	}, utls.HelloChrome_Auto)
	if err := uConn.HandshakeContext(dialCtx); err != nil {
		_ = tcp.Close()
		return nil, fmt.Errorf("transport: masq tls handshake: %w", err)
	}
	return uConn, nil
}

// MasqListener accepts inbound connections masked as ordinary TLS traffic to
// the cover site. Its Accept hands out the raw post-TLS stream; the caller
// runs the Noise server handshake over each conn (mirroring handleInbound).
type MasqListener struct {
	// CoverSNI is the cover domain the forged certificate names. It must
	// match the dialing peers' MasqDialer.CoverSNI.
	CoverSNI string

	tcpLn   net.Listener
	tlsCfg  *tls.Config
	accepts chan net.Conn
	closed  chan struct{}
	once    sync.Once
}

// MasqListen binds addr (host:port; empty host binds the wildcard) and forges
// a self-signed ECDSA P-256 certificate naming coverSNI. A masquerade with
// no cover domain has nothing to shape the camouflage against, so coverSNI
// is required.
func MasqListen(addr, coverSNI string) (*MasqListener, error) {
	if coverSNI == "" {
		return nil, errors.New("transport: masq listen requires a cover SNI")
	}
	// Inbound stays on the wildcard on purpose, like the plain TCP listener:
	// SO_BINDTODEVICE on a listening socket would refuse connections arriving
	// through the tunnel, and the masquerade exists precisely to be reachable
	// from wherever a peer actually is.
	tcpLn, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: masq tcp listen: %w", err)
	}
	tlsCfg, err := masqCoverConfig(coverSNI)
	if err != nil {
		_ = tcpLn.Close()
		return nil, err
	}
	l := &MasqListener{
		CoverSNI: coverSNI,
		tcpLn:    tcpLn,
		tlsCfg:   tlsCfg,
		accepts:  make(chan net.Conn, 16),
		closed:   make(chan struct{}),
	}
	go l.acceptLoop()
	return l, nil
}

// Accept blocks until the next inbound connection completes its TLS
// handshake, the context is done, or the listener is closed (net.ErrClosed).
// Handshake failures are dropped silently inside the listener — a scanner
// probing the port sees a TLS error, never a Moss session.
func (l *MasqListener) Accept(ctx context.Context) (net.Conn, error) {
	select {
	case c := <-l.accepts:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

// Close shuts the listener down. Connections already accepted by callers are
// untouched.
func (l *MasqListener) Close() error {
	var err error
	l.once.Do(func() {
		close(l.closed)
		err = l.tcpLn.Close()
	})
	return err
}

// Addr reports the bound address (useful when MasqListen was given port 0).
func (l *MasqListener) Addr() net.Addr { return l.tcpLn.Addr() }

func (l *MasqListener) acceptLoop() {
	for {
		raw, err := l.tcpLn.Accept()
		if err != nil {
			select {
			case <-l.closed:
				return
			default:
			}
			return
		}
		go l.handle(raw)
	}
}

// handle runs the inbound TLS handshake off the accept loop and forwards the
// resulting stream to Accept. Failures close the raw conn and vanish — the
// masquerade never surfaces a half-open Moss session.
func (l *MasqListener) handle(raw net.Conn) {
	_ = raw.SetDeadline(time.Now().Add(masqHandshakeTimeout))
	tlsConn := tls.Server(raw, l.tlsCfg)
	if err := tlsConn.HandshakeContext(context.Background()); err != nil {
		_ = raw.Close()
		return
	}
	_ = tlsConn.SetDeadline(time.Time{})
	select {
	case l.accepts <- tlsConn:
	case <-l.closed:
		_ = tlsConn.Close()
	}
}

// masqCoverConfig produces a tls.Config bearing a freshly-generated
// self-signed ECDSA P-256 certificate whose CN/SAN names the cover SNI —
// the same forged-cover shape as veil/core's realitytr listener, pinned to
// TLS 1.3 to match the client side. The certificate is generated once per
// listener, not per connection.
func masqCoverConfig(sni string) (*tls.Config, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("transport: masq ecdsa generate: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: sni},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(90 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{sni},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("transport: masq x509 create: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("transport: masq marshal ec key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("transport: masq x509 keypair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}, nil
}

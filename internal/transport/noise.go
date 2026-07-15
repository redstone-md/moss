package transport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"

	"github.com/flynn/noise"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

type HandshakeConfig struct {
	MeshID       string
	PSK          []byte
	Identity     *mcrypto.Identity
	RemoteStatic []byte
	Buffers      BufferConfig
	// BindIfIndex, when non-zero, forces the UDP listener socket to send
	// outbound packets through the named OS interface index via
	// IP_UNICAST_IF (Windows) / SO_BINDTODEVICE (Linux) / IP_BOUND_IF
	// (macOS). Zero leaves routing-table behaviour intact.
	//
	// Surfaced here because ListenUDP is the single creation point for the
	// session socket and the same option must apply for every datagram it
	// sends. Callers resolve names → indices via ResolveBindInterface.
	BindIfIndex int
	// ObfsPadMax bounds the random padding added to each obfuscated datagram.
	ObfsPadMax int
	// ObfsPadData also pads data datagrams; set false in high-throughput mode.
	ObfsPadData bool
}

// BufferConfig tunes inbound queues for sessions created by a handshake.
// Zero values preserve the transport defaults.
type BufferConfig struct {
	StreamBufferSize     int
	UDPCarrierBufferSize int
}

type identityPayload struct {
	IdentityPub []byte `json:"identity_pub"`
	Signature   []byte `json:"signature"`
	Challenge   []byte `json:"challenge,omitempty"`
}

const (
	HandshakeModeXX       byte = 1
	HandshakeModeIK       byte = 2
	maxHandshakeFrameSize      = 64 * 1024
)

func ClientHandshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	mode := selectHandshakeMode(cfg, true)
	hs, err := newHandshakeState(cfg, true, mode)
	if err != nil {
		return nil, err
	}
	if err := writeHandshakeMode(ctx, conn, mode); err != nil {
		return nil, err
	}
	var remoteID, remoteKey [32]byte
	switch mode {
	case HandshakeModeIK:
		payload1, err := marshalIdentityPayload(cfg)
		if err != nil {
			return nil, err
		}
		msg1, _, _, err := hs.WriteMessage(nil, payload1)
		if err != nil {
			return nil, err
		}
		if err := writeFrame(ctx, conn, msg1); err != nil {
			return nil, err
		}
		msg2, err := readFrame(ctx, conn)
		if err != nil {
			return nil, err
		}
		payload2, cs1, cs2, err := hs.ReadMessage(nil, msg2)
		if err != nil {
			return nil, err
		}
		remoteKey = peerStaticArray(hs.PeerStatic())
		if err := verifyIdentityPayload(payload2, cfg.MeshID, remoteKey[:], &remoteID); err != nil {
			return nil, err
		}
		sendCipher, recvCipher := splitCipherStates(true, cs1, cs2)
		return NewSessionWithBuffers(newStreamCarrier(conn), sendCipher, recvCipher, remoteID, remoteKey, mode, cfg.Buffers)
	case HandshakeModeXX:
		msg1, _, _, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, err
		}
		if err := writeFrame(ctx, conn, msg1); err != nil {
			return nil, err
		}

		msg2, err := readFrame(ctx, conn)
		if err != nil {
			return nil, err
		}
		payload2, _, _, err := hs.ReadMessage(nil, msg2)
		if err != nil {
			return nil, err
		}
		remoteKey = peerStaticArray(hs.PeerStatic())
		if err := verifyIdentityPayload(payload2, cfg.MeshID, remoteKey[:], &remoteID); err != nil {
			return nil, err
		}

		payload3, err := marshalIdentityPayload(cfg)
		if err != nil {
			return nil, err
		}
		msg3, cs1, cs2, err := hs.WriteMessage(nil, payload3)
		if err != nil {
			return nil, err
		}
		if err := writeFrame(ctx, conn, msg3); err != nil {
			return nil, err
		}
		sendCipher, recvCipher := splitCipherStates(true, cs1, cs2)
		return NewSessionWithBuffers(newStreamCarrier(conn), sendCipher, recvCipher, remoteID, remoteKey, mode, cfg.Buffers)
	default:
		return nil, errors.New("unsupported handshake mode")
	}
}

func ServerHandshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	mode, err := readHandshakeMode(ctx, conn)
	if err != nil {
		return nil, err
	}
	hs, err := newHandshakeState(cfg, false, mode)
	if err != nil {
		return nil, err
	}
	var remoteID, remoteKey [32]byte
	switch mode {
	case HandshakeModeIK:
		msg1, err := readFrame(ctx, conn)
		if err != nil {
			return nil, err
		}
		payload1, _, _, err := hs.ReadMessage(nil, msg1)
		if err != nil {
			return nil, err
		}
		remoteKey = peerStaticArray(hs.PeerStatic())
		if err := verifyIdentityPayload(payload1, cfg.MeshID, remoteKey[:], &remoteID); err != nil {
			return nil, err
		}
		payload2, err := marshalIdentityPayload(cfg)
		if err != nil {
			return nil, err
		}
		msg2, cs1, cs2, err := hs.WriteMessage(nil, payload2)
		if err != nil {
			return nil, err
		}
		if err := writeFrame(ctx, conn, msg2); err != nil {
			return nil, err
		}
		sendCipher, recvCipher := splitCipherStates(false, cs1, cs2)
		return NewSessionWithBuffers(newStreamCarrier(conn), sendCipher, recvCipher, remoteID, remoteKey, mode, cfg.Buffers)
	case HandshakeModeXX:
		msg1, err := readFrame(ctx, conn)
		if err != nil {
			return nil, err
		}
		if _, _, _, err := hs.ReadMessage(nil, msg1); err != nil {
			return nil, err
		}

		payload2, err := marshalIdentityPayload(cfg)
		if err != nil {
			return nil, err
		}
		msg2, _, _, err := hs.WriteMessage(nil, payload2)
		if err != nil {
			return nil, err
		}
		if err := writeFrame(ctx, conn, msg2); err != nil {
			return nil, err
		}

		msg3, err := readFrame(ctx, conn)
		if err != nil {
			return nil, err
		}
		payload3, cs1, cs2, err := hs.ReadMessage(nil, msg3)
		if err != nil {
			return nil, err
		}
		remoteKey = peerStaticArray(hs.PeerStatic())
		if err := verifyIdentityPayload(payload3, cfg.MeshID, remoteKey[:], &remoteID); err != nil {
			return nil, err
		}
		sendCipher, recvCipher := splitCipherStates(false, cs1, cs2)
		return NewSessionWithBuffers(newStreamCarrier(conn), sendCipher, recvCipher, remoteID, remoteKey, mode, cfg.Buffers)
	default:
		return nil, errors.New("unsupported handshake mode")
	}
}

func newHandshakeState(cfg HandshakeConfig, initiator bool, mode byte) (*noise.HandshakeState, error) {
	if cfg.Identity == nil {
		return nil, errors.New("identity is required")
	}
	pattern := noise.HandshakeXX
	switch mode {
	case HandshakeModeXX:
		pattern = noise.HandshakeXX
	case HandshakeModeIK:
		pattern = noise.HandshakeIK
		if initiator && len(cfg.RemoteStatic) != 32 {
			return nil, errors.New("ik handshake requires cached remote static key")
		}
	default:
		return nil, errors.New("unsupported handshake mode")
	}
	config := noise.Config{
		CipherSuite:   noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s),
		Pattern:       pattern,
		Initiator:     initiator,
		Prologue:      []byte("moss|" + cfg.MeshID),
		StaticKeypair: cfg.Identity.NoiseStaticKeypair(),
	}
	if initiator && mode == HandshakeModeIK {
		config.PeerStatic = append([]byte(nil), cfg.RemoteStatic...)
	}
	if len(cfg.PSK) > 0 {
		if len(cfg.PSK) != 32 {
			return nil, errors.New("psk must be 32 bytes")
		}
		config.PresharedKey = append([]byte(nil), cfg.PSK...)
		config.PresharedKeyPlacement = 0
	}
	return noise.NewHandshakeState(config)
}

func marshalIdentityPayload(cfg HandshakeConfig) ([]byte, error) {
	return marshalIdentityPayloadWithChallenge(cfg, nil)
}

func marshalIdentityPayloadWithChallenge(cfg HandshakeConfig, challenge []byte) ([]byte, error) {
	payload := identityPayload{
		IdentityPub: cfg.Identity.PublicKeyBytes(),
		Signature:   cfg.Identity.Sign(signaturePayload(cfg.MeshID, cfg.Identity.NoiseStaticPublic())),
		Challenge:   append([]byte(nil), challenge...),
	}
	return json.Marshal(payload)
}

func verifyIdentityPayload(raw []byte, meshID string, remoteStatic []byte, out *[32]byte) error {
	_, err := verifyIdentityPayloadChallenge(raw, meshID, remoteStatic, out)
	return err
}

func verifyIdentityPayloadChallenge(raw []byte, meshID string, remoteStatic []byte, out *[32]byte) ([]byte, error) {
	var payload identityPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if len(payload.IdentityPub) != len(out) {
		return nil, errors.New("invalid identity length")
	}
	if len(payload.Signature) != 64 {
		return nil, errors.New("invalid identity signature length")
	}
	if !mcrypto.Verify(payload.IdentityPub, signaturePayload(meshID, remoteStatic), payload.Signature) {
		return nil, errors.New("invalid identity signature")
	}
	copy(out[:], payload.IdentityPub)
	return append([]byte(nil), payload.Challenge...), nil
}

func signaturePayload(meshID string, noiseStaticPublic []byte) []byte {
	out := append([]byte("moss-identity|"+meshID+"|"), noiseStaticPublic...)
	return out
}

func selectHandshakeMode(cfg HandshakeConfig, initiator bool) byte {
	if initiator && len(cfg.RemoteStatic) == 32 {
		return HandshakeModeIK
	}
	return HandshakeModeXX
}

func splitCipherStates(initiator bool, cs1, cs2 *noise.CipherState) (*noise.CipherState, *noise.CipherState) {
	if initiator {
		return cs1, cs2
	}
	return cs2, cs1
}

func peerStaticArray(raw []byte) [32]byte {
	var out [32]byte
	copy(out[:], raw)
	return out
}

func writeHandshakeMode(ctx context.Context, conn net.Conn, mode byte) error {
	return writeRaw(ctx, conn, []byte{mode})
}

func readHandshakeMode(ctx context.Context, conn net.Conn) (byte, error) {
	buf := make([]byte, 1)
	if err := readRaw(ctx, conn, buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func writeRaw(ctx context.Context, conn net.Conn, payload []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}
	return writeAll(conn, payload)
}

func readRaw(ctx context.Context, conn net.Conn, payload []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}
	_, err := io.ReadFull(conn, payload)
	return err
}

func writeFrame(ctx context.Context, conn net.Conn, payload []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if err := writeRaw(ctx, conn, header); err != nil {
		return err
	}
	return writeRaw(ctx, conn, payload)
}

func readFrame(ctx context.Context, conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if err := readRaw(ctx, conn, header); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header)
	if size == 0 {
		return nil, nil
	}
	if size > maxHandshakeFrameSize {
		return nil, errors.New("handshake frame too large")
	}
	payload := make([]byte, size)
	if err := readRaw(ctx, conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

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

	mcrypto "moss/internal/crypto"
)

type HandshakeConfig struct {
	MeshID   string
	PSK      []byte
	Identity *mcrypto.Identity
}

type identityPayload struct {
	IdentityPub []byte `json:"identity_pub"`
	Signature   []byte `json:"signature"`
}

func ClientHandshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	hs, err := newHandshakeState(cfg, true)
	if err != nil {
		return nil, err
	}
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
	var remoteID [32]byte
	if err := verifyIdentityPayload(payload2, cfg.MeshID, hs.PeerStatic(), &remoteID); err != nil {
		return nil, err
	}

	payload3, err := marshalIdentityPayload(cfg)
	if err != nil {
		return nil, err
	}
	msg3, sendCipher, recvCipher, err := hs.WriteMessage(nil, payload3)
	if err != nil {
		return nil, err
	}
	if err := writeFrame(ctx, conn, msg3); err != nil {
		return nil, err
	}
	return NewSession(newStreamCarrier(conn), sendCipher, recvCipher, remoteID)
}

func ServerHandshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	hs, err := newHandshakeState(cfg, false)
	if err != nil {
		return nil, err
	}
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
	payload3, recvCipher, sendCipher, err := hs.ReadMessage(nil, msg3)
	if err != nil {
		return nil, err
	}
	var remoteID [32]byte
	if err := verifyIdentityPayload(payload3, cfg.MeshID, hs.PeerStatic(), &remoteID); err != nil {
		return nil, err
	}
	return NewSession(newStreamCarrier(conn), sendCipher, recvCipher, remoteID)
}

func newHandshakeState(cfg HandshakeConfig, initiator bool) (*noise.HandshakeState, error) {
	if cfg.Identity == nil {
		return nil, errors.New("identity is required")
	}
	config := noise.Config{
		CipherSuite:   noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s),
		Pattern:       noise.HandshakeXX,
		Initiator:     initiator,
		Prologue:      []byte("moss|" + cfg.MeshID),
		StaticKeypair: cfg.Identity.NoiseStaticKeypair(),
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
	payload := identityPayload{
		IdentityPub: cfg.Identity.PublicKeyBytes(),
		Signature:   cfg.Identity.Sign(signaturePayload(cfg.MeshID, cfg.Identity.NoiseStaticPublic())),
	}
	return json.Marshal(payload)
}

func verifyIdentityPayload(raw []byte, meshID string, remoteStatic []byte, out *[32]byte) error {
	var payload identityPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	if len(payload.IdentityPub) != len(out) {
		return errors.New("invalid identity length")
	}
	if !mcrypto.Verify(payload.IdentityPub, signaturePayload(meshID, remoteStatic), payload.Signature) {
		return errors.New("invalid identity signature")
	}
	copy(out[:], payload.IdentityPub)
	return nil
}

func signaturePayload(meshID string, noiseStaticPublic []byte) []byte {
	out := append([]byte("moss-identity|"+meshID+"|"), noiseStaticPublic...)
	return out
}

func writeFrame(ctx context.Context, conn net.Conn, payload []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func readFrame(ctx context.Context, conn net.Conn) ([]byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header)
	if size == 0 {
		return nil, nil
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

package transport

import (
	"context"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"

	mcrypto "moss/internal/crypto"
)

type HandshakeConfig struct {
	MeshID   string
	PSK      []byte
	Identity *mcrypto.Identity
}

type handshakeHello struct {
	MeshID      string `json:"mesh_id"`
	IdentityPub []byte `json:"identity_pub"`
	Ephemeral   []byte `json:"ephemeral"`
	Signature   []byte `json:"signature"`
}

type proofMessage struct {
	Proof []byte `json:"proof"`
	Ack   bool   `json:"ack"`
}

func ClientHandshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	eph, ephPub, err := mcrypto.NewECDHKeypair()
	if err != nil {
		return nil, err
	}
	hello := handshakeHello{
		MeshID:      cfg.MeshID,
		IdentityPub: cfg.Identity.PublicKeyBytes(),
		Ephemeral:   ephPub,
	}
	hello.Signature = cfg.Identity.Sign(signBytes("client", cfg.MeshID, ephPub, nil))
	if err := writeJSON(ctx, conn, hello); err != nil {
		return nil, err
	}
	var response handshakeHello
	if err := readJSON(ctx, conn, &response); err != nil {
		return nil, err
	}
	if err := verifyHello("server", cfg.MeshID, response, ephPub); err != nil {
		return nil, err
	}
	session, proofKey, err := buildSession(conn, cfg, eph, response)
	if err != nil {
		return nil, err
	}
	if err := session.writeProof(cfg.MeshID, cfg.PSK, proofKey, true); err != nil {
		return nil, err
	}
	ok, err := session.readProof(cfg.MeshID, cfg.PSK, proofKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("mesh rejected")
	}
	return session, nil
}

func ServerHandshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*Session, error) {
	var hello handshakeHello
	if err := readJSON(ctx, conn, &hello); err != nil {
		return nil, err
	}
	if err := verifyHello("client", cfg.MeshID, hello, nil); err != nil {
		return nil, err
	}
	eph, ephPub, err := mcrypto.NewECDHKeypair()
	if err != nil {
		return nil, err
	}
	response := handshakeHello{
		MeshID:      cfg.MeshID,
		IdentityPub: cfg.Identity.PublicKeyBytes(),
		Ephemeral:   ephPub,
	}
	response.Signature = cfg.Identity.Sign(signBytes("server", cfg.MeshID, ephPub, hello.Ephemeral))
	if err := writeJSON(ctx, conn, response); err != nil {
		return nil, err
	}
	session, proofKey, err := buildSession(conn, cfg, eph, hello)
	if err != nil {
		return nil, err
	}
	ok, err := session.readProof(cfg.MeshID, cfg.PSK, proofKey)
	if err != nil {
		return nil, err
	}
	if err := session.writeProof(cfg.MeshID, cfg.PSK, proofKey, ok); err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("mesh rejected")
	}
	return session, nil
}

func verifyHello(role, expectedMesh string, hello handshakeHello, peerEphemeral []byte) error {
	if hello.MeshID != expectedMesh {
		return errors.New("mesh mismatch")
	}
	if !mcrypto.Verify(hello.IdentityPub, signBytes(role, hello.MeshID, hello.Ephemeral, peerEphemeral), hello.Signature) {
		return errors.New("invalid handshake signature")
	}
	return nil
}

func buildSession(conn net.Conn, cfg HandshakeConfig, private *ecdh.PrivateKey, peer handshakeHello) (*Session, []byte, error) {
	remotePub, err := mcrypto.ParseECDHPublicKey(peer.Ephemeral)
	if err != nil {
		return nil, nil, err
	}
	shared, err := private.ECDH(remotePub)
	if err != nil {
		return nil, nil, err
	}
	salt := append([]byte("moss-session|"+cfg.MeshID), cfg.PSK...)
	aesKey, err := mcrypto.Expand(shared, salt, "aes")
	if err != nil {
		return nil, nil, err
	}
	proofKey, err := mcrypto.Expand(shared, salt, "proof")
	if err != nil {
		return nil, nil, err
	}
	var remoteID [32]byte
	copy(remoteID[:], peer.IdentityPub)
	session, err := NewSession(conn, aesKey, remoteID)
	if err != nil {
		return nil, nil, err
	}
	return session, proofKey, nil
}

func signBytes(role, meshID string, ephemeral, peerEphemeral []byte) []byte {
	out := append([]byte("moss-"+role+"|"+meshID+"|"), ephemeral...)
	if len(peerEphemeral) > 0 {
		out = append(out, peerEphemeral...)
	}
	return out
}

func (s *Session) writeProof(meshID string, psk, proofKey []byte, ack bool) error {
	mac := hmac.New(sha256.New, proofKey)
	mac.Write([]byte(meshID))
	mac.Write(psk)
	message, err := json.Marshal(proofMessage{Proof: mac.Sum(nil), Ack: ack})
	if err != nil {
		return err
	}
	return s.WritePacket(message)
}

func (s *Session) readProof(meshID string, psk, proofKey []byte) (bool, error) {
	packet, err := s.ReadPacket()
	if err != nil {
		return false, err
	}
	var proof proofMessage
	if err := json.Unmarshal(packet, &proof); err != nil {
		return false, err
	}
	mac := hmac.New(sha256.New, proofKey)
	mac.Write([]byte(meshID))
	mac.Write(psk)
	return proof.Ack && hmac.Equal(proof.Proof, mac.Sum(nil)), nil
}

func writeJSON(ctx context.Context, conn net.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err = conn.Write(payload)
	return err
}

func readJSON(ctx context.Context, conn net.Conn, out any) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header)
	payload := make([]byte, size)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return err
	}
	return json.Unmarshal(payload, out)
}

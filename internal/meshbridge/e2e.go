package meshbridge

import (
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/tun"
	"golang.org/x/crypto/chacha20poly1305"
)

// TunBridge is the end-to-end encryption glue between a local
// tun.PacketIface and the bridged channel: TUN packets ride the whole
// bridge path — pump, Link, far pump's re-Publish, the moss mesh — as
// room-sealed blobs that only a holder of the room PSK can open.
//
// Path (design option (a), room-broadcast, no directed SendToPeer):
//
//	near host: iface.ReadPacket → glue seal → pump.PublishBridge → Link
//	           (ciphertext on the wire, MBRIDGE frames of a sealed blob)
//	far pump:  Link frame → reassemble → pump.publishOnChannel →
//	           node.Publish — the REAL mesh publish path, which
//	           room-seals again and floods the room topic
//	far host:  mesh messageCB → glue open → iface.WritePacket
//
// Two AEAD layers therefore exist on the mesh legs: the room seal mesh
// itself applies in Publish/deliverLocal (outer) and the glue seal made
// here (inner). That double seal is the price of end-to-end secrecy
// without a public "seal this blob" API in package mesh — sealRoom /
// sealRoomIn are unexported, so the bridge mirrors their exact
// derivation and format instead of inventing a second convention.
//
// Trust perimeter: the room is a broadcast medium. The glue key IS the
// room key, so every holder of the room PSK — including both gateways,
// whose pumps are transport, not authors — can open the inner seal.
// The gateways do not open it in code: the near gateway relays sealed
// bytes blind, the far gateway re-Publishes without decrypting; only
// the receiving host's glue opens the packet layer. But "does not" is
// a code path, not a key boundary: E2E here means "opaque on the Link
// wire and at every relay point that lacks the PSK", not "invisible
// to PSK holders". A members-only perimeter needs the invitation path
// (CreateRoomInvite), which is out of scope.
//
// Wire budget: the glue seal adds chacha20poly1305 overhead — 12-byte
// nonce + 16-byte tag = 28 bytes — BEFORE MBRIDGE encoding. A
// DefaultMTU (1500) packet becomes a 1528-byte blob = exactly 8
// frames of MBRIPayloadMax (191) bytes, within PublishBridge's
// 255-frame budget and the mesh's 64KB MaxMessageSizeBytes.
//
// Gaps, fixed by MVP scope: the pump never consumes mesh-side
// publishes (this glue splices messageCB itself, so a channel with
// BOTH human publishers and a bridge needs the glue to pass foreign
// traffic to prev); publishOnChannel publishes into the pump node's
// OWN room, so gateway nodes must be constructed with
// mesh.NewNode(room, psk, ...) rather than joining later; two bridge
// pairs sharing a substrate and channel would loop (anti-loop is the
// gateway-ID check on the Link leg only), so the MVP is one pair and
// gateways never Subscribe the bridged channel; the messageCB contract
// is non-blocking, so a slow real-TUN WritePacket can stall the
// channel's delivery worker (tun.Loopback never blocks — safe here,
// documented risk on real devices); an empty PSK is rejected because
// the legacy no-PSK room key derives from the public room NAME.

// TunBridge glues one packet interface to the bridged channel. A
// bridge with a pump is an egress side (iface → mesh); a bridge with
// pump == nil is ingress-only (mesh → iface). Both sides splice the
// node's message callback, so the same type serves the near host
// (egress, nothing arrives for it because it never subscribes) and
// the far host (ingress, nothing leaves because there is no pump).
type TunBridge struct {
	node  *mesh.Node
	pump  *Pump
	iface tun.PacketIface

	// Topic is the moss channel whose traffic carries sealed TUN
	// packets. Zero means DefaultTopic. It must match the pump's
	// topic on the other side of the Link.
	Topic string

	// aead is the room AEAD built once at attach from the same
	// derivation mesh uses (bridgeRoomKey).
	aead cipher.AEAD

	mu       sync.Mutex
	stop     chan struct{}
	done     chan struct{}
	attached bool

	// prev is the message callback found at attach time, restored by
	// Detach — the same caller-supplies-prev contract as AttachPump,
	// for the same reason: the mesh API has no callback getter.
	prev mesh.MessageCallback

	// Counters: monotonic, only grow.
	packetsEgress      atomic.Uint64 // packets sealed and PublishBridge'd
	packetsIngress     atomic.Uint64 // sealed blobs opened and written to the iface
	egressErrors       atomic.Uint64 // seal or publish failures on the egress loop
	ingressDropped     atomic.Uint64 // undecodable or unwritable ingress blobs
	ingressPassthrough atomic.Uint64 // foreign-channel callbacks forwarded to prev
}

// AttachTunBridge seals iface's packets into the room and splices the
// node's message callback for the return direction. With pump != nil
// an egress goroutine reads the iface, seals each packet under the
// room key and PublishBridge's the blob; it exits on Detach, iface
// Close or Link Close. With pump == nil no goroutine starts — the
// bridge is the far host's receive half only.
//
// room and psk select the same key mesh derives for the room
// (bit-for-bit: bridgeRoomKey mirrors mesh's deriveRoomKey). An empty
// room or empty PSK is an error: the no-PSK legacy path would derive
// the "room key" from the public room name and the seal would protect
// nothing.
//
// prev is the message callback to preserve for non-bridged channels
// (nil is legal). It is restored verbatim by Detach. Do not register
// another message callback between attach and Detach — last writer
// wins in SetMessageCallback's contract, same as AttachPump.
func AttachTunBridge(node *mesh.Node, pump *Pump, iface tun.PacketIface, room string, psk []byte, prev mesh.MessageCallback) (*TunBridge, error) {
	if node == nil {
		return nil, errors.New("mbridge/tun: node is required")
	}
	if iface == nil {
		return nil, errors.New("mbridge/tun: iface is required")
	}
	if room == "" {
		return nil, errors.New("mbridge/tun: room is required")
	}
	if len(psk) == 0 {
		return nil, errors.New("mbridge/tun: room PSK is required — without it the key derives from the public room name")
	}
	key, err := bridgeRoomKey(room, psk)
	if err != nil {
		return nil, err
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}
	t := &TunBridge{
		node:     node,
		pump:     pump,
		iface:    iface,
		Topic:    DefaultTopic,
		aead:     aead,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		attached: true,
		prev:     prev,
	}
	if pump != nil {
		go func() {
			defer close(t.done)
			t.egressLoop()
		}()
	} else {
		close(t.done) // no goroutine until there is a pump to serve
	}
	node.SetMessageCallback(t.messageSplice)
	return t, nil
}

// Detach reverses AttachTunBridge: the egress goroutine (if any) is
// stopped and reaped — the iface Close is what unblocks its ReadPacket
// — and the node's message callback is restored to prev exactly as
// supplied at attach. Detach is idempotent. Registering a different
// message callback between AttachTunBridge and Detach forfeits the
// restore; Detach still puts prev back.
func (t *TunBridge) Detach() {
	t.mu.Lock()
	if !t.attached {
		t.mu.Unlock()
		return
	}
	t.attached = false
	select {
	case <-t.stop:
		// Already closed by a previous Detach that lost the attached
		// race — impossible (attached guard), but the select keeps
		// close idempotent for free.
	default:
		close(t.stop)
	}
	t.mu.Unlock()

	// Unblock the egress loop's ReadPacket, then reap it.
	_ = t.iface.Close()
	<-t.done

	t.mu.Lock()
	prev := t.prev
	t.mu.Unlock()
	t.node.SetMessageCallback(prev)
}

// egressLoop is the iface → mesh half: read, seal, PublishBridge. It
// exits on Detach (stop closed, then iface Close unblocks the read),
// on iface Close by anyone else, or when PublishBridge fails for good
// (a closed Link ends the bridge — there is no Link left to serve).
func (t *TunBridge) egressLoop() {
	for {
		pkt, err := t.iface.ReadPacket()
		if err != nil {
			return
		}
		select {
		case <-t.stop:
			return
		default:
		}
		sealed, err := t.sealPacket(pkt)
		if err != nil {
			t.egressErrors.Add(1)
			continue
		}
		if err := t.pump.PublishBridge(sealed); err != nil {
			t.egressErrors.Add(1)
			return
		}
		t.packetsEgress.Add(1)
	}
}

// messageSplice is the head of the node's message chain while
// attached: blobs on the bridged channel are glue-opened and written
// to the iface, every other channel reaches prev unchanged. The
// callback contract is non-blocking (mesh runs it on the channel's
// delivery worker): tun.Loopback never blocks, a slow real TUN device
// can stall that worker — a documented MVP risk.
func (t *TunBridge) messageSplice(channel string, sender [32]byte, data []byte) {
	if channel != t.dataTopic() {
		t.ingressPassthrough.Add(1)
		t.mu.Lock()
		prev := t.prev
		t.mu.Unlock()
		if prev != nil {
			prev(channel, sender, data)
		}
		return
	}
	pkt, err := t.openPacket(data)
	if err != nil {
		t.ingressDropped.Add(1)
		return
	}
	if err := t.iface.WritePacket(pkt); err != nil {
		t.ingressDropped.Add(1)
		return
	}
	t.packetsIngress.Add(1)
}

// dataTopic is the moss channel this bridge listens on.
func (t *TunBridge) dataTopic() string {
	if t.Topic == "" {
		return DefaultTopic
	}
	return t.Topic
}

// sealPacket encrypts one packet under the room key. It is a
// bit-for-bit mirror of mesh's sealRoomIn (node_room.go): fresh random
// nonce, nonce used as both dst prefix and AAD-source, nil additional
// data. Output: nonce ‖ ciphertext.
func (t *TunBridge) sealPacket(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return t.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// openPacket reverses sealPacket, mirroring mesh's openRoom: shorter
// than a nonce is undecodable, a failed AEAD open is a drop, never a
// delivery.
func (t *TunBridge) openPacket(payload []byte) ([]byte, error) {
	if len(payload) < chacha20poly1305.NonceSize {
		return nil, errors.New("mbridge/tun: blob shorter than a nonce")
	}
	nonce, ciphertext := payload[:chacha20poly1305.NonceSize], payload[chacha20poly1305.NonceSize:]
	return t.aead.Open(nil, nonce, ciphertext, nil)
}

// bridgeRoomKey derives the 32-byte room key EXACTLY as mesh's
// deriveRoomKey does with a non-empty PSK — same HKDF, same salt, same
// info string — so the glue's seal is interchangeable with mesh's own
// room seal for every holder of the room PSK. The room name must be
// non-empty (a roomless node has no key to mirror) and the PSK must
// be non-empty (the no-PSK legacy path derives from the public room
// NAME; sealing under it would be cosmetic).
func bridgeRoomKey(room string, psk []byte) ([]byte, error) {
	if room == "" {
		return nil, errors.New("mbridge/tun: room is required")
	}
	if len(psk) == 0 {
		return nil, errors.New("mbridge/tun: room PSK is required")
	}
	return mcrypto.Expand(psk, []byte(room), "moss-room-v1")
}

// Counters, all monotonic (they only grow):

func (t *TunBridge) PacketsEgress() uint64      { return t.packetsEgress.Load() }
func (t *TunBridge) PacketsIngress() uint64     { return t.packetsIngress.Load() }
func (t *TunBridge) EgressErrors() uint64       { return t.egressErrors.Load() }
func (t *TunBridge) IngressDropped() uint64     { return t.ingressDropped.Load() }
func (t *TunBridge) IngressPassthrough() uint64 { return t.ingressPassthrough.Load() }

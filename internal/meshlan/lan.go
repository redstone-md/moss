// meshlan lan.go: LanNode, the moss-lan product seam. One LanNode stitches
// the three core surfaces into a participant: an intranet edge (AttachTun
// over a tun.PacketIface the caller injects — a kernel TUN device in a full
// product, a tun.Loopback in the MVP), presence discovery (heartbeat +
// NickTable on the room's "lan:presence" topic), and the invite flow
// (invite.go over mesh.CreateRoomInvite/AcceptRoomInvite).
//
// Ownership is strict: the *mesh.Node is constructed, started and stopped
// by the CALLER. LanNode never starts or stops it — Start/Stop here manage
// only what LanNode owns: the tun attachment and the presence ticker.
package meshlan

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/tun"
)

// LanNode is one moss-lan participant over a caller-owned mesh node.
type LanNode struct {
	node   *mesh.Node
	meshID string
	nick   string
	cidr   string

	// iface is the local edge; the caller injects it (Loopback in the
	// MVP CLI, a kernel TUN device in a fuller product).
	iface tun.PacketIface

	// selfIP is our own virtual intranet address, assigned at Start via
	// mesh.PeerAddr on our own peer ID.
	selfIP net.IP

	presence *Presence

	mu       sync.Mutex
	started  bool
	stopped  bool
	stopOnce sync.Once
}

// Stats is a snapshot of LanNode's monotonic counters.
type Stats struct {
	// Presence carries the discovery layer's counters
	// (heartbeats sent/seen/rejected, evictions).
	Presence PresenceStats
}

// NewLanNode builds a moss-lan participant over node. node must be a live,
// started mesh.Node in the target room (meshID non-empty, joined — with a
// PSK via mesh.JoinRoom, or born into it). iface is the local packet edge
// injected by the caller: meshlan attaches it with mesh.AttachTun at Start
// and detaches at Stop, closing it in the process (DetachTun closes the
// interface; a stopped interface is not reusable). cidr is the intranet
// address pool, e.g. "10.66.0.0/24".
//
// nick is validated; psk is unused when the caller already arranged room
// membership — moss-lan does not join rooms on the node's behalf, it only
// needs the node to be in one.
func NewLanNode(node *mesh.Node, nick, meshID, cidr string, iface tun.PacketIface) (*LanNode, error) {
	if node == nil {
		return nil, errors.New("mesh node is required")
	}
	if err := ValidateNick(nick); err != nil {
		return nil, err
	}
	if meshID == "" {
		return nil, errors.New("mesh ID is required")
	}
	if iface == nil {
		return nil, errors.New("packet interface is required")
	}
	if _, err := tun.NewTable(cidr); err != nil {
		return nil, err
	}
	return &LanNode{
		node:   node,
		meshID: meshID,
		nick:   nick,
		cidr:   cidr,
		iface:  iface,
	}, nil
}

// Start attaches the intranet edge, assigns this node's own virtual IP,
// wires presence, and starts heartbeating. It must be called after
// node.Start(). Once started, the node's message callback routes
// "lan:presence" deliveries into the NickTable — but the callback is
// REPLACED by a chaining wrapper, so the node must have no application
// callback registered between Start and Stop (the same constraint
// mesh.AttachTun documents for SetPacketCallback).
func (l *LanNode) Start() error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return errors.New("lan node already started")
	}
	if l.stopped {
		l.mu.Unlock()
		return errors.New("lan node already stopped")
	}
	l.mu.Unlock()

	// Attach the edge: packets from the interface route to peers, directed
	// payloads from peers land on the interface.
	if err := mesh.AttachTun(l.node, l.iface, l.cidr); err != nil {
		return err
	}
	// Assign our own virtual IP from the pool — read-only consumption of
	// mesh.PeerAddr. The peer ID is the hex of our public key.
	peerID := l.peerID()
	selfIP, err := mesh.PeerAddr(l.node, peerID)
	if err != nil {
		_ = mesh.DetachTun(l.node)
		return err
	}

	// Presence: heartbeat {nick, peerID, selfIP} on the room's presence
	// topic. Publish goes through the node so the room seals it.
	presence, err := NewPresence(l.nick, peerID, selfIP, func(channel string, data []byte) int32 {
		return l.node.PublishRoom(l.meshID, channel, data)
	})
	if err != nil {
		_ = mesh.DetachTun(l.node)
		return err
	}
	l.presence = presence

	// Take over the message callback for the lifetime of the LanNode: the
	// node exposes a single callback slot and no getter to chain by hand,
	// so between Start and Stop the callback belongs to moss-lan — the
	// same one-slot constraint mesh.AttachTun documents for
	// SetPacketCallback. Register it BEFORE the subscription so the
	// first heartbeat this node receives cannot race a nil callback.
	l.node.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == PresenceTopic {
			presence.Observe(data)
		}
	})

	// Subscribe the presence topic inside the room: without the
	// subscription nothing is ever delivered, and the topic resolves
	// only if the node holds the room key — a create-side node born into
	// the room with its PSK, or a join-side node that accepted the invite
	// first. Fail closed: a LanNode that cannot hear peers is not a LAN.
	if code := l.node.SubscribeRoom(l.meshID, PresenceTopic); code != mesh.MOSS_OK {
		l.node.SetMessageCallback(nil)
		l.presence = nil
		_ = mesh.DetachTun(l.node)
		return fmt.Errorf("cannot subscribe presence topic (room key missing? code %d)", code)
	}

	l.selfIP = selfIP
	l.mu.Lock()
	l.started = true
	l.mu.Unlock()

	presence.Start()
	return nil
}

// Stop ends everything LanNode started: the presence ticker, the message
// callback, and the tun attachment (which closes the injected interface —
// the mesh.Node itself is untouched and keeps running; the caller owns its
// lifecycle). Idempotent.
func (l *LanNode) Stop() {
	l.stopOnce.Do(func() {
		l.mu.Lock()
		started := l.started
		l.stopped = true
		l.mu.Unlock()
		if !started {
			return
		}
		if l.presence != nil {
			l.presence.Stop()
		}
		l.node.SetMessageCallback(nil)
		// DetachTun closes l.iface (idempotent per the PacketIface
		// contract) and restores the pre-attach packet callback.
		_ = mesh.DetachTun(l.node)
	})
}

// peerID derives this node's own peer ID the way the core does: the hex of
// the Ed25519 public key (PublicKey is the exported read-only accessor;
// the core's localPeerID is unexported, and its format is pinned by the
// core as hex.EncodeToString of the 32-byte key).
func (l *LanNode) peerID() string {
	pub := l.node.PublicKey()
	return hex.EncodeToString(pub[:])
}

// SelfIP returns this node's own virtual intranet address (assigned at
// Start; nil before).
func (l *LanNode) SelfIP() net.IP {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append(net.IP(nil), l.selfIP...)
}

// Nick returns the local nickname.
func (l *LanNode) Nick() string { return l.nick }

// MeshID returns the room id this participant was built for.
func (l *LanNode) MeshID() string { return l.meshID }

// ResolveNick returns the virtual IP of a remote participant by nickname,
// via the presence NickTable.
func (l *LanNode) ResolveNick(nick string) (net.IP, error) {
	if l.presence == nil {
		return nil, errors.New("lan node is not started")
	}
	return l.presence.ResolveNick(nick)
}

// Peers returns a snapshot of live remote participants (sorted by nick).
func (l *LanNode) Peers() []PeerInfo {
	if l.presence == nil {
		return nil
	}
	return l.presence.Peers()
}

// Stats returns a snapshot of this node's counters (presence included).
func (l *LanNode) Stats() Stats {
	if l.presence == nil {
		return Stats{}
	}
	return Stats{Presence: l.presence.Stats()}
}

// MakeInvite packs a fresh creator-side room invite (mesh.CreateRoomInvite)
// for inviteePeerID into a moss-lan:// string. The creator must know the
// invitee's noise static (the state peering builds); this is the same
// contract as the core.
func (l *LanNode) MakeInvite(inviteePeerID string) (string, error) {
	inviteBytes, err := l.node.CreateRoomInvite(l.meshID, inviteePeerID)
	if err != nil {
		return "", err
	}
	return PackInvite(l.meshID, inviteBytes)
}

// AcceptInvite unpacks a moss-lan:// invite string and joins its room on
// the underlying node (mesh.AcceptRoomInvite). The invite is for this
// node's peer ID or it fails closed, per the core's contract.
func (l *LanNode) AcceptInvite(inviteStr string) error {
	meshID, inviteBytes, err := UnpackInvite(inviteStr)
	if err != nil {
		return err
	}
	if err := l.node.AcceptRoomInvite(inviteBytes); err != nil {
		return err
	}
	l.mu.Lock()
	l.meshID = meshID
	l.mu.Unlock()
	return nil
}

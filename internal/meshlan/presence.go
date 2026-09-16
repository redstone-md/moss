// Package meshlan is moss-lan: a LAN-shaped product layered on the moss
// core. The core (internal/mesh, internal/tun, …) is consumed read-only;
// everything here is glue — discovery of peers by nickname, an invite
// string exchangeable out of band, and one LanNode that stitches them.
//
// presence.go implements the discovery layer: every participant heartbeats
// {nick, peerID, virtualIP} on the "lan:presence" topic inside the room, and
// every participant keeps a NickTable — nick → {peerID, ip, lastSeen} — with
// a TTL sweep, so a peer that disappears from the mesh stops resolving
// presenceTTL after its last heartbeat.
package meshlan

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// PresenceTopic is the room channel presence heartbeats are published on.
// It is an ordinary room channel: sealed under the room key, invisible to
// non-members.
const PresenceTopic = "lan:presence"

const (
	// PresenceInterval is how often a started participant re-announces
	// itself. Three heartbeats fit inside the TTL, so a single lost
	// publish does not evict a live peer from remote tables.
	PresenceInterval = 15 * time.Second

	// PresenceTTL is how long a table entry survives after its last
	// heartbeat. 3× the interval: two consecutive losses are absorbed.
	PresenceTTL = 45 * time.Second

	// NickTableCap bounds the NickTable. A room that outgrows this evicts
	// its stalest entry instead of growing unbounded; 256 is far beyond a
	// real LAN and small enough to sweep instantly.
	NickTableCap = 256

	// presenceFreshWindow bounds how far a heartbeat's timestamp may lie
	// from local now, in either direction: a staler beat is a replay, a
	// further-future one is a lie. Generous enough for real clock skew.
	presenceFreshWindow = 60 * time.Second
)

// ValidateNick checks a nickname: 1–32 runes, ASCII letters, digits,
// underscore and hyphen only. The same rule is applied to remote
// heartbeats, so a hostile peer cannot fill the table with unprintable
// or empty keys.
func ValidateNick(nick string) error {
	if nick == "" {
		return errors.New("nick is empty")
	}
	if utf8.RuneCountInString(nick) > 32 {
		return errors.New("nick is longer than 32 runes")
	}
	for _, r := range nick {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '-':
		default:
			return fmt.Errorf("nick contains disallowed rune %q", r)
		}
	}
	return nil
}

// presenceEnvelope is the heartbeat wire format, published as JSON on
// PresenceTopic. The ts field lets a receiver reject replayed or
// far-future heartbeats; virtualIP is the sender's own address inside the
// intranet pool, assigned by the sender through mesh.PeerAddr on its own
// peer ID.
type presenceEnvelope struct {
	Nick      string `json:"nick"`
	PeerID    string `json:"peer_id"`
	VirtualIP string `json:"virtual_ip"`
	TS        int64  `json:"ts"`
}

// PeerInfo is one remote participant as seen by the local NickTable.
type PeerInfo struct {
	Nick     string
	PeerID   string
	IP       net.IP
	LastSeen time.Time
}

// PresenceStats is a plain snapshot of the discovery layer's monotonic
// counters. Every counter only ever grows.
type PresenceStats struct {
	// HeartbeatsSent counts local heartbeats accepted by the room
	// (publish returned MOSS_OK).
	HeartbeatsSent uint64
	// HeartbeatFails counts publish attempts the room rejected
	// (no peers, not started, marshal failure).
	HeartbeatFails uint64
	// HeartbeatsSeen counts remote heartbeats accepted into the table.
	HeartbeatsSeen uint64
	// HeartbeatsRejected counts remote heartbeats dropped: malformed
	// JSON, bad nick, bad address, stale or far-future timestamp, or a
	// full table that could not evict.
	HeartbeatsRejected uint64
	// Evictions counts TTL expiries plus cap-driven evictions.
	Evictions uint64
}

// presenceCounters are the atomic write side of PresenceStats. Snapshots
// are taken field by field, so the atomics are never copied.
type presenceCounters struct {
	heartbeatsSent     atomic.Uint64
	heartbeatFails     atomic.Uint64
	heartbeatsSeen     atomic.Uint64
	heartbeatsRejected atomic.Uint64
	evictions          atomic.Uint64
}

// PublishFunc publishes data on a channel inside the room. LanNode wires
// it to (*mesh.Node).Publish; the int32 is the mesh error code, 0 = OK.
type PublishFunc func(channel string, data []byte) int32

// Presence handles one node's discovery layer: heartbeating its own nick
// and maintaining the remote NickTable. The zero value is not usable; use
// NewPresence.
type Presence struct {
	nick   string
	peerID string
	selfIP net.IP

	// publish sends a heartbeat on PresenceTopic.
	publish PublishFunc

	// now is swappable for tests; production gets time.Now.
	now func() time.Time

	// interval is the heartbeat period; tests may shorten it after
	// construction (production keeps PresenceInterval).
	interval time.Duration

	mu      sync.Mutex // guards table
	table   map[string]PeerInfo
	counts  presenceCounters
	stop    chan struct{}
	done    chan struct{}
	started bool // guarded by mu; set when the ticker launches

	// onPeer, when set, is called for every accepted remote heartbeat
	// with that peer's ID and self-reported virtual IP. LanNode wires it
	// to register the address into the routing table so packets to a
	// discovered peer resolve without a separate lookup. It runs outside
	// the table lock.
	onPeer func(peerID string, ip net.IP)

	// startOnce / stopOnce make Start and Stop idempotent.
	startOnce sync.Once
	stopOnce  sync.Once
}

// NewPresence builds the discovery layer for one participant. nick is
// validated eagerly; peerID is the node's own peer ID (hex of its Ed25519
// public key); selfIP is the node's own virtual intranet address, assigned
// via mesh.PeerAddr(node, ownPeerID); publish is called on every heartbeat
// tick — wire it to (*mesh.Node).Publish. Call Start to begin heartbeating.
// The caller owns the node's lifecycle; Presence owns only its ticker
// goroutine, which Start launches and Stop ends.
func NewPresence(nick, peerID string, selfIP net.IP, publish PublishFunc) (*Presence, error) {
	if err := ValidateNick(nick); err != nil {
		return nil, err
	}
	if peerID == "" {
		return nil, errors.New("peer ID is empty")
	}
	if selfIP == nil || selfIP.To4() == nil {
		return nil, errors.New("self virtual IP must be IPv4")
	}
	if publish == nil {
		return nil, errors.New("publish function is required")
	}
	return &Presence{
		nick:     nick,
		peerID:   peerID,
		selfIP:   append(net.IP(nil), selfIP.To4()...),
		publish:  publish,
		now:      time.Now,
		interval: PresenceInterval,
		table:    make(map[string]PeerInfo, NickTableCap),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}, nil
}

// Nick returns the local nickname.
func (p *Presence) Nick() string { return p.nick }

// SelfIP returns the local virtual intranet address.
func (p *Presence) SelfIP() net.IP { return append(net.IP(nil), p.selfIP...) }

// Start launches the heartbeat ticker. The first beat goes out
// immediately, so a joiner discovers the room's peers without waiting a
// full interval. Idempotent.
func (p *Presence) Start() {
	p.startOnce.Do(func() {
		p.mu.Lock()
		p.started = true
		p.mu.Unlock()
		go func() {
			defer close(p.done)
			p.beatOnce()
			t := time.NewTicker(p.interval)
			defer t.Stop()
			for {
				select {
				case <-p.stop:
					return
				case <-t.C:
					p.beatOnce()
				}
			}
		}()
	})
}

// Stop ends the ticker goroutine and waits for its exit. Idempotent; safe
// without a prior Start (in which case it just returns). Stop then Start
// is not supported — a stopped Presence is dead; build a new one.
func (p *Presence) Stop() {
	p.stopOnce.Do(func() { close(p.stop) })
	p.mu.Lock()
	started := p.started
	p.mu.Unlock()
	if started {
		<-p.done
	}
}

// beatOnce publishes one heartbeat. A non-zero code counts as a fail; the
// next tick retries. Publishing needs no lock: publish must be safe to
// call from any goroutine (mesh.Publish is).
func (p *Presence) beatOnce() {
	env := presenceEnvelope{
		Nick:      p.nick,
		PeerID:    p.peerID,
		VirtualIP: p.selfIP.String(),
		TS:        p.now().Unix(),
	}
	data, err := json.Marshal(env)
	if err != nil {
		// A struct of primitives marshals or the process is broken.
		p.counts.heartbeatFails.Add(1)
		return
	}
	if p.publish(PresenceTopic, data) == 0 {
		p.counts.heartbeatsSent.Add(1)
	} else {
		p.counts.heartbeatFails.Add(1)
	}
}

// Observe feeds one remote heartbeat — the payload of a message delivered
// on PresenceTopic — into the NickTable. LanNode routes its message
// callback here. The payload is untrusted: malformed JSON, a bad nick, a
// missing peer ID, a bad address, or a timestamp outside the freshness
// window is counted and dropped. The table is nick-keyed: a newer
// heartbeat for a nick wins over older state. Two participants using the
// same nick are therefore last-writer-wins — an inherent property of
// nick-keyed discovery, and the room key already gates who can heartbeat
// at all. The sender's own heartbeat echoed back by the room (Publish
// delivers locally too) is ignored, so the table holds only remote peers.
func (p *Presence) Observe(data []byte) {
	var env presenceEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		p.counts.heartbeatsRejected.Add(1)
		return
	}
	if err := ValidateNick(env.Nick); err != nil {
		p.counts.heartbeatsRejected.Add(1)
		return
	}
	if env.PeerID == "" {
		p.counts.heartbeatsRejected.Add(1)
		return
	}
	if env.Nick == p.nick && env.PeerID == p.peerID {
		return // our own heartbeat echoed back; not a remote peer
	}
	ip := net.ParseIP(env.VirtualIP)
	if ip == nil || ip.To4() == nil {
		p.counts.heartbeatsRejected.Add(1)
		return
	}
	ip = ip.To4()
	now := p.now()
	ts := time.Unix(env.TS, 0)
	d := ts.Sub(now)
	if d < 0 {
		d = -d
	}
	if d > presenceFreshWindow {
		p.counts.heartbeatsRejected.Add(1)
		return
	}

	p.mu.Lock()
	// The cap: at capacity the stalest entry is evicted — even a live one —
	// because a table that refuses all comers loses every new peer.
	if len(p.table) >= NickTableCap {
		staleNick, staleSeen := "", time.Time{}
		for nick, entry := range p.table {
			if staleNick == "" || entry.LastSeen.Before(staleSeen) {
				staleNick, staleSeen = nick, entry.LastSeen
			}
		}
		if staleNick == "" {
			// Defensive: a full-but-unscannable table rejects rather
			// than corrupts; unreachable while the map is well-formed.
			p.mu.Unlock()
			p.counts.heartbeatsRejected.Add(1)
			return
		}
		delete(p.table, staleNick)
		p.counts.evictions.Add(1)
	}
	p.table[env.Nick] = PeerInfo{
		Nick:     env.Nick,
		PeerID:   env.PeerID,
		IP:       append(net.IP(nil), ip...),
		LastSeen: now,
	}
	hook := p.onPeer
	p.mu.Unlock()
	p.counts.heartbeatsSeen.Add(1)
	// Hand the discovered address to the wiring callback outside the lock:
	// LanNode uses it to register the peer in the routing table.
	if hook != nil {
		hook(env.PeerID, ip)
	}
}

// OnPeer installs a callback fired for every accepted remote heartbeat
// with that peer's ID and self-reported virtual IP. Set it before Start.
// Passing nil clears it. It is stored under the table lock and invoked
// outside it, so the callback must not re-enter Presence.
func (p *Presence) OnPeer(fn func(peerID string, ip net.IP)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onPeer = fn
}

// ResolveNick returns the virtual intranet IP of a remote participant by
// nickname. Unknown and TTL-expired entries are errors, never a zero IP.
func (p *Presence) ResolveNick(nick string) (net.IP, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.table[nick]
	if !ok {
		return nil, fmt.Errorf("nick %q is unknown", nick)
	}
	if age := p.now().Sub(entry.LastSeen); age > PresenceTTL {
		return nil, fmt.Errorf("nick %q expired %s ago", nick, age.Round(time.Second))
	}
	return append(net.IP(nil), entry.IP...), nil
}

// Peers returns a snapshot of the live (TTL-fresh) remote entries, sorted
// by nick. The local participant is never in it.
func (p *Presence) Peers() []PeerInfo {
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]PeerInfo, 0, len(p.table))
	for _, entry := range p.table {
		if now.Sub(entry.LastSeen) <= PresenceTTL {
			out = append(out, entry)
		}
	}
	slices.SortFunc(out, func(a, b PeerInfo) int { return cmp.Compare(a.Nick, b.Nick) })
	return out
}

// SweepNow evicts TTL-expired entries and returns how many. The steady
// state does not need it — ResolveNick and Peers already treat expired
// entries as gone — but Stop calls it so a Presence resumed under
// inspection (Peers via tests) never reports stale rows it just decided to
// forget. Tests drive it with a fake clock.
func (p *Presence) SweepNow() int {
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	evicted := 0
	for nick, entry := range p.table {
		if now.Sub(entry.LastSeen) > PresenceTTL {
			delete(p.table, nick)
			evicted++
		}
	}
	if evicted > 0 {
		p.counts.evictions.Add(uint64(evicted))
	}
	return evicted
}

// Stats returns a snapshot of the monotonic counters.
func (p *Presence) Stats() PresenceStats {
	return PresenceStats{
		HeartbeatsSent:     p.counts.heartbeatsSent.Load(),
		HeartbeatFails:     p.counts.heartbeatFails.Load(),
		HeartbeatsSeen:     p.counts.heartbeatsSeen.Load(),
		HeartbeatsRejected: p.counts.heartbeatsRejected.Load(),
		Evictions:          p.counts.evictions.Load(),
	}
}

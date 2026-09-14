package meshbridge

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redstone-md/moss/internal/mesh"
)

// Pump wires one moss node to one Link leg. Two directions:
//
// moss → Link: the pump is spliced into the node's packet-callback
// chain as the head — MBRIDGE-tagged directed payloads are consumed
// by the bridge, everything else falls through to prev, the caller's
// previously registered callback. This is the AttachTun splice
// pattern (node_tun_bind.go:139-156) with one adaptation: meshbridge
// lives outside package mesh, whose public API has no callback
// getter, so the CALLER supplies prev at attach instead of the pump
// reading it.
//
// Link → moss: frames arriving on the bridged topic are decoded,
// reassembled, and either Published on their channel (channelHash
// class) or sent to a single peer (FlagDirect class).
//
// Bridge-layer flag bits FlagDirect (1<<3) and FlagKeepalive (1<<4)
// are defined in codec.go with the codec's own wire constants: the
// flags byte is opaque to the codec beyond bit0, but the definitions
// live with the wire format they are carried in. The pump routes on
// them; the pump never redefines them.
//
// Anti-loop: this gateway's own frames coming back on the Link are
// dropped by gatewayID comparison. A full loop needs a second bridge
// to re-emit what it heard; without that, the ID check is complete.
type Pump struct {
	node *mesh.Node
	link Link

	// gatewayID is this bridge's marker in every outbound frame's
	// srcPeerID: the node's own Ed25519 public key (raw, not hex).
	gatewayID [32]byte
	// peerIDHex is gatewayID in the hex form mesh APIs key peers by.
	peerIDHex string

	// table is the address map (may be nil: pub/sub-only bridges skip
	// keepalive liveness but still route frames).
	table *MbsTable

	// Topic is the moss channel / Link topic pair this pump bridges.
	// Zero means DefaultTopic. Frames' channelHash must match it.
	Topic string

	mu               sync.Mutex
	stop             chan struct{}
	done             chan struct{}
	keepaliveStarted atomic.Bool

	// prev is the packet callback the pump found at attach time. It is
	// restored by Detach. There is no callback getter in the public
	// mesh API, so the pump cannot verify at detach that the node still
	// carries its splice: the contract is the same one SetPacketCallback
	// documents — whoever registers last is the head of the chain. Do
	// not register another packet callback between AttachPump and
	// Detach; if the application must, it owns re-splicing.
	prev mesh.PacketCallback
	// attached remembers whether Detach still has work to do.
	attached bool

	reasm *Reassembler

	// Counters: monotonic, only grow.
	keepaliveSent      atomic.Uint64
	keepaliveEncodeErr atomic.Uint64
	channelDropped     atomic.Uint64
	framesPublished    atomic.Uint64 // frames sent down the Link
	framesIngress      atomic.Uint64 // frames accepted off the Link
	framesData         atomic.Uint64 // complete messages delivered to moss
	ownLoopbackDropped atomic.Uint64 // own frames echoed back, dropped
	directDropped      atomic.Uint64 // direct sends that failed
	ingressSpliced     atomic.Uint64 // callbacks that hit the splice
	ingressPassthrough atomic.Uint64 // callbacks forwarded to prev
}

// DefaultTopic is the moss channel this pump bridges when created
// without an explicit Topic. It doubles as the Link topic frames ride
// on, keeping the two legs' namespaces identical in the MVP.
const DefaultTopic = "mbridge/data"

// AttachPump splices a pump between node and link and returns it.
// Ordering contract: the application registers its packet callback
// FIRST (node.SetPacketCallback), then passes that same callback as
// prev here — the public mesh API has no callback getter, so the
// caller is the only one who can name what must survive the splice.
// While attached, every directed payload that is not an MBRIDGE frame
// reaches prev unchanged, in call order; MBRIDGE frames are consumed
// by the bridge. Detach restores prev verbatim. Passing nil prev is
// legal (a bridge-only node). Attach works on a stopped or started
// node — the callback chain is live either way.
//
// The pump does not start the keepalive goroutine; call
// StartKeepalive for that.
func AttachPump(node *mesh.Node, link Link, table *MbsTable, prev mesh.PacketCallback) (*Pump, error) {
	if node == nil {
		return nil, errors.New("node is required")
	}
	if link == nil {
		return nil, errors.New("link is required")
	}
	p := &Pump{
		node:     node,
		link:     link,
		table:    table,
		prev:     prev,
		Topic:    DefaultTopic,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		attached: true,
	}
	p.gatewayID = node.PublicKey()
	p.peerIDHex = peerIDHex(p.gatewayID)
	close(p.done) // no goroutines until StartKeepalive reopens it

	// Link side: one subscription carries every data frame in. Topic
	// is pinned to DefaultTopic here — a caller wanting a different
	// topic must set it before the first frame, and Detach's teardown
	// (link.Close) is what re-subscribes nothing.
	if err := link.Subscribe(p.dataTopic(), p.ingressFromLink); err != nil {
		return nil, err
	}

	// Moss side, directed payloads: the pump's splice is the head of
	// the chain while attached — the documented SetPacketCallback
	// last-writer contract, same as AttachTun's registration order.
	node.SetPacketCallback(p.packetSplice)

	return p, nil
}

// Detach reverses AttachPump: the keepalive goroutine is stopped and
// reaped, the Link is closed (its Close is the subscription teardown
// for every leg; FakeLink drops handlers), and the node's packet
// callback is restored to prev exactly as supplied at attach. Detach
// is idempotent. Registering a different packet callback between
// AttachPump and Detach forfeits the restore — the last writer is
// the head by SetPacketCallback's contract, and Detach still puts
// prev back.
func (p *Pump) Detach() {
	p.mu.Lock()
	if !p.attached {
		p.mu.Unlock()
		return
	}
	p.attached = false
	select {
	case <-p.stop:
		// StartKeepalive was never called: done is already closed by
		// AttachPump, nothing to reap.
	default:
		close(p.stop)
	}
	p.mu.Unlock()
	<-p.done

	p.mu.Lock()
	prev := p.prev
	p.mu.Unlock()
	p.node.SetPacketCallback(prev)
	_ = p.link.Close()
}

// StartKeepalive launches the GW_KEEPALIVE loop with the default
// interval. It returns false (without starting) on an already-attached
// pump that runs one, or on a detached pump.
func (p *Pump) StartKeepalive() bool {
	return p.StartKeepaliveEvery(KeepaliveInterval)
}

// StartKeepaliveEvery is StartKeepalive with a custom interval (tests
// use sub-second values). The loop exits on Detach or Link close.
func (p *Pump) StartKeepaliveEvery(interval time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.attached {
		return false
	}
	if !p.keepaliveStarted.CompareAndSwap(false, true) {
		return false
	}
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		keepalive(p, p.link, interval, p.stop)
	}()
	return true
}

// packetSplice is the head of the node's directed-payload chain while
// the pump is attached: MBRIDGE frames go to the Link, everything else
// reaches the preserved application callback unchanged.
func (p *Pump) packetSplice(sender [32]byte, data []byte) {
	p.ingressSpliced.Add(1)
	if !IsMBridge(data) {
		p.ingressPassthrough.Add(1)
		p.mu.Lock()
		prev := p.prev
		p.mu.Unlock()
		if prev != nil {
			prev(sender, data)
		}
		return
	}
	// A directed MBRIDGE frame means the mesh side speaks bridge: take
	// the frame to the Link so the far side receives it. Own-marker
	// anti-loop on this path is the Link side's job (the sender here is
	// a mesh peer, possibly this node itself).
	p.publishFrame(data)
}

// PublishBridge encodes one application payload (a whole message, e.g.
// what a moss Publish delivered to the bridge's channel) as MBRIDGE
// frames from this gateway and publishes them down the Link. It is the
// send-side twin of the Link→moss path: the pump's own Topic is the
// channelHash, and the peer count never rides along (data, not
// keepalive).
func (p *Pump) PublishBridge(payload []byte) error {
	if len(payload) > 255*MBRIPayloadMax {
		return errors.New("payload exceeds the bridge frame budget")
	}
	msgID := NewMsgID(p.gatewayID, payload)
	frames, err := Encode(p.gatewayID, msgID, ChannelHash(p.Topic), 0, payload)
	if err != nil {
		return err
	}
	for _, frame := range frames {
		if err := p.link.Publish(p.dataTopic(), frame); err != nil {
			return err
		}
		p.framesPublished.Add(1)
	}
	return nil
}

// ingressFromLink handles one frame off the Link leg. Frames are
// classified in this order: own-marker anti-loop, keepalive (table
// liveness), then decode/reassemble and route. Non-MBRIDGE frames on
// the topic are ignored: the topic may carry foreign traffic.
func (p *Pump) ingressFromLink(topic string, frame []byte) {
	p.framesIngress.Add(1)
	if !IsMBridge(frame) {
		return
	}
	f, err := Decode(frame)
	if err != nil {
		return
	}
	if f.SrcPeerID == p.gatewayID {
		// Our own frame echoed back (a broker echoing our uplink, or a
		// second bridge forwarding what it heard): drop before loop.
		p.ownLoopbackDropped.Add(1)
		return
	}
	if f.Flags&FlagKeepalive != 0 {
		p.handleKeepalive(f)
		return
	}
	complete, ok := p.reassembler().Feed(f, time.Now())
	if !ok {
		return
	}
	p.framesData.Add(1)
	if complete.Flags&FlagDirect != 0 {
		p.sendDirect(complete)
		return
	}
	p.publishOnChannel(complete)
}

// handleKeepalive refreshes the sender's liveness in the table. The
// payload (a uvarint peer count) is advisory MVP data; liveness is the
// only effect. A gateway with no table (nil) tracks nothing.
func (p *Pump) handleKeepalive(f Frame) {
	if p.table == nil {
		return
	}
	p.table.Touch(keepaliveNodeNum(f))
}

// sendDirect delivers a FlagDirect frame's payload to one moss peer.
// Layout: [32B raw dst peer key][data]. The peer key is the raw form of
// the hex peerID SendToPeer keys by.
func (p *Pump) sendDirect(f Frame) {
	if len(f.Payload) < 32 {
		p.directDropped.Add(1)
		return
	}
	var dst [32]byte
	copy(dst[:], f.Payload[:32])
	if err := p.node.SendToPeer(peerIDHex(dst), f.Payload[32:], 5*time.Second); err != nil {
		p.directDropped.Add(1)
	}
}

// publishOnChannel re-publishes a channel-class frame's payload into
// the moss mesh on the channel its channelHash maps to. Only the
// pump's own Topic is bridged in the MVP; anything else dies here
// rather than flooding a wrong channel.
func (p *Pump) publishOnChannel(f Frame) {
	if f.ChannelHash != ChannelHash(p.Topic) {
		p.channelDropped.Add(1)
		return
	}
	p.node.Publish(p.dataTopic(), f.Payload)
}

// publishFrame sends one whole directed MBRIDGE frame (already in wire
// format, built by the mesh-side sender) down the Link.
func (p *Pump) publishFrame(frame []byte) {
	if err := p.link.Publish(p.dataTopic(), frame); err != nil {
		return
	}
	p.framesPublished.Add(1)
}

// dataTopic is the moss channel and Link topic this pump bridges.
func (p *Pump) dataTopic() string {
	if p.Topic == "" {
		return DefaultTopic
	}
	return p.Topic
}

// reassembler lazily builds the reassembly table so an attached pump
// carries no per-message state until fragmentation shows up.
func (p *Pump) reassembler() *Reassembler {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reasm == nil {
		ttl := KeepaliveInterval
		p.reasm = NewReassembler(ttl, 64)
	}
	return p.reasm
}

// nodePeers reports the gateway's moss peer count for keepalives.
// The public mesh API does not export a peer count (MeshInfoJSON is
// JSON-shaped, k-anon telemetry is aggregate), so the MVP keepalive
// carries 0 as its advisory peer count — the uvarint payload keeps
// the wire shape the serial/MQTT legs will fill with a real count.
func (p *Pump) nodePeers() int { return 0 }

func (p *Pump) countKeepaliveSent()      { p.keepaliveSent.Add(1) }
func (p *Pump) countKeepaliveEncodeErr() { p.keepaliveEncodeErr.Add(1) }

// PeerID returns this gateway's moss peer ID (hex), the value other
// nodes' APIs key this bridge node by.
func (p *Pump) PeerID() string { return p.peerIDHex }

// ChannelHash derives the 16-bit channel hint the MBRIDGE header
// carries: FNV-1a of the topic name, truncated. Both legs of a bridge
// pair compute it from the shared topic string, so no hash table needs
// syncing in the MVP.
func ChannelHash(topic string) uint16 {
	var h uint32 = 2166136261
	for i := 0; i < len(topic); i++ {
		h ^= uint32(topic[i])
		h *= 16777619
	}
	return uint16(h >> 16)
}

// peerIDHex is the raw→hex form of a peer key, matching how
// mesh formats peer IDs (lowercase hex).
func peerIDHex(key [32]byte) string {
	return hex.EncodeToString(key[:])
}

// keepaliveNodeNum folds a keepalive frame's message ID into the
// uint32 the table keys liveness by. The ID already identifies the
// sender uniquely (NewMsgID of gateway + salt), so its first 4 bytes
// are a stable per-gateway key.
func keepaliveNodeNum(f Frame) uint32 {
	return binary.BigEndian.Uint32(f.MsgID[:4])
}

// Counters, all monotonic (they only grow):

func (p *Pump) KeepalivesSent() uint64     { return p.keepaliveSent.Load() }
func (p *Pump) FramesPublished() uint64    { return p.framesPublished.Load() }
func (p *Pump) FramesIngress() uint64      { return p.framesIngress.Load() }
func (p *Pump) FramesData() uint64         { return p.framesData.Load() }
func (p *Pump) OwnLoopbackDropped() uint64 { return p.ownLoopbackDropped.Load() }
func (p *Pump) DirectDropped() uint64      { return p.directDropped.Load() }
func (p *Pump) ChannelDropped() uint64     { return p.channelDropped.Load() }
func (p *Pump) IngressSpliced() uint64     { return p.ingressSpliced.Load() }
func (p *Pump) IngressPassthrough() uint64 { return p.ingressPassthrough.Load() }

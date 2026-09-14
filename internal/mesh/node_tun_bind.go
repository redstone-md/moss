package mesh

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/redstone-md/moss/internal/tun"
)

// The intranet binding: a packet interface (real TUN device, loopback, or
// test double) attached to one node, routed over the node's directed-packet
// transport (SendToPeer / packet callback). The mesh layer owns none of the
// routing — internal/tun does — this file is the glue: goroutine lifetime,
// packet-callback chaining, and the virtual-IP registry per node.
//
// v1, fixated: NO FRAGMENTATION. Packets over the MTU are dropped and
// counted on both directions (see tun.Router). Frame cap of the underlying
// streams is untouched.

// tunSendTimeout bounds the relay-path fallback inside SendToPeer when an
// outbound packet targets a peer with no live session. Direct-session sends
// are enqueue-or-synchronous and ignore the timeout.
const tunSendTimeout = 5 * time.Second

// tunDetachWait bounds how long DetachTun waits for the binding's goroutines
// to confirm exit. A well-behaved PacketIface unblocks its reader on Close,
// so the wait is instant in practice; the bound keeps a wedged interface
// from wedging the caller.
const tunDetachWait = 5 * time.Second

// tunBinds is the per-node intranet registry. Node cannot grow fields
// (node_types.go is frozen for this change), so the binding lives beside
// it. tunBindsMu serializes the attach/detach control plane so the packet
// callback chain is only ever spliced by one binding at a time.
var (
	tunBinds   sync.Map // map[*Node]*tunBinding
	tunBindsMu sync.Mutex
)

// tunBinding is one node's attached intranet. Immutable after construction
// except the router's atomic counters.
type tunBinding struct {
	node   *Node
	iface  tun.PacketIface
	table  *tun.Table
	router *tun.Router
	// prev is the packet callback registered before attachment; DetachTun
	// restores it verbatim.
	prev PacketCallback
	// stop is closed by DetachTun to terminate the binding's goroutines.
	stop chan struct{}
	// done is closed once BOTH the pump and the route loop have exited.
	done chan struct{}
}

// AttachTun attaches a packet interface to node as the node's intranet
// edge: packets read from iface are routed to the mesh peers that own
// their destination virtual IP (inside cidr, e.g. "10.66.0.0/24"), and
// directed payloads received from the mesh are written to iface.
//
// Callback chaining: the node's current packet callback keeps receiving
// NON-IP payloads untouched; IP packets are demultiplexed into the
// intranet (tun.IsIPv4 classifies). DetachTun restores the callback that
// was registered at attach time — the application must not call
// SetPacketCallback between AttachTun and DetachTun, or that registration
// is lost on detach.
//
// Lifecycle: attach AFTER node.Start() for full wiring — the binding
// captures the node's live root context, so node.Stop() alone terminates
// the binding (and closes iface). Attaching a never-started node is
// allowed (unit tests drive it directly); the binding then lives until
// DetachTun. Attaching a node that was started and already stopped is
// rejected. DetachTun closes the interface: PacketIface.Close must be
// idempotent, and a detached interface is not reusable.
//
// A second attach while one is attached fails without touching the
// existing binding.
func AttachTun(n *Node, iface tun.PacketIface, cidr string) error {
	if n == nil {
		return errors.New("node is required")
	}
	if iface == nil {
		return errors.New("packet interface is required")
	}
	table, err := tun.NewTable(cidr)
	if err != nil {
		return err
	}

	tunBindsMu.Lock()
	defer tunBindsMu.Unlock()

	if _, attached := tunBinds.Load(n); attached {
		return errors.New("a tun interface is already attached to this node")
	}
	n.mu.RLock()
	stopped := n.rootCtx != nil && n.rootCtx.Err() != nil
	n.mu.RUnlock()
	if stopped {
		return errors.New("node is stopped; attach before Stop or after a fresh Start")
	}

	b := &tunBinding{
		node:  n,
		iface: iface,
		table: table,
		router: tun.NewRouter(iface, table, func(peerID string, payload []byte) error {
			return n.SendToPeer(peerID, payload, tunSendTimeout)
		}, tun.DefaultMTU),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	// The control-plane lock makes the splice and the registry insert one
	// atomic step: there is no window where a second attach could observe
	// no binding yet overwrite the winner's callback with its own.
	n.mu.Lock()
	b.prev = n.packetCB
	n.packetCB = func(senderID [32]byte, data []byte) {
		if tun.IsIPv4(data) {
			b.router.RouteInbound(data)
			return
		}
		if b.prev != nil {
			b.prev(senderID, data)
		}
	}
	n.mu.Unlock()
	tunBinds.Store(n, b)

	go b.run()
	return nil
}

// DetachTun stops the node's intranet, closes its interface, restores the
// packet callback registered at attach time, and releases the virtual-IP
// table. Safe after Stop: a binding already terminated by node.Stop() is
// cleanly reaped here. Returns an error when nothing is attached.
func DetachTun(n *Node) error {
	if n == nil {
		return errors.New("node is required")
	}
	tunBindsMu.Lock()
	defer tunBindsMu.Unlock()
	v, ok := tunBinds.LoadAndDelete(n)
	if !ok {
		return errors.New("no tun interface is attached to this node")
	}
	b := v.(*tunBinding)

	// Terminate and wait. close(stop) wakes the route loop and the pump's
	// in-flight send; run's exit path closes the interface, which unblocks
	// a pump parked inside ReadPacket (the PacketIface contract).
	close(b.stop)
	select {
	case <-b.done:
	case <-time.After(tunDetachWait):
		// The binding is already out of the registry; a wedged interface
		// leaks its goroutine (the only safe move — closing from two
		// places would race), but the node stays usable for a re-attach.
		return errors.New("tun binding did not stop in time")
	}

	// Restore the pre-attach callback under the node lock so a concurrent
	// dispatch sees either the chained or the restored one, never a torn
	// one. If the application registered a NEW callback after AttachTun
	// via SetPacketCallback, that registration is overwritten — the
	// documented constraint forbids the mix.
	n.mu.Lock()
	n.packetCB = b.prev
	n.mu.Unlock()
	return nil
}

// PeerAddr returns the virtual intranet address of peerID as seen from
// node's attached intranet, assigning one on first use (lowest free
// address in the pool, skipping the network and broadcast addresses).
func PeerAddr(n *Node, peerID string) (net.IP, error) {
	if n == nil {
		return nil, errors.New("node is required")
	}
	v, ok := tunBinds.Load(n)
	if !ok {
		return nil, errors.New("no tun interface is attached to this node")
	}
	addr, err := v.(*tunBinding).table.PeerAddr(peerID)
	if err != nil {
		return nil, err
	}
	a4 := addr.As4()
	return net.IP(append([]byte(nil), a4[:]...)), nil
}

// run is the binding's goroutine: a pump that reads packets off the
// interface and a loop that routes them, either stopping on DetachTun's
// stop channel or on the node's captured root context (node.Stop()).
// When both have exited, done closes; the interface is closed on any exit
// path — that is also what unblocks a pump parked in ReadPacket.
func (b *tunBinding) run() {
	// The node's root context at attach time: nil for a never-started
	// node (the loop then lives until stop), closed for a stopped one
	// (the loop exits immediately — AttachTun rejected that state, this
	// belt-and-braces guard covers a Stop racing the attach).
	b.node.mu.RLock()
	ctxDone := b.node.rootCtx
	b.node.mu.RUnlock()
	var rootDone <-chan struct{}
	if ctxDone != nil {
		rootDone = ctxDone.Done()
	}

	packets := make(chan []byte, 64)
	pumpDone := make(chan struct{})
	go func() {
		// Pump: interface → packets channel. ReadPacket blocking is the
		// interface's normal state; Close (run's exit path) is what ends
		// it, per the PacketIface contract.
		defer close(pumpDone)
		defer close(packets)
		for {
			packet, err := b.iface.ReadPacket()
			if err != nil {
				return
			}
			select {
			case packets <- packet:
			case <-b.stop:
				return
			}
		}
	}()

	defer func() {
		// Close first: it is what unblocks a pump parked in ReadPacket
		// when the exit was triggered by stop/rootDone rather than by the
		// interface's own closure. Idempotent by contract.
		_ = b.iface.Close()
		<-pumpDone
		close(b.done)
	}()

	for {
		select {
		case <-b.stop:
			return
		case <-rootDone:
			return
		case packet, ok := <-packets:
			if !ok {
				// The interface closed under us: the pump is exiting.
				return
			}
			b.router.RouteOutbound(packet)
		}
	}
}

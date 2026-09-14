package meshbridge

import (
	"encoding/binary"
	"time"
)

// Keepalive defaults. One GW_KEEPALIVE MBRIDGE packet every 60s tells the
// far side the gateway is alive; the table sweep timeout is 3x that, so a
// gateway that misses one keepalive (a busy LoRa downlink, a broker
// hiccup) is not declared dead — only a sustained silence is.
const (
	// KeepaliveInterval is how often the bridge announces itself.
	KeepaliveInterval = 60 * time.Second
	// KeepaliveTimeout is the table liveness budget: 3x the interval.
	KeepaliveTimeout = 3 * KeepaliveInterval
	// keepaliveTopic is the Link topic GW_KEEPALIVE packets ride on.
	keepaliveTopic = "mbridge/keepalive"
)

// keepalive publishes a GW_KEEPALIVE MBRIDGE packet on the Link every
// interval until stop is closed, and sweeps the address table with the
// same rhythm. One goroutine, one ticker, one shutdown path: it exits
// only on stop or when the Link reports itself closed.
//
// The packet is a single MBRIDGE frame: FlagKeepalive set, channelHash
// zero (keepalives are not topic traffic), payload a uvarint of this
// gateway's current moss peer count. The peer count is the only payload
// member in the MVP — the geo-tag idea from the design review was
// dropped as out of scope.
func keepalive(p *Pump, link Link, interval time.Duration, stop <-chan struct{}) {
	if p == nil || link == nil || interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		payload := binary.AppendUvarint(nil, uint64(p.nodePeers()))
		frames, err := Encode(p.gatewayID, newKeepaliveMsgID(), 0, FlagKeepalive, payload)
		if err != nil {
			// Encode of a legal keepalive cannot fail (single small
			// frame); count and move on rather than killing the loop.
			p.countKeepaliveEncodeErr()
			continue
		}
		for _, frame := range frames {
			if err := link.Publish(keepaliveTopic, frame); err != nil {
				// A closed Link ends the loop: every further publish
				// would fail the same way, so stop burning the ticker.
				return
			}
		}
		p.countKeepaliveSent()
		// The sweep rides the same tick so no second timer (and no
		// second goroutine) is needed; it is the 3x-timeout expiry the
		// table contract defines.
		if p.table != nil {
			p.table.Sweep(time.Now(), KeepaliveTimeout)
		}
	}
}

// newKeepaliveMsgID derives the 8-byte message ID for a keepalive frame.
// Keepalives carry no application payload to key an ID on, so a fixed
// salt plus the gateway identity pins it: every keepalive from this
// gateway is recognizable as "from this gateway, keepalive class"
// without admitting a wall-clock value into the ID (two gateways
// starting at the same second must not collide).
func newKeepaliveMsgID() [8]byte {
	return NewMsgID(keepaliveIDSalt, nil)
}

// keepaliveIDSalt is the srcPeerID-domain separator for keepalive
// message IDs: it is never a real peer key (all-zero is not a valid
// Ed25519 public key), so its IDs cannot collide with a data message's.
var keepaliveIDSalt = [32]byte{}

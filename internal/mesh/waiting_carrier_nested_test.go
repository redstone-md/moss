package mesh

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

func TestCarrierReplacementRejectsNestedSameIdentityOwner(t *testing.T) {
	f := startHeldCarrierCommit(t)
	current := f.phone.peerByID(f.core.localPeerID())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", nodeAddr(f.phone))
	if err != nil {
		t.Fatal(err)
	}
	session, err := transport.ClientHandshake(ctx, conn, transport.HandshakeConfig{MeshID: f.core.networkID, PSK: f.core.transportHandshakePSK(), Identity: f.core.identity, RemoteStatic: f.phone.identity.NoiseStaticPublic()})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	defer session.Close()
	closed := make(chan struct{})
	go func() {
		for {
			if _, err := session.ReadPacket(); err != nil {
				close(closed)
				return
			}
		}
	}()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("nested same-identity replacement was not refused")
	}
	if f.phone.peerByID(f.core.localPeerID()) != current || current.replacement.Load().old.readClosed.Load() {
		t.Fatal("nested attempt displaced an existing replacement owner")
	}
	assertWaitingAlternativePackets(t, f.origin, f.phone)
}

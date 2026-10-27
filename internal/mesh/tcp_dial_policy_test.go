package mesh

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func TestKnownTCPDialOverlapKeepsCallerCancellationIndependent(t *testing.T) {
	for _, reason := range []string{"waiter", "owner", "stop"} {
		t.Run(reason, func(t *testing.T) {
			origin, core, addr, accepted, release := heldTCPDialFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ownerCtx, cancelOwner := context.WithCancel(ctx)
			defer cancelOwner()
			waiterCtx, cancelWaiter := context.WithCancel(ctx)
			defer cancelWaiter()
			owner, waiter := make(chan error, 1), make(chan error, 1)
			go func() { owner <- origin.connectPeerTCPWithHint(ownerCtx, addr, core.localPeerID()) }()
			waitFor(t, func() bool { return accepted.Load() == 1 }, "owner did not reach listener")
			go func() { waiter <- origin.connectPeerTCPWithHint(waiterCtx, addr, core.localPeerID()) }()
			// This gate holds a real handshake while both original contexts remain live.
			time.Sleep(20 * time.Millisecond)
			switch reason {
			case "waiter":
				cancelWaiter()
				assertDialFailure(t, waiter, ctx)
				select {
				case err := <-owner:
					t.Fatalf("waiter cancelled owner: %v", err)
				default:
				}
				release()
				assertDialSuccess(t, owner, ctx)
			case "owner":
				cancelOwner()
				assertDialFailure(t, owner, ctx)
				waitFor(t, func() bool { return accepted.Load() == 2 }, "live waiter inherited expired owner's failure")
				release()
				assertDialSuccess(t, waiter, ctx)
			case "stop":
				origin.Stop()
				assertDialFailure(t, owner, ctx)
				assertDialFailure(t, waiter, ctx)
				origin.Start()
				restarted := make(chan error, 1)
				go func() { restarted <- origin.connectPeerTCPWithHint(ctx, addr, core.localPeerID()) }()
				waitFor(t, func() bool { return accepted.Load() == 2 }, "restart retained previous flight owner")
				release()
				assertDialSuccess(t, restarted, ctx)
			}
		})
	}
}

func TestKnownTCPDialOverlapDoesNotBlockOtherIdentity(t *testing.T) {
	origin, core, addr, accepted, release := heldTCPDialFixture(t)
	other := startCappedOverlayNode(t, "independent-dial")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- origin.connectPeerTCPWithHint(ctx, addr, core.localPeerID()) }()
	waitFor(t, func() bool { return accepted.Load() == 1 }, "held dial did not begin")
	if err := origin.connectPeerTCPWithHint(ctx, nodeAddr(other), other.localPeerID()); err != nil {
		t.Fatal(err)
	}
	if origin.peerByID(other.localPeerID()) == nil {
		t.Fatal("independent identity was not admitted")
	}
	release()
	assertDialSuccess(t, result, ctx)
}

func assertDialFailure(t *testing.T, result <-chan error, ctx context.Context) {
	t.Helper()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled TCP dial reported success")
		}
	case <-ctx.Done():
		t.Fatal("TCP waiter exceeded original context")
	}
}
func assertDialSuccess(t *testing.T, result <-chan error, ctx context.Context) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("live TCP waiter exceeded original context")
	}
}

func TestKnownTCPDialOverlapKeepsRefreshedAddressOpportunity(t *testing.T) {
	origin, core := startCappedOverlayNode(t, "fresh-dial-origin"), startCappedOverlayNode(t, "fresh-dial-core")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		close(accepted)
		<-release
		conn.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- origin.connectPeerTCPWithHint(ctx, listener.Addr().String(), core.localPeerID()) }()
	select {
	case <-accepted:
	case <-ctx.Done():
		t.Fatal("stale address did not accept held connection")
	}
	go func() { second <- origin.connectPeerTCPWithHint(ctx, nodeAddr(core), core.localPeerID()) }()
	time.Sleep(20 * time.Millisecond)
	unblock()
	assertDialFailure(t, first, ctx)
	assertDialSuccess(t, second, ctx)
	waitFor(t, func() bool { return core.peerByID(origin.localPeerID()) != nil }, "fresh address counterpart did not admit")
	assertWaitingAlternativePackets(t, origin, core)
}

func TestKnownTCPDialWaiterRequiresActualAdmission(t *testing.T) {
	origin, core, addr, accepted, release := heldTCPDialFixture(t)
	other := startCappedOverlayNode(t, "allowed-other")
	origin.AllowPeer(other.localPeerID())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- origin.connectPeerTCPWithHint(ctx, addr, core.localPeerID()) }()
	waitFor(t, func() bool { return accepted.Load() == 1 }, "first refused dial did not reach listener")
	go func() { second <- origin.connectPeerTCPWithHint(ctx, addr, core.localPeerID()) }()
	time.Sleep(20 * time.Millisecond)
	release()
	// Existing single-caller registration-refusal return semantics stay intact.
	select {
	case <-first:
	case <-ctx.Done():
		t.Fatal("owner exceeded original context")
	}
	assertDialFailure(t, second, ctx)
	if origin.peerByID(core.localPeerID()) != nil {
		t.Fatal("revoked carrier entered the directory")
	}
}

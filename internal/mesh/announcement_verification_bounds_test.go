package mesh

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

func TestAnnouncementVerificationEvictsOldFactsAtCapacity(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	key := identity.PublicKey()
	var memo announcementVerifier
	calls := 0
	check := func(key, payload, signature []byte) bool {
		calls++
		return mcrypto.Verify(key, payload, signature)
	}
	for index := range announcementVerificationLimit + 1 {
		payload := []byte(fmt.Sprintf("announcement-%d", index))
		if !memo.verify(key[:], payload, identity.Sign(payload), check) {
			t.Fatal("valid announcement rejected")
		}
	}
	latest := []byte(fmt.Sprintf("announcement-%d", announcementVerificationLimit))
	if !memo.verify(key[:], latest, identity.Sign(latest), check) || calls != announcementVerificationLimit+1 {
		t.Fatal("most recent successful verification was not retained")
	}
	first := []byte("announcement-0")
	if !memo.verify(key[:], first, identity.Sign(first), check) || calls != announcementVerificationLimit+2 {
		t.Fatal("evicted announcement did not run real verification again")
	}
	if len(memo.entries) != announcementVerificationLimit || len(memo.order) != announcementVerificationLimit {
		t.Fatal("verification memo exceeded its storage bound")
	}
}

func TestAnnouncementVerificationConcurrentMissesDoNotBlockCrypto(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	key := identity.PublicKey()
	payload := []byte("concurrent announcement")
	signature := identity.Sign(payload)
	var memo announcementVerifier
	var calls atomic.Int64
	ready, release, results := make(chan struct{}, 2), make(chan struct{}), make(chan bool, 2)
	check := func(key, payload, signature []byte) bool {
		calls.Add(1)
		ready <- struct{}{}
		<-release
		return mcrypto.Verify(key, payload, signature)
	}
	for range 2 {
		go func() { results <- memo.verify(key[:], payload, signature, check) }()
	}
	for range 2 {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("one peer's crypto check blocked another peer")
		}
	}
	close(release)
	if !<-results || !<-results || !memo.verify(key[:], payload, signature, check) || calls.Load() != 2 {
		t.Fatal("concurrent successful fact was not reused afterward")
	}
}

package nat

import (
	"errors"
	"net"
	"testing"
	"time"
)

type fakeRouter struct {
	addErr        error
	externalIP    net.IP
	externalPort  uint16
	addCalls      int
	deleteCalls   int
	protocols     []string
	lastDeleteExt int
	lastDeleteInt int
}

func (f *fakeRouter) AddMapping(protocol string, extport, intport int, name string, lifetime time.Duration) (uint16, error) {
	f.addCalls++
	f.protocols = append(f.protocols, protocol)
	if f.addErr != nil {
		return 0, f.addErr
	}
	if f.externalPort != 0 {
		return f.externalPort, nil
	}
	return uint16(extport), nil
}

func (f *fakeRouter) DeleteMapping(protocol string, extport, intport int) error {
	f.deleteCalls++
	f.lastDeleteExt = extport
	f.lastDeleteInt = intport
	return nil
}

func (f *fakeRouter) ExternalIP() (net.IP, error) {
	if f.externalIP == nil {
		return nil, errors.New("no ip")
	}
	return f.externalIP, nil
}

func TestManagedMapperFallsBackToNextBackend(t *testing.T) {
	first := &fakeRouter{addErr: errors.New("no upnp")}
	second := &fakeRouter{externalIP: net.ParseIP("198.51.100.20"), externalPort: 41000}
	mapper := newManagedMapper(MappingOptions{
		Description: "test",
		Lifetime:    time.Minute,
	}, []namedRouter{
		{name: "upnp", nat: first},
		{name: "natpmp", nat: second},
	})
	addr, ok := mapper.Map(40000)
	if !ok {
		t.Fatal("expected mapper to succeed on fallback backend")
	}
	if addr != "198.51.100.20:41000" {
		t.Fatalf("unexpected mapped address %q", addr)
	}
	if first.addCalls != 2 {
		t.Fatalf("expected first backend to be tried for udp and tcp, got %d", first.addCalls)
	}
	if second.addCalls != 2 {
		t.Fatalf("expected second backend to map udp and tcp, got %d calls", second.addCalls)
	}
	mapper.Close()
	if second.deleteCalls != 2 || second.lastDeleteExt != 41000 || second.lastDeleteInt != 40000 {
		t.Fatalf("expected both mapping leases to be released, got deleteCalls=%d ext=%d int=%d", second.deleteCalls, second.lastDeleteExt, second.lastDeleteInt)
	}
}

func TestManagedMapperCachesActiveMapping(t *testing.T) {
	router := &fakeRouter{externalIP: net.ParseIP("203.0.113.8")}
	mapper := newManagedMapper(MappingOptions{}, []namedRouter{{name: "upnp", nat: router}})
	firstAddr, ok := mapper.Map(45000)
	if !ok {
		t.Fatal("expected initial mapping to succeed")
	}
	secondAddr, ok := mapper.Map(45000)
	if !ok {
		t.Fatal("expected cached mapping lookup to succeed")
	}
	if firstAddr != secondAddr {
		t.Fatalf("expected cached mapping to keep address, got %q and %q", firstAddr, secondAddr)
	}
	if router.addCalls != 2 {
		t.Fatalf("expected udp and tcp mapping calls, got %d", router.addCalls)
	}
	mapper.Close()
}

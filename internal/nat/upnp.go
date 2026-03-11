package nat

import (
	"net"
	"strconv"
	"sync"
	"time"
)

type routerInterface interface {
	AddMapping(protocol string, extport, intport int, name string, lifetime time.Duration) (uint16, error)
	DeleteMapping(protocol string, extport, intport int) error
	ExternalIP() (net.IP, error)
}

type MappingOptions struct {
	EnableUPnP   bool
	EnableNATPMP bool
	EnablePCP    bool
	Description  string
	Lifetime     time.Duration
}

type PortMapper interface {
	Map(port int) (string, bool)
	Close()
}

type NoopMapper struct{}

func (NoopMapper) Map(port int) (string, bool) {
	return "", false
}

func (NoopMapper) Close() {}

type namedRouter struct {
	name string
	nat  routerInterface
}

type managedMapper struct {
	mu          sync.Mutex
	description string
	lifetime    time.Duration
	routes      []namedRouter
	mappings    map[int]mappedPort
}

type mappedLease struct {
	protocol     string
	externalPort int
}

type mappedPort struct {
	router       routerInterface
	internalPort int
	externalPort int
	leases       []mappedLease
}

func NewPortMapper(opts MappingOptions) PortMapper {
	routes := make([]namedRouter, 0, 3)
	if opts.EnableUPnP {
		routes = append(routes, namedRouter{name: "upnp", nat: newUPnPBackend()})
	}
	if opts.EnableNATPMP {
		routes = append(routes, namedRouter{name: "natpmp", nat: newNATPMPBackend()})
	}
	if opts.EnablePCP {
		routes = append(routes, namedRouter{name: "pcp", nat: newPCPBackend()})
	}
	return newManagedMapper(opts, routes)
}

func newManagedMapper(opts MappingOptions, routes []namedRouter) PortMapper {
	usable := make([]namedRouter, 0, len(routes))
	for _, route := range routes {
		if route.nat != nil {
			usable = append(usable, route)
		}
	}
	if len(usable) == 0 {
		return NoopMapper{}
	}
	description := opts.Description
	if description == "" {
		description = "moss"
	}
	lifetime := opts.Lifetime
	if lifetime <= 0 {
		lifetime = 10 * time.Minute
	}
	return &managedMapper{
		description: description,
		lifetime:    lifetime,
		routes:      usable,
		mappings:    make(map[int]mappedPort),
	}
}

func (m *managedMapper) Map(port int) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.mappings[port]; ok {
		return mappedAddress(existing.router, existing.externalPort)
	}
	for _, route := range m.routes {
		mapped := mappedPort{
			router:       route.nat,
			internalPort: port,
		}
		for _, protocol := range []string{"udp", "tcp"} {
			requestedPort := port
			if mapped.externalPort != 0 {
				requestedPort = mapped.externalPort
			}
			externalPort, err := route.nat.AddMapping(protocol, requestedPort, port, m.description, m.lifetime)
			if err != nil {
				continue
			}
			if mapped.externalPort == 0 {
				mapped.externalPort = int(externalPort)
			}
			mapped.leases = append(mapped.leases, mappedLease{
				protocol:     protocol,
				externalPort: int(externalPort),
			})
		}
		if mapped.externalPort == 0 {
			continue
		}
		address, ok := mappedAddress(route.nat, mapped.externalPort)
		if !ok {
			for _, lease := range mapped.leases {
				_ = route.nat.DeleteMapping(lease.protocol, lease.externalPort, port)
			}
			continue
		}
		m.mappings[port] = mapped
		return address, true
	}
	return "", false
}

func (m *managedMapper) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for port, mapped := range m.mappings {
		for _, lease := range mapped.leases {
			_ = mapped.router.DeleteMapping(lease.protocol, lease.externalPort, mapped.internalPort)
		}
		delete(m.mappings, port)
	}
}

func mappedAddress(router routerInterface, port int) (string, bool) {
	ip, err := router.ExternalIP()
	if err != nil || ip == nil {
		return "", false
	}
	if ip.IsUnspecified() {
		return "", false
	}
	return net.JoinHostPort(ip.String(), strconv.Itoa(port)), true
}

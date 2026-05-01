package nat

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/huin/goupnp"
	"github.com/huin/goupnp/dcps/internetgateway1"
	"github.com/huin/goupnp/dcps/internetgateway2"
)

const (
	upnpDiscoveryTimeout = 3 * time.Second
	upnpRequestTimeout   = 3 * time.Second
	upnpRateLimit        = 200 * time.Millisecond
	upnpRetryCount       = 3
	upnpRandomPortTries  = 3
)

type upnpRouter struct {
	device      *goupnp.RootDevice
	location    *url.URL
	service     upnpClient
	serviceName string
	mu          sync.Mutex
	lastRequest time.Time
}

type upnpClient interface {
	GetExternalIPAddress() (string, error)
	AddPortMapping(string, uint16, string, uint16, string, bool, string, uint32) error
	DeletePortMapping(string, uint16, string) error
	GetNATRSIPStatus() (bool, bool, error)
}

type upnpAnyPortClient interface {
	AddAnyPortMapping(string, uint16, string, uint16, string, bool, string, uint32) (uint16, error)
}

func newUPnPBackend() routerInterface {
	ctx, cancel := context.WithTimeout(context.Background(), upnpDiscoveryTimeout)
	defer cancel()

	found := make(chan *upnpRouter, 2)
	go discoverUPnP(ctx, found, internetgateway1.URN_WANConnectionDevice_1, matchUPnPIGDv1)
	go discoverUPnP(ctx, found, internetgateway2.URN_WANConnectionDevice_2, matchUPnPIGDv2)
	for i := 0; i < cap(found); i++ {
		if router := <-found; router != nil {
			return router
		}
	}
	return nil
}

func (r *upnpRouter) AddMapping(protocol string, extport, intport int, desc string, lifetime time.Duration) (uint16, error) {
	if lifetime <= 0 {
		return 0, errors.New("lifetime must not be <= 0")
	}
	if extport == 0 {
		extport = intport
	}
	internalIP, err := r.internalAddress()
	if err != nil {
		return 0, err
	}
	protocol = strings.ToUpper(protocol)
	leaseSeconds := uint32(lifetime / time.Second)
	if client, ok := r.service.(upnpAnyPortClient); ok {
		return r.addAnyPortMapping(client, protocol, extport, intport, internalIP.String(), desc, leaseSeconds)
	}
	return r.addPortMapping(protocol, extport, intport, internalIP.String(), desc, leaseSeconds)
}

func (r *upnpRouter) DeleteMapping(protocol string, extport, intport int) error {
	return r.withRateLimit(func() error {
		return r.service.DeletePortMapping("", uint16(extport), strings.ToUpper(protocol))
	})
}

func (r *upnpRouter) ExternalIP() (net.IP, error) {
	var raw string
	err := r.withRateLimit(func() error {
		var err error
		raw, err = r.service.GetExternalIPAddress()
		return err
	})
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(raw)
	if ip == nil {
		return nil, errors.New("bad upnp external ip response")
	}
	return ip, nil
}

func (r *upnpRouter) addAnyPortMapping(client upnpAnyPortClient, protocol string, extport, intport int, internalIP, desc string, leaseSeconds uint32) (uint16, error) {
	var mapped uint16
	err := r.withRateLimit(func() error {
		var err error
		mapped, err = client.AddAnyPortMapping("", uint16(extport), protocol, uint16(intport), internalIP, true, desc, leaseSeconds)
		return err
	})
	return mapped, err
}

func (r *upnpRouter) addPortMapping(protocol string, extport, intport int, internalIP, desc string, leaseSeconds uint32) (uint16, error) {
	var lastErr error
	for i := 0; i <= upnpRetryCount; i++ {
		lastErr = r.withRateLimit(func() error {
			return r.service.AddPortMapping("", uint16(extport), protocol, uint16(intport), internalIP, true, desc, leaseSeconds)
		})
		if lastErr == nil {
			return uint16(extport), nil
		}
	}
	for i := 0; i < upnpRandomPortTries; i++ {
		port, err := randomUPnPPort()
		if err != nil {
			return 0, err
		}
		lastErr = r.withRateLimit(func() error {
			return r.service.AddPortMapping("", port, protocol, uint16(intport), internalIP, true, desc, leaseSeconds)
		})
		if lastErr == nil {
			return port, nil
		}
	}
	return 0, lastErr
}

func (r *upnpRouter) natEnabled() bool {
	var enabled bool
	err := r.withRateLimit(func() error {
		_, nat, err := r.service.GetNATRSIPStatus()
		enabled = nat
		return err
	})
	return err == nil && enabled
}

func (r *upnpRouter) internalAddress() (net.IP, error) {
	host := ""
	if r.device != nil {
		host = r.device.URLBase.Host
	}
	if host == "" && r.location != nil {
		host = r.location.Host
	}
	if host == "" {
		return nil, errors.New("upnp device address is unknown")
	}
	deviceAddr, err := net.ResolveUDPAddr("udp4", host)
	if err != nil {
		return nil, err
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			network, ok := addr.(*net.IPNet)
			if ok && network.Contains(deviceAddr.IP) {
				return network.IP, nil
			}
		}
	}
	return nil, errors.New("could not find local address for upnp device")
}

func (r *upnpRouter) withRateLimit(fn func() error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if elapsed := time.Since(r.lastRequest); elapsed < upnpRateLimit {
		time.Sleep(upnpRateLimit - elapsed)
	}
	err := fn()
	r.lastRequest = time.Now()
	return err
}

func discoverUPnP(ctx context.Context, out chan<- *upnpRouter, target string, matcher func(goupnp.ServiceClient) *upnpRouter) {
	devices, err := goupnp.DiscoverDevicesCtx(ctx, target)
	if err != nil {
		out <- nil
		return
	}
	for _, device := range devices {
		if device.Root == nil || device.Err != nil {
			continue
		}
		var found *upnpRouter
		device.Root.Device.VisitServices(func(service *goupnp.Service) {
			if found != nil {
				return
			}
			client := goupnp.ServiceClient{
				SOAPClient: service.NewSOAPClient(),
				RootDevice: device.Root,
				Location:   device.Location,
				Service:    service,
			}
			client.SOAPClient.HTTPClient.Timeout = upnpRequestTimeout
			candidate := matcher(client)
			if candidate == nil {
				return
			}
			candidate.device = device.Root
			candidate.location = device.Location
			if candidate.natEnabled() {
				found = candidate
			}
		})
		if found != nil {
			out <- found
			return
		}
	}
	out <- nil
}

func matchUPnPIGDv1(client goupnp.ServiceClient) *upnpRouter {
	switch client.Service.ServiceType {
	case internetgateway1.URN_WANIPConnection_1:
		return &upnpRouter{serviceName: "igd1-ip1", service: &internetgateway1.WANIPConnection1{ServiceClient: client}}
	case internetgateway1.URN_WANPPPConnection_1:
		return &upnpRouter{serviceName: "igd1-ppp1", service: &internetgateway1.WANPPPConnection1{ServiceClient: client}}
	default:
		return nil
	}
}

func matchUPnPIGDv2(client goupnp.ServiceClient) *upnpRouter {
	switch client.Service.ServiceType {
	case internetgateway2.URN_WANIPConnection_1:
		return &upnpRouter{serviceName: "igd2-ip1", service: &internetgateway2.WANIPConnection1{ServiceClient: client}}
	case internetgateway2.URN_WANIPConnection_2:
		return &upnpRouter{serviceName: "igd2-ip2", service: &internetgateway2.WANIPConnection2{ServiceClient: client}}
	case internetgateway2.URN_WANPPPConnection_1:
		return &upnpRouter{serviceName: "igd2-ppp1", service: &internetgateway2.WANPPPConnection1{ServiceClient: client}}
	default:
		return nil
	}
}

func randomUPnPPort() (uint16, error) {
	const minPort = 10000
	span := big.NewInt(65535 - minPort + 1)
	value, err := rand.Int(rand.Reader, span)
	if err != nil {
		return 0, err
	}
	return uint16(minPort + value.Int64()), nil
}

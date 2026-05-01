package nat

import (
	"errors"
	"net"
	"strings"
	"time"

	natpmp "github.com/jackpal/go-nat-pmp"
)

const (
	natPMPDiscoveryTimeout = time.Second
	natPMPRequestTimeout   = 2 * time.Second
)

type natPMPRouter struct {
	gateway net.IP
	client  *natpmp.Client
}

func newNATPMPBackend() routerInterface {
	gateways := potentialGateways()
	if len(gateways) == 0 {
		return nil
	}
	found := make(chan *natPMPRouter, len(gateways))
	for _, gateway := range gateways {
		gateway := append(net.IP(nil), gateway...)
		go func() {
			client := natpmp.NewClientWithTimeout(gateway, natPMPRequestTimeout)
			if _, err := client.GetExternalAddress(); err != nil {
				found <- nil
				return
			}
			found <- &natPMPRouter{gateway: gateway, client: client}
		}()
	}

	timer := time.NewTimer(natPMPDiscoveryTimeout)
	defer timer.Stop()
	for range gateways {
		select {
		case router := <-found:
			if router != nil {
				return router
			}
		case <-timer.C:
			return nil
		}
	}
	return nil
}

func (r *natPMPRouter) AddMapping(protocol string, extport, intport int, name string, lifetime time.Duration) (uint16, error) {
	if lifetime <= 0 {
		return 0, errors.New("lifetime must not be <= 0")
	}
	if extport == 0 {
		extport = intport
	}
	result, err := r.client.AddPortMapping(strings.ToLower(protocol), intport, extport, int(lifetime/time.Second))
	if err != nil {
		return 0, err
	}
	return result.MappedExternalPort, nil
}

func (r *natPMPRouter) DeleteMapping(protocol string, extport, intport int) error {
	_, err := r.client.AddPortMapping(strings.ToLower(protocol), intport, 0, 0)
	return err
}

func (r *natPMPRouter) ExternalIP() (net.IP, error) {
	result, err := r.client.GetExternalAddress()
	if err != nil {
		return nil, err
	}
	return net.IPv4(
		result.ExternalIPAddress[0],
		result.ExternalIPAddress[1],
		result.ExternalIPAddress[2],
		result.ExternalIPAddress[3],
	), nil
}

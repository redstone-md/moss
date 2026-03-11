package nat

import (
	"net"
	"net/netip"
	"strconv"
)

type Type string

const (
	TypeUnknown        Type = "unknown"
	TypePublic         Type = "public"
	TypeFullCone       Type = "full_cone"
	TypeRestrictedCone Type = "restricted_cone"
	TypePortRestricted Type = "port_restricted_cone"
	TypeSymmetric      Type = "symmetric_nat"
	TypeCGNAT          Type = "cgnat"
)

type Profile struct {
	Type            Type   `json:"type"`
	PublicReachable bool   `json:"public_reachable"`
	ExternalAddress string `json:"external_address,omitempty"`
}

type Profiler struct{}

func NewProfiler() *Profiler {
	return &Profiler{}
}

func (p *Profiler) Detect(listenAddr string) Profile {
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return Profile{Type: TypeUnknown}
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return Profile{Type: TypeUnknown}
	}
	if addr.IsLoopback() {
		return Profile{Type: TypeRestrictedCone, ExternalAddress: listenAddr}
	}
	if addr.IsUnspecified() {
		return Profile{Type: TypeUnknown}
	}
	if isCarrierGrade(addr) {
		return Profile{Type: TypeCGNAT, ExternalAddress: listenAddr}
	}
	if addr.IsPrivate() {
		return Profile{Type: TypeFullCone, ExternalAddress: listenAddr}
	}
	if !addr.IsGlobalUnicast() {
		return Profile{Type: TypeUnknown}
	}
	return Profile{Type: TypePublic, PublicReachable: true, ExternalAddress: listenAddr}
}

func (p *Profiler) WithExternalAddress(profile Profile, externalAddr string) Profile {
	host, _, err := net.SplitHostPort(externalAddr)
	if err != nil {
		return profile
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return profile
	}
	profile.ExternalAddress = externalAddr
	if addr.IsGlobalUnicast() && !addr.IsPrivate() && !isCarrierGrade(addr) {
		profile.PublicReachable = true
		if profile.Type != TypePublic && profile.Type != TypeCGNAT {
			profile.Type = TypeFullCone
		}
	}
	return profile
}

func (p *Profiler) WithReachability(profile Profile, reachable bool) Profile {
	profile.PublicReachable = reachable
	return profile
}

func (p *Profiler) WithBindingObservations(profile Profile, observations []string) Profile {
	ports := bindingPorts(observations)
	if len(ports) < 2 {
		return profile
	}
	first := ports[0]
	for _, port := range ports[1:] {
		if port != first {
			profile.Type = TypeSymmetric
			profile.PublicReachable = false
			return profile
		}
	}
	if profile.Type == TypeFullCone || profile.Type == TypeUnknown {
		profile.Type = TypePortRestricted
	}
	return profile
}

func isCarrierGrade(addr netip.Addr) bool {
	if !addr.Is4() {
		return false
	}
	return netip.MustParsePrefix("100.64.0.0/10").Contains(addr)
}

func bindingPorts(observations []string) []int {
	out := make([]int, 0, len(observations))
	for _, observed := range observations {
		_, port, err := net.SplitHostPort(observed)
		if err != nil {
			continue
		}
		value, err := strconv.Atoi(port)
		if err != nil {
			continue
		}
		out = append(out, value)
	}
	return out
}

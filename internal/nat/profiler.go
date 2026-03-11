package nat

import (
	"net"
	"net/netip"
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

func isCarrierGrade(addr netip.Addr) bool {
	if !addr.Is4() {
		return false
	}
	return netip.MustParsePrefix("100.64.0.0/10").Contains(addr)
}

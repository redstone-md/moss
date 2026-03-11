package nat

import (
	"net"
	"strings"
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
	ip := net.ParseIP(host)
	if ip == nil {
		return Profile{Type: TypeUnknown}
	}
	if ip.IsLoopback() {
		return Profile{Type: TypeRestrictedCone, ExternalAddress: listenAddr}
	}
	if ip.IsPrivate() || strings.HasPrefix(host, "100.") {
		if strings.HasPrefix(host, "100.") {
			return Profile{Type: TypeCGNAT, ExternalAddress: listenAddr}
		}
		return Profile{Type: TypeFullCone, ExternalAddress: listenAddr}
	}
	return Profile{Type: TypePublic, PublicReachable: true, ExternalAddress: listenAddr}
}

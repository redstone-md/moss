package mesh

import (
	"testing"
	"time"
)

func TestPreviousRelayCarrierRequiresCurrentAuthenticatedPolicy(t *testing.T) {
	for _, scope := range []string{"valid", "no-target", "no-recovery", "no-via", "unverified", "no-key", "revoked", "untrusted", "private", "no-address", "via-denied", "target-denied", "self", "stopped", "waiting"} {
		t.Run(scope, func(t *testing.T) {
			n, err := NewNode("previous-via-policy", nil, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			n.started = true
			n.explicitTargets["target"] = &explicitTarget{identityRetry: true, recoveryVia: "via"}
			n.knownPeers["via"] = knownPeer{id: "via", addr: "198.51.100.1:1", verified: true, noiseStatic: make([]byte, 32), natTrusted: true, relayCapable: true, publicReachable: true}
			info := n.knownPeers["via"]
			switch scope {
			case "no-target":
				delete(n.explicitTargets, "target")
			case "no-recovery":
				n.explicitTargets["target"].identityRetry = false
			case "no-via":
				n.explicitTargets["target"].recoveryVia = ""
			case "unverified":
				info.verified = false
			case "no-key":
				info.noiseStatic = nil
			case "revoked":
				info.relayCapable = false
			case "untrusted":
				info.natTrusted = false
			case "private":
				info.publicReachable = false
			case "no-address":
				info.addr = ""
			case "via-denied":
				n.allowlist = map[string]struct{}{"target": {}}
			case "target-denied":
				n.allowlist = map[string]struct{}{"via": {}}
			case "self":
				n.explicitTargets["target"].recoveryVia = "target"
			case "stopped":
				n.started = false
			case "waiting":
				n.activity.requested.Store(int32(ActivityWaiting))
			}
			n.knownPeers["via"] = info
			via, _ := n.previousRelayCarrierLocked("target")
			if (via == "via") != (scope == "valid") {
				t.Fatal("previous carrier bypassed current policy: " + scope)
			}
		})
	}
}

func TestPreviousRelayCarrierIsOwnedByCurrentEstablishedRemoval(t *testing.T) {
	for _, scope := range []string{"current-lost", "pending", "stale-session", "live-carrier", "unregistered"} {
		t.Run(scope, func(t *testing.T) {
			n, err := NewNode("previous-via-owner", nil, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			n.explicitTargets["target"] = &explicitTarget{}
			n.peers["target"] = &peerConn{id: "target", relayed: true, viaPeerID: "via", relaySessionID: "current", connectedAt: time.Now()}
			r := relayLocalSession{sessionID: "current", viaPeerID: "via", remotePeerID: "target", established: true}
			switch scope {
			case "pending":
				r.established = false
			case "stale-session":
				r.sessionID = "old"
			case "live-carrier":
				n.peers["via"] = &peerConn{id: "via"}
			case "unregistered":
				delete(n.explicitTargets, "target")
			}
			n.removeRelayedPeerLocked(r)
			got := ""
			if x := n.explicitTargets["target"]; x != nil {
				got = x.recoveryVia
			}
			if (got == "via") != (scope == "current-lost") {
				t.Fatal("unowned relay state changed remembered carrier: " + scope)
			}
		})
	}
}

func TestAuthenticatedReplacementConsumesPreviousRelayCarrier(t *testing.T) {
	for _, opted := range []bool{false, true} {
		n, err := NewNode("previous-via-consume", nil, DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		n.activity.optedIn = opted
		n.explicitTargets["target"] = &explicitTarget{identityRetry: true, recoveryVia: "via"}
		n.resetExplicitTargetLocked("target")
		if n.explicitTargets["target"].recoveryVia != "via" {
			t.Fatal("speculative generic admission consumed previous carrier")
		}
		n.confirmExplicitRelayRecoveryLocked("target")
		if n.explicitTargets["target"].recoveryVia != "" || n.explicitTargets["target"].identityRetry != opted {
			t.Fatal("authenticated replacement broke recovery ownership or lifecycle preference")
		}
	}
}

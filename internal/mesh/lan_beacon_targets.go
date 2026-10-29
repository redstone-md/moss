package mesh

import "net"

type lanBeaconTargets struct {
	payload    []byte
	interfaces []net.Interface
	broadcasts []*net.UDPAddr
}

func (n *Node) prepareLANBeacon(interfaces *interfaceSnapshot) (lanBeaconTargets, error) {
	payload, err := n.lanDiscoveryPayloadWithInterfaces(interfaces)
	if err != nil {
		return lanBeaconTargets{}, err
	}
	targets := lanBeaconTargets{payload: payload, interfaces: interfaces.multicast()}
	if len(targets.interfaces) > 0 {
		targets.broadcasts = lanDiscoveryBroadcastAddrs(interfaces)
	}
	return targets, nil
}

package nat

import (
	ethnat "github.com/ethereum/go-ethereum/p2p/nat"
)

func newUPnPBackend() routerInterface {
	return ethnat.UPnP()
}

func newNATPMPBackend() routerInterface {
	return ethnat.PMP(nil)
}

func newPCPBackend() routerInterface {
	// The go-ethereum NAT helper does not expose PCP port mappings directly.
	return nil
}

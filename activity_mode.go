package moss

import "github.com/redstone-md/moss/internal/mesh"

// ActivityMode opts one node into a runtime lifecycle; default is active.
type ActivityMode = mesh.ActivityMode

const (
	ActivityActive  = mesh.ActivityActive
	ActivityWaiting = mesh.ActivityWaiting
)

// SetActivityMode requests a bounded asynchronous transition without replacing
// the node. Inspect MeshInfoJSON or event 12 for actual state/reachability.
func (n *Node) SetActivityMode(mode ActivityMode) error {
	return errorCode(n.inner.SetActivityMode(mode))
}

// NotifyNetworkChanged schedules a bounded waiting-path check without changing
// policy, discarding a healthy carrier or declaring reachability from a hint.
func (n *Node) NotifyNetworkChanged() error {
	return errorCode(n.inner.NotifyNetworkChanged())
}

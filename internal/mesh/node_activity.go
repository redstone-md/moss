package mesh

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// ActivityMode is an opt-in runtime policy, independent of host UI state.
type ActivityMode int32

const (
	ActivityActive ActivityMode = iota
	ActivityWaiting
)

func (mode ActivityMode) String() string {
	if mode == ActivityWaiting {
		return "waiting"
	}
	return "active"
}

type activityState struct {
	requested       atomic.Int32
	actual          ActivityMode // remaining fields guarded by Node.mu
	optedIn         bool
	wake            chan struct{}
	maintenanceWake chan struct{}
	networkWake     chan struct{}
	networkPending  bool
	networkDue      time.Time
	networkLast     time.Time
	attachment      string
	candidate       string
	candidateAddr   string
	candidateReady  chan struct{}
	attempt         uint64
	reachability    string
	activeCancel    context.CancelFunc
	activeDone      <-chan struct{}
	attemptCancel   context.CancelFunc
	retry           time.Duration
	lastCandidate   string
	lastReport      activityReport
	presenceAt      time.Time
	presencePeers   [2]*peerConn
	rendezvous      *peerConn
	draining        map[*peerConn]<-chan struct{}
	recovering      map[string]*peerConn
	migrationOwned  bool
}

type activityReport struct {
	Mode         string `json:"activity_mode"`
	Requested    string `json:"requested_activity_mode"`
	Reachability string `json:"waiting_reachability"`
	Attachments  int    `json:"waiting_attachment_count"`
}

type waitingAttemptKey struct{}

// SetActivityMode accepts an asynchronous policy change without recreating the
// node or host-owned state. Connectivity is reported separately by MeshInfo.
func (n *Node) SetActivityMode(mode ActivityMode) int32 {
	if mode != ActivityActive && mode != ActivityWaiting {
		return MOSS_ERR_ACTIVITY_MODE_INVALID
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started {
		return MOSS_ERR_NOT_STARTED
	}
	n.activity.optedIn = true
	if ActivityMode(n.activity.requested.Load()) == mode {
		return MOSS_OK
	}
	n.activity.requested.Store(int32(mode))
	if mode == ActivityActive {
		n.activity.networkPending = false
		n.activity.recovering = nil
		n.activity.migrationOwned = false
		n.activity.presencePeers = [2]*peerConn{}
		n.activity.rendezvous = nil
		n.activity.presenceAt = time.Time{}
	}
	if n.activity.attemptCancel != nil {
		n.activity.attemptCancel()
	}
	if mode == ActivityWaiting {
		n.cancelActiveWorkLocked()
	}
	select {
	case n.activity.wake <- struct{}{}:
	default:
	}
	select {
	case n.activity.maintenanceWake <- struct{}{}:
	default:
	}
	return MOSS_OK
}

func (n *Node) waitingRequested() bool {
	return ActivityMode(n.activity.requested.Load()) == ActivityWaiting
}

func (n *Node) activityLoop(ctx context.Context) {
	defer n.wg.Done()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	networkTimer := time.NewTimer(time.Hour)
	networkTimer.Stop()
	defer networkTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.activity.networkWake:
			n.processNetworkSignal(ctx, networkTimer)
			continue
		case <-networkTimer.C:
			n.processNetworkSignal(ctx, networkTimer)
			continue
		case <-n.activity.wake:
		case <-timer.C:
		}
		if n.waitingRequested() {
			n.reconcileWaiting(ctx)
			n.mu.Lock()
			if n.activity.retry == 0 {
				n.activity.retry = 5 * time.Second
			} else {
				n.activity.retry = min(5*time.Minute, n.activity.retry*2)
			}
			wait := n.activity.retry
			if n.activity.reachability == "ready" && len(n.activity.recovering) == 0 {
				n.activity.retry = 0
				wait = time.Hour
			}
			n.mu.Unlock()
			if wait == time.Hour {
				timer.Stop()
			} else {
				// Jitter never retries earlier than the initial five-second floor.
				timer.Reset(min(5*time.Minute, wait+time.Duration(rand.Int64N(int64(wait/5)))))
			}
		} else {
			n.activateOrdinaryWork()
			timer.Stop()
		}
	}
}

func (n *Node) emitActivityMode() {
	n.mu.Lock()
	detail := activityReport{
		Mode: n.activity.actual.String(), Requested: ActivityMode(n.activity.requested.Load()).String(),
		Reachability: n.activity.reachability, Attachments: n.waitingAttachmentCountLocked(),
	}
	if detail == n.activity.lastReport {
		n.mu.Unlock()
		return
	}
	n.activity.lastReport = detail
	n.mu.Unlock()
	n.enqueueEvent(EventActivityModeChanged, detail)
}

func (n *Node) wakeActivity() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.started || !n.waitingRequested() {
		return
	}
	select {
	case n.activity.wake <- struct{}{}:
	default:
	}
}

func (n *Node) waitingAttachmentCountLocked() int {
	ids := [3]string{n.activity.attachment, n.activity.candidate}
	if owner := n.activity.rendezvous; owner != nil && n.peers[owner.id] == owner {
		ids[2] = owner.id
	}
	count := 0
	for i, id := range ids {
		duplicate := id == ""
		for _, previous := range ids[:i] {
			duplicate = duplicate || previous == id
		}
		if peer := n.peers[id]; !duplicate && peer != nil && !peer.relayed {
			count++
			if state := peer.replacement.Load(); state != nil && !state.committed.Load() && !state.old.readClosed.Load() {
				count++
			}
		}
	}
	return count
}

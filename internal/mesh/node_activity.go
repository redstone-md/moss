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
	for {
		select {
		case <-ctx.Done():
			return
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
			if n.activity.reachability == "ready" {
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
	count := 0
	for i, id := range []string{n.activity.attachment, n.activity.candidate} {
		if i == 1 && id == n.activity.attachment {
			continue
		}
		if peer := n.peers[id]; peer != nil && !peer.relayed {
			count++
		}
	}
	return count
}

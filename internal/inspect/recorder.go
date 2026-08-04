package inspect

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Recorder writes a .mossrec file: the same events a debug session streams, plus
// periodic snapshots of the node's state, as newline-delimited JSON.
//
// One line per frame, flushed continuously, because the file that matters most
// is the one a crash left behind — a format that needs a clean close would lose
// exactly the recording worth having. A truncated last line parses as far as it
// goes and the rest replays normally.
//
// The same format is what a public scope persists for its epoch history, so the
// replay UI is identical whether you opened last night's crash or the network's
// past month.
type Recorder struct {
	path    string
	maxSize int64

	mu      sync.Mutex
	file    *os.File
	w       *bufio.Writer
	written int64
	closed  bool
	dropped uint64
}

type frame struct {
	TS     int64  `json:"ts"`
	Metric string `json:"metric"`
	Data   any    `json:"data"`
}

// NewRecorder opens (truncating) the target file.
//
// maxMB caps growth: a debug recording left on overnight must not fill the disk
// out from under the node it is observing. On reaching the cap the recorder
// stops writing and says so, rather than rotating — a half-file with a known end
// beats two files whose boundary lands mid-incident.
func NewRecorder(path string, maxMB int) (*Recorder, error) {
	if maxMB <= 0 {
		maxMB = 256
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("mossrec: %w", err)
	}
	return &Recorder{
		path:    path,
		maxSize: int64(maxMB) << 20,
		file:    f,
		w:       bufio.NewWriterSize(f, 64<<10),
	}, nil
}

// Write appends one frame.
func (r *Recorder) Write(metric string, ts int64, data any) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.written >= r.maxSize {
		r.dropped++
		return
	}
	line, err := json.Marshal(frame{TS: ts, Metric: metric, Data: data})
	if err != nil {
		r.dropped++
		return
	}
	n, err := r.w.Write(append(line, '\n'))
	if err != nil {
		r.dropped++
		return
	}
	r.written += int64(n)
	// Flushed per frame on purpose: buffering trades the last seconds of a
	// recording for throughput, and the last seconds are the incident.
	_ = r.w.Flush()
}

// Run streams the bus into the file until ctx-like stop, sampling snapshots on
// the way. It returns when Close is called.
func (r *Recorder) Run(bus *Bus, provider Provider, metrics []string, every time.Duration) {
	events, cancel := bus.Subscribe(nil, 4096)
	defer cancel()

	if every <= 0 {
		every = 5 * time.Second
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	// Backfill whatever the ring already holds, so a recording started after
	// something went wrong still contains the run-up to it.
	for _, ev := range bus.History(0, nil) {
		r.Write("event", ev.TS, ev)
	}

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			r.Write("event", ev.TS, ev)
		case <-ticker.C:
			if r.isClosed() {
				return
			}
			for _, m := range metrics {
				if provider == nil {
					break
				}
				data, err := provider.Metric(m, nil)
				if err != nil || data == nil {
					continue
				}
				r.Write(m, bus.ElapsedNanos(), data)
			}
		}
	}
}

func (r *Recorder) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// Stats describes the recording itself, so the UI can show it is still growing
// and whether anything was lost.
func (r *Recorder) Stats() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return map[string]any{
		"path":      r.path,
		"bytes":     r.written,
		"max_bytes": r.maxSize,
		"dropped":   r.dropped,
		"at_limit":  r.written >= r.maxSize,
		"is_closed": r.closed,
	}
}

func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	_ = r.w.Flush()
	return r.file.Close()
}

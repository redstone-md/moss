package inspect

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"
)

// Config turns the debug plane on. Every field is off or empty by default: a
// node that is not explicitly asked to open a debug port does not open one.
type Config struct {
	// Enabled opens the loopback debug listener.
	Enabled bool `json:"enabled"`
	// Addr to bind. Empty means 127.0.0.1:0 — an ephemeral loopback port, which
	// is what discovery is for. A non-loopback address is refused outright.
	Addr string `json:"addr"`
	// Token authorises attachment. Empty means one is generated at startup and
	// printed; a debug session exposes peer identities and message ids, so it is
	// never left unauthenticated.
	Token string `json:"token"`
	// RingSize is how many recent events are kept for a debugger that attaches
	// after something has already gone wrong.
	RingSize int `json:"ring_size"`
	// ServeUI serves the embedded MossScope bundle at /, so a browser can be
	// pointed straight at the node without installing anything.
	ServeUI bool `json:"serve_ui"`
	// RecordPath, when set, streams everything to a .mossrec file as it happens.
	// The point is the crash you were not watching: a recording that needs a
	// clean shutdown is the one you will not have.
	RecordPath string `json:"record_path"`
	// RecordMaxMB caps the recording so an overnight session cannot fill the
	// disk out from under the node it observes. 0 → 256 MB.
	RecordMaxMB int `json:"record_max_mb"`
	// RecordEverySec is how often node snapshots are sampled into the recording.
	// 0 → 5s.
	RecordEverySec int `json:"record_every_sec"`
}

// Provider lets the node answer snapshot questions without this package
// importing mesh (which imports this one). The node registers itself; a nil
// provider simply means those replies are empty.
type Provider interface {
	// Metric returns the current value of a named dataset, e.g. "peers".
	Metric(name string, params map[string]string) (any, error)
	// Trace reconstructs the causal chain of one message id.
	Trace(messageID string) (any, error)
	// Describe returns stable identity for the session banner.
	Describe() SessionInfo
}

// SessionInfo is what discovery reveals before authentication: enough for a
// browser to say "there is a moss node here, open it?" and nothing more. No
// peer list, no addresses, no mesh ids.
type SessionInfo struct {
	Moss          bool   `json:"moss"`
	Session       string `json:"session"`
	Node          string `json:"node"` // short public key, first 4 bytes
	Version       string `json:"version"`
	PID           int    `json:"pid"`
	StartedUnix   int64  `json:"started_unix"`
	RequiresToken bool   `json:"requires_token"`
	WS            string `json:"ws"`
	UI            bool   `json:"ui"`
}

// Server is the loopback debug endpoint: discovery, WebSocket, optional UI.
type Server struct {
	cfg      Config
	bus      *Bus
	provider Provider
	ui       http.Handler

	token   string
	session string
	started time.Time
	ln      net.Listener
	srv     *http.Server
}

// New prepares a debug server. Nothing is bound until Start.
func New(cfg Config, bus *Bus, provider Provider, ui http.Handler) (*Server, error) {
	if cfg.RingSize <= 0 {
		cfg.RingSize = 16384
	}
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if err := requireLoopback(cfg.Addr); err != nil {
		return nil, err
	}
	token := cfg.Token
	if token == "" {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		token = hex.EncodeToString(b[:])
	}
	var sid [8]byte
	if _, err := rand.Read(sid[:]); err != nil {
		return nil, err
	}
	return &Server{
		cfg:      cfg,
		bus:      bus,
		provider: provider,
		ui:       ui,
		token:    token,
		session:  hex.EncodeToString(sid[:]),
		started:  time.Now(),
	}, nil
}

// Start binds and serves. The returned URL carries the token, so printing it is
// all a host has to do for the user to get in.
func (s *Server) Start() (string, error) {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return "", fmt.Errorf("debug listen: %w", err)
	}
	s.ln = ln
	s.srv = &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = s.srv.Serve(ln) }()

	u := url.URL{Scheme: "http", Host: ln.Addr().String(), Path: "/"}
	q := url.Values{"token": {s.token}}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func (s *Server) Token() string { return s.token }

func (s *Server) Close() error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Close()
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()

	// Discovery. Deliberately unauthenticated and deliberately thin: a browser
	// scanning loopback ports needs to know a moss node is here and that it will
	// ask for a token. Anything richer would be an information leak to any local
	// page that decides to scan.
	mux.HandleFunc("/debug/hello", func(w http.ResponseWriter, r *http.Request) {
		if !s.allowOrigin(w, r) {
			return
		}
		writeJSON(w, s.info())
	})

	mux.HandleFunc("/debug/ws", s.serveWS)

	if s.cfg.ServeUI && s.ui != nil {
		mux.Handle("/", s.ui)
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("content-type", "text/plain; charset=utf-8")
			fmt.Fprintf(w, "moss debug plane · session %s\nattach: moss-scope attach --endpoint http://%s\n",
				s.session, s.Addr())
		})
	}
	return mux
}

func (s *Server) info() SessionInfo {
	info := SessionInfo{
		Moss:          true,
		Session:       s.session,
		Version:       "1",
		PID:           os.Getpid(),
		StartedUnix:   s.started.Unix(),
		RequiresToken: true,
		WS:            "/debug/ws",
		UI:            s.cfg.ServeUI && s.ui != nil,
	}
	if s.provider != nil {
		d := s.provider.Describe()
		if d.Node != "" {
			info.Node = d.Node
		}
		if d.Version != "" {
			info.Version = d.Version
		}
	}
	return info
}

// allowOrigin implements the one security rule a loopback debug port genuinely
// needs: a page from an arbitrary origin must not be able to talk to it. Same
// check guards both the JSON endpoint and the WebSocket, because a WebSocket
// ignores the same-origin policy on its own.
func (s *Server) allowOrigin(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// curl, or a same-origin fetch from the page this server itself served.
		w.Header().Set("cache-control", "no-store")
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || !isLoopbackHost(u.Hostname()) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return false
	}
	w.Header().Set("access-control-allow-origin", origin)
	w.Header().Set("vary", "origin")
	w.Header().Set("cache-control", "no-store")
	return true
}

func (s *Server) authorised(r *http.Request) bool {
	given := r.URL.Query().Get("token")
	if given == "" {
		given = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	return subtle.ConstantTimeCompare([]byte(given), []byte(s.token)) == 1
}

func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("debug addr %q: %w", addr, err)
	}
	if !isLoopbackHost(host) {
		// Refusing rather than warning: a debug port carries peer identities and
		// message contents, and "I'll bind it publicly just this once" is how
		// that ends up on the internet.
		return errors.New("debug addr must be loopback (127.0.0.1 or ::1)")
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if host == "" || host == "localhost" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func processSample() map[string]any {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return map[string]any{
		"goroutines":  runtime.NumGoroutine(),
		"heap_mb":     float64(m.HeapAlloc) / (1 << 20),
		"sys_mb":      float64(m.Sys) / (1 << 20),
		"gc_pause_ms": float64(m.PauseNs[(m.NumGC+255)%256]) / 1e6,
		"num_gc":      m.NumGC,
	}
}

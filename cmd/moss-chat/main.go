package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	mcrypto "moss/internal/crypto"
	"moss/internal/mesh"
)

const (
	systemRoom  = "system"
	defaultRoom = "lobby"
	defaultMesh = "moss-chat-demo"
)

type stringListFlag []string

func (f *stringListFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("value cannot be empty")
	}
	*f = append(*f, value)
	return nil
}

type options struct {
	nickname     string
	meshID       string
	listenPort   int
	peers        []string
	rooms        []string
	trackers     []string
	noTrackers   bool
	identityFile string
}

func main() {
	opts, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	identityPath, err := resolveIdentityPath(opts.nickname, opts.identityFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	identity, err := loadOrCreateIdentity(identityPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "identity error: %v\n", err)
		os.Exit(1)
	}

	cfg := mesh.DefaultConfig()
	cfg.ListenPort = opts.listenPort
	cfg.StaticPeers = append([]string(nil), opts.peers...)
	cfg.AnnounceIntervalSec = 3
	cfg.GossipSub.HeartbeatMS = 250
	if opts.noTrackers {
		cfg.Trackers = []string{}
	} else if len(opts.trackers) > 0 {
		cfg.Trackers = append([]string(nil), opts.trackers...)
	}

	node, err := mesh.NewNodeWithIdentity(opts.meshID, nil, cfg, identity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "node init failed: %v\n", err)
		os.Exit(1)
	}
	pub := node.PublicKey()

	app := newChatApp(chatConfig{
		node:         node,
		meshID:       opts.meshID,
		nickname:     opts.nickname,
		rooms:        opts.rooms,
		bootstrap:    opts.peers,
		trackers:     trackerMode(cfg.Trackers, opts.noTrackers),
		identityPath: identityPath,
		localPeerID:  hex.EncodeToString(pub[:]),
	})
	if err := app.run(); err != nil {
		fmt.Fprintf(os.Stderr, "chat error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() (options, error) {
	var opts options
	opts.meshID = defaultMesh
	opts.rooms = []string{defaultRoom}

	var peers stringListFlag
	var rooms stringListFlag
	var trackers stringListFlag

	flag.StringVar(&opts.nickname, "nickname", "", "nickname shown in chat")
	flag.StringVar(&opts.meshID, "mesh", defaultMesh, "mesh identifier shared by participants")
	flag.IntVar(&opts.listenPort, "listen-port", 0, "local listen port")
	flag.Var(&peers, "peer", "static peer address HOST:PORT (repeatable)")
	flag.Var(&rooms, "room", "initial room to join (repeatable)")
	flag.Var(&trackers, "tracker", "override tracker list (repeatable)")
	flag.BoolVar(&opts.noTrackers, "no-trackers", false, "disable tracker bootstrap")
	flag.StringVar(&opts.identityFile, "identity-file", "", "path to persistent identity file")
	flag.Parse()

	if strings.TrimSpace(opts.nickname) == "" {
		return options{}, errors.New("--nickname is required")
	}
	opts.nickname = strings.TrimSpace(opts.nickname)
	opts.peers = append([]string(nil), peers...)
	if len(rooms) > 0 {
		opts.rooms = nil
		for _, room := range rooms {
			normalized, err := normalizeRoom(room)
			if err != nil {
				return options{}, err
			}
			if !containsString(opts.rooms, normalized) {
				opts.rooms = append(opts.rooms, normalized)
			}
		}
	}
	opts.trackers = append([]string(nil), trackers...)
	return opts, nil
}

func resolveIdentityPath(nickname, explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	safeName := sanitizeName(nickname)
	return filepath.Join(configDir, "moss-chat", safeName+".identity"), nil
}

func loadOrCreateIdentity(path string) (*mcrypto.Identity, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		return mcrypto.DecodeIdentity(raw)
	case !os.IsNotExist(err):
		return nil, err
	}

	identity, err := mcrypto.NewIdentity()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, identity.Encode(), 0o600); err != nil {
		return nil, err
	}
	return identity, nil
}

func sanitizeName(raw string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(raw)) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "user"
	}
	return b.String()
}

func normalizeRoom(raw string) (string, error) {
	room := strings.ToLower(strings.TrimSpace(raw))
	room = strings.TrimPrefix(room, "#")
	if room == "" {
		return "", errors.New("room name cannot be empty")
	}
	return room, nil
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func trackerMode(trackers []string, disabled bool) string {
	if disabled {
		return "disabled"
	}
	if len(trackers) == 0 {
		return "default"
	}
	return "custom"
}

func defaultBinaryName() string {
	if runtime.GOOS == "windows" {
		return "moss-chat.exe"
	}
	return "moss-chat"
}

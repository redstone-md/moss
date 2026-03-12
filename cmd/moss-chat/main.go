package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	mcrypto "moss/internal/crypto"
	"moss/internal/mesh"
)

const (
	systemRoom   = "system"
	controlRoom  = "__moss_chat_control__"
	defaultRoom  = "lobby"
	defaultMesh  = "moss-chat-demo"
	maxRoomLines = 500
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
	opts, err = promptMissingOptions(opts, os.Stdin, os.Stdout)
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

func promptMissingOptions(opts options, in io.Reader, out io.Writer) (options, error) {
	if strings.TrimSpace(opts.nickname) != "" {
		return finalizeOptions(opts)
	}
	if in == os.Stdin && !isInteractiveInput() {
		return options{}, errors.New("--nickname is required when stdin is not interactive")
	}

	reader := bufio.NewReader(in)
	fmt.Fprintln(out, "Moss Chat setup")
	fmt.Fprintln(out, "Press Enter to accept defaults.")

	nickname, err := promptRequired(reader, out, "Nickname")
	if err != nil {
		return options{}, err
	}
	opts.nickname = nickname

	meshID, err := promptDefault(reader, out, "Mesh ID", opts.meshID)
	if err != nil {
		return options{}, err
	}
	opts.meshID = meshID

	portText, err := promptDefault(reader, out, "Listen port (0 = auto)", strconv.Itoa(opts.listenPort))
	if err != nil {
		return options{}, err
	}
	port, err := strconv.Atoi(strings.TrimSpace(portText))
	if err != nil || port < 0 || port > 65535 {
		return options{}, errors.New("listen port must be a number between 0 and 65535")
	}
	opts.listenPort = port

	roomDefault := defaultRoom
	if len(opts.rooms) > 0 {
		roomDefault = opts.rooms[0]
	}
	room, err := promptDefault(reader, out, "Initial room", roomDefault)
	if err != nil {
		return options{}, err
	}
	opts.rooms = []string{room}

	peerText, err := promptDefault(reader, out, "Static peer HOST:PORT (optional)", "")
	if err != nil {
		return options{}, err
	}
	if peerText != "" {
		for _, peer := range strings.Split(peerText, ",") {
			peer = strings.TrimSpace(peer)
			if peer != "" {
				opts.peers = append(opts.peers, peer)
			}
		}
	}

	disableTrackers, err := promptDefault(reader, out, "Disable tracker bootstrap?", "n")
	if err != nil {
		return options{}, err
	}
	opts.noTrackers = isYes(disableTrackers)

	return finalizeOptions(opts)
}

func finalizeOptions(opts options) (options, error) {
	opts.nickname = strings.TrimSpace(opts.nickname)
	if opts.nickname == "" {
		return options{}, errors.New("nickname cannot be empty")
	}

	opts.meshID = strings.TrimSpace(opts.meshID)
	if opts.meshID == "" {
		opts.meshID = defaultMesh
	}

	if opts.listenPort < 0 || opts.listenPort > 65535 {
		return options{}, errors.New("listen port must be a number between 0 and 65535")
	}

	if len(opts.rooms) == 0 {
		opts.rooms = []string{defaultRoom}
	}
	normalizedRooms := make([]string, 0, len(opts.rooms))
	for _, room := range opts.rooms {
		normalized, err := normalizeRoom(room)
		if err != nil {
			return options{}, err
		}
		if !containsString(normalizedRooms, normalized) {
			normalizedRooms = append(normalizedRooms, normalized)
		}
	}
	opts.rooms = normalizedRooms

	normalizedPeers := make([]string, 0, len(opts.peers))
	for _, peer := range opts.peers {
		peer = strings.TrimSpace(peer)
		if peer != "" && !containsString(normalizedPeers, peer) {
			normalizedPeers = append(normalizedPeers, peer)
		}
	}
	opts.peers = normalizedPeers

	normalizedTrackers := make([]string, 0, len(opts.trackers))
	for _, tracker := range opts.trackers {
		tracker = strings.TrimSpace(tracker)
		if tracker != "" && !containsString(normalizedTrackers, tracker) {
			normalizedTrackers = append(normalizedTrackers, tracker)
		}
	}
	opts.trackers = normalizedTrackers
	return opts, nil
}

func promptRequired(reader *bufio.Reader, out io.Writer, label string) (string, error) {
	for {
		value, err := promptDefault(reader, out, label, "")
		if err != nil {
			return "", err
		}
		value = strings.TrimSpace(value)
		if value != "" {
			return value, nil
		}
		fmt.Fprintln(out, "Value is required.")
	}
}

func promptDefault(reader *bufio.Reader, out io.Writer, label, defaultValue string) (string, error) {
	if defaultValue == "" {
		fmt.Fprintf(out, "%s: ", label)
	} else {
		fmt.Fprintf(out, "%s [%s]: ", label, defaultValue)
	}
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	value := strings.TrimSpace(line)
	if value == "" {
		return defaultValue, nil
	}
	return value, nil
}

func isInteractiveInput() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func isYes(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "y", "yes", "1", "true":
		return true
	default:
		return false
	}
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

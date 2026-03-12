package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"moss/internal/mesh"
)

type chatConfig struct {
	node         *mesh.Node
	meshID       string
	nickname     string
	rooms        []string
	bootstrap    []string
	trackers     string
	identityPath string
	localPeerID  string
}

type roomState struct {
	name       string
	title      string
	subscribed bool
	lines      []string
	unread     int
}

type chatPayload struct {
	Kind   string `json:"kind,omitempty"`
	Nick   string `json:"nick,omitempty"`
	Text   string `json:"text,omitempty"`
	SentAt string `json:"sent_at,omitempty"`
}

type eventDetail struct {
	Peer           string `json:"peer,omitempty"`
	Addr           string `json:"addr,omitempty"`
	CandidatePeers int    `json:"candidate_peers,omitempty"`
	ConnectedPeers int    `json:"connected_peers,omitempty"`
	Error          string `json:"error,omitempty"`
	Session        string `json:"session,omitempty"`
	NATType        string `json:"nat_type,omitempty"`
}

type runtimeInfo struct {
	MeshID         string   `json:"mesh_id"`
	ListenPort     int      `json:"listen_port"`
	AdvertisedAddr string   `json:"advertised_addr"`
	PeerCount      int      `json:"peer_count"`
	Peers          []string `json:"peers"`
	KnownPeerCount int      `json:"known_peer_count"`
	KnownPeers     []string `json:"known_peers"`
	Channels       []string `json:"channels"`
	NATType        string   `json:"nat_type"`
	PublicKey      string   `json:"public_key"`
	SupernodeReady bool     `json:"supernode_ready"`
}

type chatApp struct {
	node         *mesh.Node
	meshID       string
	nickname     string
	identityPath string
	bootstrap    []string
	trackers     string
	localPeerID  string

	ui       *tview.Application
	root     *tview.Flex
	header   *tview.TextView
	rooms    *tview.List
	messages *tview.TextView
	input    *tview.InputField
	help     *tview.TextView
	status   *tview.TextView

	stopCh chan struct{}
	once   sync.Once
	mu     sync.RWMutex

	running      bool
	syncingRooms bool
	visibleRooms []string
	roomOrder    []string
	roomByName   map[string]*roomState
	currentRoom  string
	info         runtimeInfo
}

func newChatApp(cfg chatConfig) *chatApp {
	app := &chatApp{
		node:         cfg.node,
		meshID:       cfg.meshID,
		nickname:     cfg.nickname,
		identityPath: cfg.identityPath,
		bootstrap:    append([]string(nil), cfg.bootstrap...),
		trackers:     cfg.trackers,
		localPeerID:  cfg.localPeerID,
		stopCh:       make(chan struct{}),
		roomByName: map[string]*roomState{
			systemRoom: {
				name:       systemRoom,
				title:      "System",
				subscribed: true,
			},
		},
		roomOrder:   []string{systemRoom},
		currentRoom: defaultRoom,
	}
	for _, room := range cfg.rooms {
		app.ensureRoom(room, true)
	}
	if len(cfg.rooms) > 0 {
		app.currentRoom = cfg.rooms[0]
	}
	app.buildUI()
	return app
}

func (c *chatApp) run() error {
	c.node.SetMessageCallback(c.onMessage)
	c.node.SetEventCallback(c.onEvent)
	if code := c.node.Start(); code != mesh.MOSS_OK {
		return fmt.Errorf("node start failed: %d", code)
	}
	for _, room := range c.subscribedRooms() {
		if room == systemRoom {
			continue
		}
		if code := c.node.Subscribe(room); code != mesh.MOSS_OK {
			_ = c.node.Stop()
			return fmt.Errorf("subscribe %s failed: %d", room, code)
		}
	}
	c.systemMessage(
		fmt.Sprintf(
			"Connected as %s. Active mesh: %s. F2/F3 switch rooms. /help for commands.",
			c.nickname,
			c.infoOrMeshID(),
		),
	)
	switch c.trackers {
	case "disabled":
		c.systemMessage("Tracker bootstrap is disabled. Use inbound peers or /connect HOST:PORT.")
	case "custom":
		c.systemMessage("Tracker bootstrap is enabled with a custom tracker set.")
	default:
		c.systemMessage("Tracker bootstrap is enabled with the default public tracker set.")
	}
	if len(c.bootstrap) > 0 {
		c.systemMessage("Static peers: " + strings.Join(c.bootstrap, ", "))
	}
	c.systemMessage("Identity file: " + c.identityPath)
	go c.statusLoop()
	defer c.shutdown()
	c.ui.SetRoot(c.root, true)
	c.ui.SetFocus(c.input)
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()
	c.refresh()
	return c.ui.Run()
}

func (c *chatApp) shutdown() {
	c.once.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
		_ = c.node.Stop()
	})
}

func (c *chatApp) buildUI() {
	c.ui = tview.NewApplication()

	c.header = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft).
		SetText("[::b]Moss Chat[::-]")
	c.header.SetBackgroundColor(tcell.NewRGBColor(38, 50, 56))

	c.rooms = tview.NewList().ShowSecondaryText(false)
	c.rooms.SetBorder(true).SetTitle(" Rooms ")
	c.rooms.SetChangedFunc(func(index int, _, _ string, _ rune) {
		c.mu.Lock()
		if c.syncingRooms {
			c.mu.Unlock()
			return
		}
		if index >= 0 && index < len(c.visibleRooms) {
			c.currentRoom = c.visibleRooms[index]
			if room := c.roomByName[c.currentRoom]; room != nil {
				room.unread = 0
			}
		}
		c.mu.Unlock()
		c.refresh()
	})

	c.messages = tview.NewTextView().
		SetDynamicColors(false).
		SetScrollable(true).
		SetWrap(true)
	c.messages.SetBorder(true).SetTitle(" Messages ")

	c.input = tview.NewInputField().
		SetLabel("> ")
	c.input.SetBorder(true).SetTitle(" Composer ")
	c.input.SetDoneFunc(func(key tcell.Key) {
		if key != tcell.KeyEnter {
			return
		}
		text := strings.TrimSpace(c.input.GetText())
		c.input.SetText("")
		if text != "" {
			c.handleInput(text)
		}
	})

	c.help = tview.NewTextView().SetDynamicColors(true)
	c.help.SetText("Enter send | /join room | /leave | /goto room | /nick NAME | /status | /net | F2/F3 | Ctrl-C")
	c.status = tview.NewTextView().SetDynamicColors(true)

	body := tview.NewFlex().
		AddItem(c.rooms, 26, 1, false).
		AddItem(c.messages, 0, 4, false)
	footer := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(c.input, 3, 0, true).
		AddItem(c.help, 1, 0, false).
		AddItem(c.status, 1, 0, false)

	c.root = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(c.header, 1, 0, false).
		AddItem(body, 0, 1, false).
		AddItem(footer, 5, 0, true)

	c.root.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyF2:
			c.cycleRoom(-1)
			return nil
		case tcell.KeyF3:
			c.cycleRoom(1)
			return nil
		case tcell.KeyCtrlC:
			c.ui.Stop()
			return nil
		default:
			return event
		}
	})

	c.refresh()
}

func (c *chatApp) statusLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			var info runtimeInfo
			_ = json.Unmarshal([]byte(c.node.MeshInfoJSON()), &info)
			c.mu.Lock()
			c.info = info
			c.mu.Unlock()
			c.queueRefresh()
		}
	}
}

func (c *chatApp) handleInput(text string) {
	if strings.HasPrefix(text, "/") {
		c.handleCommand(strings.TrimPrefix(text, "/"))
		return
	}

	c.mu.RLock()
	room := c.currentRoom
	c.mu.RUnlock()
	if room == systemRoom {
		c.systemMessage("Switch to a chat room before sending messages.")
		return
	}

	payload := chatPayload{
		Kind:   "chat",
		Nick:   c.nickname,
		Text:   text,
		SentAt: time.Now().Format("15:04:05"),
	}
	raw, _ := json.Marshal(payload)
	code := c.node.Publish(room, raw)
	switch code {
	case mesh.MOSS_OK:
	case mesh.MOSS_ERR_NO_PEERS:
		c.systemMessage(fmt.Sprintf("Room #%s has no connected peers yet; message stayed local.", room))
	default:
		c.systemMessage(fmt.Sprintf("Publish failed with code %d.", code))
	}
}

func (c *chatApp) handleCommand(raw string) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return
	}
	command := strings.ToLower(parts[0])
	arg := ""
	if len(parts) > 1 {
		arg = strings.TrimSpace(strings.Join(parts[1:], " "))
	}

	switch command {
	case "help":
		c.systemMessage("Commands: /join ROOM, /leave [ROOM], /goto ROOM, /nick NAME, /rooms, /status, /net, /connect HOST:PORT, /quit")
	case "quit", "exit":
		c.ui.Stop()
	case "join":
		if arg == "" {
			c.systemMessage("Usage: /join ROOM")
			return
		}
		room, err := normalizeRoom(arg)
		if err != nil {
			c.systemMessage(err.Error())
			return
		}
		state, ok := c.roomByName[room]
		if !ok || !state.subscribed {
			if code := c.node.Subscribe(room); code != mesh.MOSS_OK {
				c.systemMessage(fmt.Sprintf("Subscribe failed for #%s: %d", room, code))
				return
			}
		}
		c.ensureRoom(room, true)
		c.selectRoom(room)
		c.systemMessage("Joined room #" + room)
	case "goto":
		if arg == "" {
			c.systemMessage("Usage: /goto ROOM")
			return
		}
		room, err := normalizeRoom(arg)
		if err != nil {
			c.systemMessage(err.Error())
			return
		}
		if _, ok := c.roomByName[room]; !ok {
			c.systemMessage("Room #" + room + " is not joined.")
			return
		}
		c.selectRoom(room)
	case "leave":
		target := arg
		if target == "" {
			c.mu.RLock()
			target = c.currentRoom
			c.mu.RUnlock()
		}
		room, err := normalizeRoom(target)
		if err != nil {
			c.systemMessage(err.Error())
			return
		}
		if room == systemRoom {
			c.systemMessage("System room cannot be left.")
			return
		}
		current, ok := c.roomByName[room]
		if !ok || !current.subscribed {
			c.systemMessage("Room #" + room + " is not joined.")
			return
		}
		if code := c.node.Unsubscribe(room); code != mesh.MOSS_OK {
			c.systemMessage(fmt.Sprintf("Unsubscribe failed for #%s: %d", room, code))
			return
		}
		c.mu.Lock()
		current.subscribed = false
		c.mu.Unlock()
		c.systemMessage("Left room #" + room)
		c.selectFallbackRoom(room)
	case "nick":
		if arg == "" {
			c.systemMessage("Usage: /nick NAME")
			return
		}
		c.mu.Lock()
		c.nickname = arg
		c.mu.Unlock()
		c.systemMessage("Nickname changed to " + arg)
		c.queueRefresh()
	case "rooms":
		c.mu.RLock()
		joined := make([]string, 0, len(c.roomOrder))
		for _, room := range c.roomOrder {
			if room == systemRoom {
				continue
			}
			if c.roomByName[room].subscribed {
				joined = append(joined, "#"+room)
			}
		}
		c.mu.RUnlock()
		if len(joined) == 0 {
			c.systemMessage("Joined rooms: (none)")
			return
		}
		c.systemMessage("Joined rooms: " + strings.Join(joined, ", "))
	case "status":
		c.mu.RLock()
		info := c.info
		current := c.currentRoom
		c.mu.RUnlock()
		c.systemMessage(fmt.Sprintf(
			"mesh=%s room=#%s peers=%d nat=%s advertised=%s",
			info.MeshID, current, info.PeerCount, info.NATType, info.AdvertisedAddr,
		))
	case "net", "diag":
		c.showNetworkStatus()
	case "connect":
		if arg == "" {
			c.systemMessage("Usage: /connect HOST:PORT")
			return
		}
		if code := c.node.Connect(arg); code != mesh.MOSS_OK {
			c.systemMessage(fmt.Sprintf("Connect to %s failed: %d", arg, code))
			return
		}
		c.systemMessage("Connecting to " + arg + "...")
	default:
		c.systemMessage("Unknown command: /" + command)
	}
}

func (c *chatApp) showNetworkStatus() {
	c.mu.RLock()
	info := c.info
	c.mu.RUnlock()
	binaryPath, _ := os.Executable()
	c.systemMessage(fmt.Sprintf(
		"net: peers=%d known=%d nat=%s relay=%t advertised=%s",
		info.PeerCount, info.KnownPeerCount, info.NATType, info.SupernodeReady, info.AdvertisedAddr,
	))
	if len(info.Peers) > 0 {
		c.systemMessage("net: connected peers=" + strings.Join(info.Peers, ", "))
	}
	if len(info.KnownPeers) > 0 {
		c.systemMessage("net: known peers=" + strings.Join(info.KnownPeers, ", "))
	}
	c.systemMessage("net: identity=" + c.identityPath)
	c.systemMessage("net: binary=" + filepath.Base(binaryPath))
}

func (c *chatApp) onMessage(channel string, sender [32]byte, data []byte) {
	room, err := normalizeRoom(channel)
	if err != nil {
		return
	}
	c.ensureRoom(room, true)

	line := string(data)
	var payload chatPayload
	if json.Unmarshal(data, &payload) == nil && payload.Text != "" {
		nick := payload.Nick
		if nick == "" {
			nick = formatPeer(hex.EncodeToString(sender[:]))
		}
		if hex.EncodeToString(sender[:]) == c.localPeerID {
			nick = "you"
		}
		sentAt := payload.SentAt
		if sentAt == "" {
			sentAt = time.Now().Format("15:04:05")
		}
		line = fmt.Sprintf("[%s] %s: %s", sentAt, nick, payload.Text)
	}
	c.appendLine(room, line)
}

func (c *chatApp) onEvent(eventType int32, detailJSON string) {
	var detail eventDetail
	_ = json.Unmarshal([]byte(detailJSON), &detail)

	switch eventType {
	case mesh.EventPeerJoined:
		c.systemMessage(fmt.Sprintf("Peer joined: %s @ %s", formatPeer(detail.Peer), detail.Addr))
	case mesh.EventPeerLeft:
		c.systemMessage("Peer left: " + formatPeer(detail.Peer))
	case mesh.EventSupernodePromoted:
		c.systemMessage("Local node became relay-capable (" + detail.NATType + ").")
	case mesh.EventSupernodeRevoked:
		c.systemMessage("Local node is no longer relay-capable (" + detail.NATType + ").")
	case mesh.EventTrackerAnnounce:
		c.systemMessage(fmt.Sprintf(
			"Tracker returned %d candidate peers; connected now: %d.",
			detail.CandidatePeers,
			detail.ConnectedPeers,
		))
	case mesh.EventTrackerFailure:
		c.systemMessage("Tracker error: " + detail.Error)
	case mesh.EventRelayMigrated:
		c.systemMessage(fmt.Sprintf("Relay session %s migrated to direct peer %s.", detail.Session, formatPeer(detail.Peer)))
	default:
		c.systemMessage(fmt.Sprintf("event %d: %s", eventType, detailJSON))
	}
}

func (c *chatApp) appendLine(room, line string) {
	c.mu.Lock()
	state, ok := c.roomByName[room]
	if !ok {
		state = &roomState{name: room, title: "#" + room, subscribed: true}
		c.roomByName[room] = state
		c.roomOrder = append(c.roomOrder, room)
	}
	state.lines = append(state.lines, line)
	if len(state.lines) > 500 {
		state.lines = state.lines[len(state.lines)-500:]
	}
	if c.currentRoom != room {
		state.unread++
	}
	c.mu.Unlock()
	c.queueRefresh()
}

func (c *chatApp) systemMessage(line string) {
	c.appendLine(systemRoom, fmt.Sprintf("[%s] system: %s", time.Now().Format("15:04:05"), line))
}

func (c *chatApp) ensureRoom(room string, subscribed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state, ok := c.roomByName[room]; ok {
		if subscribed {
			state.subscribed = true
		}
		return
	}
	title := "#" + room
	if room == systemRoom {
		title = "System"
	}
	c.roomByName[room] = &roomState{
		name:       room,
		title:      title,
		subscribed: subscribed,
	}
	c.roomOrder = append(c.roomOrder, room)
}

func (c *chatApp) selectRoom(room string) {
	c.mu.Lock()
	if state, ok := c.roomByName[room]; ok {
		c.currentRoom = room
		state.unread = 0
	}
	c.mu.Unlock()
	c.queueRefresh()
}

func (c *chatApp) selectFallbackRoom(leftRoom string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentRoom != leftRoom {
		return
	}
	for _, room := range c.roomOrder {
		if room == leftRoom {
			continue
		}
		if c.roomByName[room].subscribed {
			c.currentRoom = room
			c.roomByName[room].unread = 0
			return
		}
	}
	c.currentRoom = systemRoom
	c.roomByName[systemRoom].unread = 0
}

func (c *chatApp) cycleRoom(step int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	visible := make([]string, 0, len(c.roomOrder))
	for _, room := range c.roomOrder {
		if room == systemRoom || c.roomByName[room].subscribed {
			visible = append(visible, room)
		}
	}
	if len(visible) == 0 {
		return
	}
	index := 0
	for i, room := range visible {
		if room == c.currentRoom {
			index = i
			break
		}
	}
	index = (index + step + len(visible)) % len(visible)
	c.currentRoom = visible[index]
	c.roomByName[c.currentRoom].unread = 0
	c.queueRefresh()
}

func (c *chatApp) subscribedRooms() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rooms := make([]string, 0, len(c.roomOrder))
	for _, room := range c.roomOrder {
		if c.roomByName[room].subscribed {
			rooms = append(rooms, room)
		}
	}
	return rooms
}

func (c *chatApp) infoOrMeshID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.info.MeshID != "" {
		return c.info.MeshID
	}
	return c.meshID
}

func (c *chatApp) queueRefresh() {
	if c.ui == nil {
		return
	}
	c.mu.RLock()
	running := c.running
	c.mu.RUnlock()
	if !running {
		c.refresh()
		return
	}
	c.ui.QueueUpdateDraw(c.refresh)
}

func (c *chatApp) refresh() {
	c.mu.RLock()
	currentRoom := c.currentRoom
	if currentRoom == "" {
		currentRoom = systemRoom
	}
	currentState := c.roomByName[currentRoom]
	roomOrder := append([]string(nil), c.roomOrder...)
	roomByName := make(map[string]roomState, len(c.roomByName))
	for name, state := range c.roomByName {
		roomByName[name] = *state
	}
	info := c.info
	nickname := c.nickname
	c.mu.RUnlock()

	c.mu.Lock()
	c.syncingRooms = true
	c.mu.Unlock()
	c.rooms.Clear()
	visibleRooms := make([]string, 0, len(roomOrder))
	selectedIndex := 0
	for _, room := range roomOrder {
		state := roomByName[room]
		if room != systemRoom && !state.subscribed {
			continue
		}
		title := state.title
		if state.unread > 0 {
			title = fmt.Sprintf("%s (%d)", title, state.unread)
		}
		c.rooms.AddItem(title, "", 0, nil)
		visibleRooms = append(visibleRooms, room)
		if room == currentRoom {
			selectedIndex = len(visibleRooms) - 1
		}
	}
	c.mu.Lock()
	c.visibleRooms = visibleRooms
	c.mu.Unlock()
	if c.rooms.GetItemCount() > 0 {
		c.rooms.SetCurrentItem(selectedIndex)
	}
	c.mu.Lock()
	c.syncingRooms = false
	c.mu.Unlock()

	lines := "No messages yet."
	if currentState != nil && len(currentState.lines) > 0 {
		lines = strings.Join(currentState.lines, "\n")
	}
	c.messages.SetText(lines)
	c.messages.ScrollToEnd()
	c.input.SetLabel(fmt.Sprintf("%s@%s> ", nickname, currentRoom))
	c.status.SetText(fmt.Sprintf(
		"mesh=%s nick=%s room=#%s peers=%d nat=%s relay-ready=%t advertised=%s",
		info.MeshID,
		nickname,
		currentRoom,
		info.PeerCount,
		emptyFallback(info.NATType, "unknown"),
		info.SupernodeReady,
		emptyFallback(info.AdvertisedAddr, net.JoinHostPort("127.0.0.1", strconv.Itoa(info.ListenPort))),
	))
}

func formatPeer(peerID string) string {
	if len(peerID) <= 12 {
		return peerID
	}
	return peerID[:8] + ".." + peerID[len(peerID)-4:]
}

func emptyFallback(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

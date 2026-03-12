package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
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
	debug        bool
	sounds       bool
	identityPath string
	downloadsDir string
	localPeerID  string
	logFile      io.WriteCloser
	logPath      string
}

type roomState struct {
	name       string
	title      string
	subscribed bool
	lines      []chatLine
	unread     int
}

type chatLine struct {
	Text     string
	RegionID string
}

type chatPayload struct {
	Kind         string `json:"kind,omitempty"`
	Nick         string `json:"nick,omitempty"`
	Text         string `json:"text,omitempty"`
	SentAt       string `json:"sent_at,omitempty"`
	Room         string `json:"room,omitempty"`
	Rooms        []string `json:"rooms,omitempty"`
	Target       string `json:"target,omitempty"`
	TransferID   string `json:"transfer_id,omitempty"`
	FileName     string `json:"file_name,omitempty"`
	FileSize     int64  `json:"file_size,omitempty"`
	FileSHA256   string `json:"file_sha256,omitempty"`
	ChunkIndex   int    `json:"chunk_index,omitempty"`
	ChunkCount   int    `json:"chunk_count,omitempty"`
	ChunkData    string `json:"chunk_data,omitempty"`
	CallID       string `json:"call_id,omitempty"`
	CallAction   string `json:"call_action,omitempty"`
	Mention      bool   `json:"mention,omitempty"`
	Attachment   bool   `json:"attachment,omitempty"`
	Notification string `json:"notification,omitempty"`
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

type choicePrompt struct {
	Title   string
	Body    string
	Buttons []string
	OnSelect func(string)
}

type chatApp struct {
	node         *mesh.Node
	meshID       string
	nickname     string
	identityPath string
	bootstrap    []string
	trackers     string
	debug        bool
	sounds       bool
	localPeerID  string
	downloadsDir string
	logFile      io.WriteCloser
	logPath      string

	ui       *tview.Application
	pages    *tview.Pages
	root     *tview.Flex
	header   *tview.TextView
	rooms    *tview.List
	messages *tview.TextView
	input    *tview.InputField
	help     *tview.TextView
	status   *tview.TextView
	members  *tview.TextView

	stopCh chan struct{}
	once   sync.Once
	mu     sync.RWMutex

	running      bool
	syncingRooms bool
	focusIndex   int
	visibleRooms []string
	roomOrder    []string
	roomByName   map[string]*roomState
	peerNames    map[string]string
	presenceSeen map[string]bool
	peerRooms    map[string]map[string]bool
	callState    callState
	incoming     map[string]*incomingAttachment
	outgoing     map[string]*outgoingAttachment
	currentRoom  string
	info         runtimeInfo
	refreshCh    chan struct{}
	activeChoice *choicePrompt
	logMu        sync.Mutex
}

func newChatApp(cfg chatConfig) *chatApp {
	app := &chatApp{
		node:         cfg.node,
		meshID:       cfg.meshID,
		nickname:     cfg.nickname,
		identityPath: cfg.identityPath,
		bootstrap:    append([]string(nil), cfg.bootstrap...),
		trackers:     cfg.trackers,
		debug:        cfg.debug,
		sounds:       cfg.sounds,
		localPeerID:  cfg.localPeerID,
		downloadsDir: cfg.downloadsDir,
		logFile:      cfg.logFile,
		logPath:      cfg.logPath,
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
		peerNames:   make(map[string]string),
		presenceSeen: make(map[string]bool),
		peerRooms:   make(map[string]map[string]bool),
		incoming:    make(map[string]*incomingAttachment),
		outgoing:    make(map[string]*outgoingAttachment),
		refreshCh:   make(chan struct{}, 1),
	}
	app.peerNames[cfg.localPeerID] = cfg.nickname
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
	c.tracef("startup: mesh=%s nick=%s rooms=%v peers=%v trackers=%s downloads=%s identity=%s", c.meshID, c.nickname, c.subscribedRooms(), c.bootstrap, c.trackers, c.downloadsDir, c.identityPath)
	c.node.SetMessageCallback(c.onMessage)
	c.node.SetEventCallback(c.onEvent)
	if code := c.node.Start(); code != mesh.MOSS_OK {
		return fmt.Errorf("node start failed: %d", code)
	}
	if code := c.node.Subscribe(controlRoom); code != mesh.MOSS_OK {
		_ = c.node.Stop()
		return fmt.Errorf("subscribe %s failed: %d", controlRoom, code)
	}
	for _, room := range c.subscribedRooms() {
		if room == systemRoom || room == controlRoom {
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
	c.systemMessage("Trace log: " + c.logPath)
	go c.broadcastPresence()
	go c.statusLoop()
	defer c.shutdown()
	c.ui.SetRoot(c.pages, true)
	c.setFocus(2)
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()
	go c.refreshLoop()
	c.refresh()
	return c.ui.Run()
}

func (c *chatApp) shutdown() {
	c.once.Do(func() {
		c.tracef("shutdown")
		close(c.stopCh)
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
		_ = c.node.Stop()
		if c.logFile != nil {
			_ = c.logFile.Close()
		}
	})
}

func (c *chatApp) refreshLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		case <-c.refreshCh:
			if c.ui == nil {
				continue
			}
			c.ui.QueueUpdateDraw(c.refresh)
		}
	}
}

func (c *chatApp) buildUI() {
	c.ui = tview.NewApplication().EnableMouse(true)

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
	c.rooms.SetSelectedFunc(func(index int, _, _ string, _ rune) {
		c.mu.Lock()
		if index >= 0 && index < len(c.visibleRooms) {
			c.currentRoom = c.visibleRooms[index]
			if room := c.roomByName[c.currentRoom]; room != nil {
				room.unread = 0
			}
		}
		c.mu.Unlock()
		c.setFocus(2)
		c.refresh()
	})

	c.messages = tview.NewTextView().
		SetDynamicColors(false).
		SetRegions(true).
		SetScrollable(true).
		SetWrap(true)
	c.messages.SetHighlightedFunc(func(added, removed, remaining []string) {
		if len(added) == 0 {
			return
		}
		c.handleMessageRegion(added[0])
	})
	c.messages.SetBorder(true).SetTitle(" Messages ")
	c.messages.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEnter:
			highlights := c.messages.GetHighlights()
			if len(highlights) == 0 {
				return event
			}
			c.handleMessageRegion(highlights[0])
			return nil
		case tcell.KeyPgUp:
			row, col := c.messages.GetScrollOffset()
			if row >= 10 {
				row -= 10
			} else {
				row = 0
			}
			c.messages.ScrollTo(row, col)
			return nil
		case tcell.KeyPgDn:
			row, col := c.messages.GetScrollOffset()
			c.messages.ScrollTo(row+10, col)
			return nil
		default:
			return event
		}
	})

	c.members = tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(false)
	c.members.SetBorder(true).SetTitle(" Channel ")

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
	c.help.SetText("F1 help | F4 room | F5 DM | F6 nick | F7 attach | F8 connect | F9 debug | F10 call | Enter send")
	c.status = tview.NewTextView().SetDynamicColors(true)

	body := tview.NewFlex().
		AddItem(c.rooms, 26, 1, false).
		AddItem(c.messages, 0, 4, false).
		AddItem(c.members, 24, 1, false)
	footer := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(c.input, 3, 0, true).
		AddItem(c.help, 1, 0, false).
		AddItem(c.status, 1, 0, false)

	c.root = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(c.header, 1, 0, false).
		AddItem(body, 0, 1, false).
		AddItem(footer, 5, 0, true)

	c.pages = tview.NewPages().AddPage("main", c.root, true, true)

	c.ui.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if c.handleChoicePromptInput(event) {
			return nil
		}
		switch event.Key() {
		case tcell.KeyF1:
			c.showHelpModal()
			return nil
		case tcell.KeyF2:
			c.cycleRoom(-1)
			return nil
		case tcell.KeyF3:
			c.cycleRoom(1)
			return nil
		case tcell.KeyF4:
			c.showJoinRoomModal()
			return nil
		case tcell.KeyF5:
			c.showDirectMessageModal()
			return nil
		case tcell.KeyF6:
			c.showNickModal()
			return nil
		case tcell.KeyF7:
			c.showAttachModal()
			return nil
		case tcell.KeyF8:
			c.showConnectModal()
			return nil
		case tcell.KeyF9:
			c.toggleDebug()
			return nil
		case tcell.KeyF10:
			c.showCallModal()
			return nil
		case tcell.KeyTAB:
			c.cycleFocus(1)
			return nil
		case tcell.KeyBacktab:
			c.cycleFocus(-1)
			return nil
		case tcell.KeyCtrlC:
			c.ui.Stop()
			return nil
		case tcell.KeyCtrlL:
			c.setFocus(2)
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

func (c *chatApp) focusables() []tview.Primitive {
	return []tview.Primitive{c.rooms, c.messages, c.input}
}

func (c *chatApp) setFocus(index int) {
	items := c.focusables()
	if len(items) == 0 {
		return
	}
	if index < 0 {
		index = len(items) - 1
	}
	index %= len(items)
	c.mu.Lock()
	c.focusIndex = index
	c.mu.Unlock()
	c.ui.SetFocus(items[index])
}

func (c *chatApp) cycleFocus(step int) {
	c.mu.Lock()
	next := c.focusIndex + step
	c.mu.Unlock()
	c.setFocus(next)
}

func (c *chatApp) showHelpModal() {
	body := strings.Join([]string{
		"F2/F3: switch rooms",
		"F4: create or join room",
		"F5: open direct chat",
		"F6: rename yourself",
		"F7: send attachment",
		"F8: connect to HOST:PORT",
		"F9: toggle debug events",
		"F10: place a call",
		"Click attachment lines (or focus them and press Enter) to confirm downloads",
		"Tab / Shift-Tab: switch focus",
		"Ctrl-L: jump to composer",
		"/join ROOM, /leave, /goto ROOM, /msg TARGET [TEXT], /attach PATH, /call TARGET, /answer, /decline, /hangup, /peers, /net, /status",
	}, "\n")
	c.showTextModal("Shortcuts", body)
}

func (c *chatApp) showTextModal(title, body string) {
	modal := tview.NewModal().
		SetText(body).
		AddButtons([]string{"Close"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			c.closeModal("overlay")
		})
	modal.SetTitle(" " + title + " ").SetBorder(true)
	c.showModal("overlay", modal, modal)
}

func (c *chatApp) showAlert(title, body string) {
	c.tracef("alert: title=%q body=%q", title, body)
	modal := tview.NewModal().
		SetText(body).
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			c.closeModal("alert")
		})
	modal.SetTitle(" " + title + " ").SetBorder(true)
	c.showModal("alert", modal, modal)
}

func (c *chatApp) showJoinRoomModal() {
	c.showInputModal("Join Room", "Room", "", func(value string) {
		c.handleCommand("join " + value)
	})
}

func (c *chatApp) showNickModal() {
	c.mu.RLock()
	current := c.nickname
	c.mu.RUnlock()
	c.showInputModal("Change Nickname", "Nickname", current, func(value string) {
		c.handleCommand("nick " + value)
	})
}

func (c *chatApp) showConnectModal() {
	c.showInputModal("Connect Peer", "HOST:PORT", "", func(value string) {
		c.handleCommand("connect " + value)
	})
}

func (c *chatApp) showAttachModal() {
	c.mu.RLock()
	room := c.currentRoom
	c.mu.RUnlock()
	go func() {
		if err := c.sendAttachment(room, ""); err != nil {
			c.showAlert("Attachment", err.Error())
		}
	}()
}

func (c *chatApp) showDirectMessageModal() {
	c.showInputModal("Open Direct Chat", "Nickname or peer id", "", func(value string) {
		if value == "" {
			return
		}
		if _, err := c.openDirectRoom(value); err != nil {
			c.systemMessage(err.Error())
		}
	})
}

func (c *chatApp) showCallModal() {
	c.showInputModal("Call Peer", "Nickname or peer id", "", func(value string) {
		c.handleCommand("call " + value)
	})
}

func (c *chatApp) toggleDebug() {
	c.mu.Lock()
	c.debug = !c.debug
	enabled := c.debug
	c.mu.Unlock()
	if enabled {
		c.systemMessage("Debug events enabled.")
		return
	}
	c.systemMessage("Debug events disabled.")
}

func (c *chatApp) showInputModal(title, label, initial string, submit func(string)) {
	input := tview.NewInputField().
		SetLabel(label + ": ").
		SetText(initial)
	form := tview.NewForm().
		AddFormItem(input).
		AddButton("OK", func() {
			value := strings.TrimSpace(input.GetText())
			c.closeModal("overlay")
			if value != "" {
				submit(value)
			}
		}).
		AddButton("Cancel", func() {
			c.closeModal("overlay")
		})
	form.SetBorder(true).SetTitle(" " + title + " ")
	form.SetButtonsAlign(tview.AlignRight)
	input.SetDoneFunc(func(key tcell.Key) {
		switch key {
		case tcell.KeyEnter:
			value := strings.TrimSpace(input.GetText())
			c.closeModal("overlay")
			if value != "" {
				submit(value)
			}
		case tcell.KeyEscape:
			c.closeModal("overlay")
		}
	})
	modal := centered(62, 9, form)
	c.showModal("overlay", modal, input)
}

func (c *chatApp) showChoiceModal(title, body string, buttons []string, onSelect func(label string)) {
	c.tracef("choice-prompt: show title=%q buttons=%v body=%q", title, buttons, body)
	c.queueUI(func() {
		c.activeChoice = &choicePrompt{
			Title:    title,
			Body:     body,
			Buttons:  append([]string(nil), buttons...),
			OnSelect: onSelect,
		}
		c.pages.RemovePage("choice")
		c.pages.AddPage("choice", c.renderChoicePrompt(), true, true)
	})
}

func (c *chatApp) renderChoicePrompt() tview.Primitive {
	c.mu.RLock()
	prompt := c.activeChoice
	c.mu.RUnlock()
	if prompt == nil {
		return centered(68, 11, tview.NewTextView().SetText(""))
	}
	lines := []string{
		prompt.Body,
		"",
		"[Enter/Y] " + prompt.Buttons[0],
	}
	if len(prompt.Buttons) > 1 {
		lines = append(lines, "[N/Esc] "+prompt.Buttons[1])
	}
	text := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true).
		SetText(strings.Join(lines, "\n"))
	text.SetBorder(true).SetTitle(" " + prompt.Title + " ")
	return centered(68, 11, text)
}

func (c *chatApp) handleChoicePromptInput(event *tcell.EventKey) bool {
	c.mu.RLock()
	prompt := c.activeChoice
	c.mu.RUnlock()
	if prompt == nil {
		return false
	}
	switch event.Key() {
	case tcell.KeyEnter:
		if len(prompt.Buttons) > 0 {
			c.tracef("choice-prompt: enter -> %q", prompt.Buttons[0])
			c.resolveChoicePrompt(prompt.Buttons[0])
		} else {
			c.tracef("choice-prompt: enter -> dismiss")
			c.dismissChoicePrompt()
		}
		return true
	case tcell.KeyEscape:
		if len(prompt.Buttons) > 1 {
			c.tracef("choice-prompt: escape -> %q", prompt.Buttons[1])
			c.resolveChoicePrompt(prompt.Buttons[1])
		} else {
			c.tracef("choice-prompt: escape -> dismiss")
			c.dismissChoicePrompt()
		}
		return true
	case tcell.KeyRune:
		switch strings.ToLower(string(event.Rune())) {
		case "y":
			if len(prompt.Buttons) > 0 {
				c.tracef("choice-prompt: y -> %q", prompt.Buttons[0])
				c.resolveChoicePrompt(prompt.Buttons[0])
				return true
			}
		case "n":
			if len(prompt.Buttons) > 1 {
				c.tracef("choice-prompt: n -> %q", prompt.Buttons[1])
				c.resolveChoicePrompt(prompt.Buttons[1])
				return true
			}
			c.tracef("choice-prompt: n -> dismiss")
			c.dismissChoicePrompt()
			return true
		}
	}
	return true
}

func (c *chatApp) resolveChoicePrompt(label string) {
	c.mu.Lock()
	var onSelect func(string)
	if c.activeChoice != nil {
		onSelect = c.activeChoice.OnSelect
	}
	c.activeChoice = nil
	c.mu.Unlock()
	c.tracef("choice-prompt: resolve %q", label)
	c.queueUI(func() {
		c.pages.RemovePage("choice")
		c.setFocus(2)
	})
	if onSelect != nil {
		onSelect(label)
	}
}

func (c *chatApp) dismissChoicePrompt() {
	c.mu.Lock()
	c.activeChoice = nil
	c.mu.Unlock()
	c.tracef("choice-prompt: dismiss")
	c.queueUI(func() {
		c.pages.RemovePage("choice")
		c.setFocus(2)
	})
}

func centered(width, height int, primitive tview.Primitive) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(
			tview.NewFlex().SetDirection(tview.FlexRow).
				AddItem(nil, 0, 1, false).
				AddItem(primitive, height, 1, true).
				AddItem(nil, 0, 1, false),
			width,
			1,
			true,
		).
		AddItem(nil, 0, 1, false)
}

func (c *chatApp) showModal(name string, primitive tview.Primitive, focus tview.Primitive) {
	c.queueUI(func() {
		c.pages.RemovePage(name)
		c.pages.AddPage(name, primitive, true, true)
		if focus != nil {
			c.ui.SetFocus(focus)
			return
		}
		c.ui.SetFocus(primitive)
	})
}

func (c *chatApp) closeModal(name string) {
	c.queueUI(func() {
		c.pages.RemovePage(name)
		c.setFocus(2)
	})
}

func (c *chatApp) handleInput(text string) {
	if strings.HasPrefix(text, "/") {
		c.handleCommand(strings.TrimPrefix(text, "/"))
		return
	}

	c.mu.RLock()
	room := c.currentRoom
	c.mu.RUnlock()
	c.sendCurrentRoomMessage(room, text)
}

func (c *chatApp) sendCurrentRoomMessage(room, text string) {
	if room == systemRoom {
		c.systemMessage("Switch to a chat room before sending messages.")
		return
	}
	if room == controlRoom {
		c.systemMessage("Control room is internal and cannot be used directly.")
		return
	}

	payload := chatPayload{
		Kind:    "chat",
		Nick:    c.nickname,
		Text:    text,
		SentAt:  time.Now().Format("15:04:05"),
		Mention: c.containsMention(text),
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
		c.systemMessage("Commands: /join ROOM, /leave [ROOM], /goto ROOM, /nick NAME, /msg TARGET [TEXT], /attach PATH, /call TARGET, /answer, /decline, /hangup, /debug, /peers, /rooms, /status, /net, /connect HOST:PORT, /quit")
	case "log", "logs":
		c.systemMessage("Trace log: " + c.logPath)
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
		go c.broadcastPresence()
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
		go c.broadcastPresence()
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
		go c.broadcastPresence()
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
	case "msg":
		if arg == "" {
			c.systemMessage("Usage: /msg TARGET [TEXT]")
			return
		}
		target, text := splitTargetAndText(arg)
		if target == "" {
			c.systemMessage("Usage: /msg TARGET [TEXT]")
			return
		}
		room, err := c.openDirectRoom(target)
		if err != nil {
			c.systemMessage(err.Error())
			return
		}
		if text != "" {
			c.sendCurrentRoomMessage(room, text)
		}
	case "peers":
		c.showPeerSummary()
	case "attach":
		if arg == "" {
			c.systemMessage("Usage: /attach PATH")
			return
		}
		c.mu.RLock()
		room := c.currentRoom
		c.mu.RUnlock()
		if err := c.sendAttachment(room, arg); err != nil {
			c.showAlert("Attachment", err.Error())
		}
	case "call":
		if arg == "" {
			c.systemMessage("Usage: /call TARGET")
			return
		}
		if err := c.startCall(arg); err != nil {
			c.showAlert("Call", err.Error())
		}
	case "answer":
		if err := c.answerCall(); err != nil {
			c.showAlert("Call", err.Error())
		}
	case "decline":
		if err := c.declineCall(); err != nil {
			c.showAlert("Call", err.Error())
		}
	case "hangup":
		if err := c.hangupCall(); err != nil {
			c.showAlert("Call", err.Error())
		}
	case "debug":
		c.toggleDebug()
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
			c.showAlert("Connect", "Usage: /connect HOST:PORT")
			return
		}
		if code := c.node.Connect(arg); code != mesh.MOSS_OK {
			c.showAlert("Connect", fmt.Sprintf("Connect to %s failed: %d", arg, code))
			return
		}
		c.systemMessage("Connecting to " + arg + "...")
	default:
		c.systemMessage("Unknown command: /" + command)
	}
}

func splitTargetAndText(raw string) (string, string) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return "", ""
	}
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], strings.TrimSpace(strings.TrimPrefix(raw, parts[0]))
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

func (c *chatApp) showPeerSummary() {
	c.mu.RLock()
	if len(c.peerNames) == 0 {
		c.mu.RUnlock()
		c.systemMessage("Peers: no known nicknames yet.")
		return
	}
	parts := make([]string, 0, len(c.peerNames))
	for peerID, name := range c.peerNames {
		if peerID == c.localPeerID {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", name, formatPeer(peerID)))
	}
	c.mu.RUnlock()
	if len(parts) == 0 {
		c.systemMessage("Peers: no known nicknames yet.")
		return
	}
	c.systemMessage("Peers: " + strings.Join(parts, ", "))
}

func (c *chatApp) onMessage(channel string, sender [32]byte, data []byte) {
	senderID := hex.EncodeToString(sender[:])
	c.tracef("message: channel=%s sender=%s size=%d", channel, formatPeer(senderID), len(data))
	var payload chatPayload
	if json.Unmarshal(data, &payload) == nil && payload.Attachment {
		targetRoom := channel
		if payload.Room != "" {
			targetRoom = payload.Room
		}
		c.handleAttachmentPayload(targetRoom, senderID, payload)
		return
	}
	if channel == controlRoom {
		c.handleControlMessage(senderID, data)
		return
	}
	room, err := normalizeRoom(channel)
	if err != nil {
		return
	}
	c.ensureRoom(room, true)

	line := string(data)
	if json.Unmarshal(data, &payload) == nil {
		if payload.Text != "" {
			c.rememberPeerName(senderID, payload.Nick)
			nick := payload.Nick
			if nick == "" {
				nick = c.displayNameForPeer(senderID)
			}
			if senderID == c.localPeerID {
				nick = "you"
			}
			sentAt := payload.SentAt
			if sentAt == "" {
				sentAt = time.Now().Format("15:04:05")
			}
			line = fmt.Sprintf("[%s] %s: %s", sentAt, nick, payload.Text)
			if senderID != c.localPeerID {
				c.notifyIncomingMessage(room, nick, payload.Text)
			}
		}
	}
	c.appendLine(room, line)
}

func (c *chatApp) handleControlMessage(senderID string, data []byte) {
	var payload chatPayload
	if json.Unmarshal(data, &payload) != nil {
		c.tracef("control: invalid-json sender=%s size=%d", formatPeer(senderID), len(data))
		return
	}
	c.tracef("control: kind=%s sender=%s target=%s room=%s transfer=%s call=%s", payload.Kind, formatPeer(senderID), formatPeer(payload.Target), payload.Room, payload.TransferID, payload.CallID)
	c.rememberPeerName(senderID, payload.Nick)
	if payload.Target != "" && payload.Target != c.localPeerID {
		return
	}
	switch payload.Kind {
	case "presence":
		name := emptyFallback(payload.Nick, c.displayNameForPeer(senderID))
		c.rememberPeerName(senderID, payload.Nick)
		c.setPeerRooms(senderID, payload.Rooms)
		c.mu.Lock()
		firstSeen := !c.presenceSeen[senderID]
		c.presenceSeen[senderID] = true
		c.mu.Unlock()
		if firstSeen {
			c.lobbyMessage(name + " joined the chat.")
		}
	case "attachment-request":
		c.promptAttachmentRequest(senderID, payload.TransferID)
	case "attachment-start", "attachment-chunk", "attachment-denied":
		targetRoom := payload.Room
		if targetRoom == "" {
			targetRoom = defaultRoom
		}
		c.handleAttachmentPayload(targetRoom, senderID, payload)
	case "dm_invite":
		room, err := normalizeRoom(payload.Room)
		if err != nil {
			return
		}
		if !c.ensureSubscribedRoom(room) {
			return
		}
		c.setRoomTitle(room, "@"+emptyFallback(payload.Nick, c.displayNameForPeer(senderID)))
		c.systemMessage("Direct chat ready with " + c.displayNameForPeer(senderID))
	case "call_invite", "call_accept", "call_decline", "call_hangup":
		c.handleCallPayload(senderID, payload)
	}
}

func (c *chatApp) onEvent(eventType int32, detailJSON string) {
	var detail eventDetail
	_ = json.Unmarshal([]byte(detailJSON), &detail)

	switch eventType {
	case mesh.EventPeerJoined:
		c.tracef("event: peer-joined peer=%s addr=%s", formatPeer(detail.Peer), detail.Addr)
		c.rememberPeerName(detail.Peer, "")
		go func() {
			time.Sleep(200 * time.Millisecond)
			c.broadcastPresence()
		}()
	case mesh.EventPeerLeft:
		c.tracef("event: peer-left peer=%s addr=%s", formatPeer(detail.Peer), detail.Addr)
		name := c.displayNameForPeer(detail.Peer)
		c.mu.Lock()
		delete(c.presenceSeen, detail.Peer)
		delete(c.peerRooms, detail.Peer)
		c.mu.Unlock()
		c.lobbyMessage(name + " left the chat.")
	case mesh.EventSupernodePromoted:
		c.systemMessage("Local node became relay-capable (" + detail.NATType + ").")
	case mesh.EventSupernodeRevoked:
		c.systemMessage("Local node is no longer relay-capable (" + detail.NATType + ").")
	case mesh.EventTrackerAnnounce:
		c.tracef("event: tracker-announce candidates=%d connected=%d", detail.CandidatePeers, detail.ConnectedPeers)
		c.mu.RLock()
		debug := c.debug
		c.mu.RUnlock()
		if !debug {
			return
		}
		c.systemMessage(fmt.Sprintf(
			"Tracker returned %d candidate peers; connected now: %d.",
			detail.CandidatePeers,
			detail.ConnectedPeers,
		))
	case mesh.EventTrackerFailure:
		c.tracef("event: tracker-failure error=%s", detail.Error)
		c.systemMessage("Tracker error: " + detail.Error)
	case mesh.EventRelayMigrated:
		c.systemMessage(fmt.Sprintf("Relay session %s migrated to direct peer %s.", detail.Session, formatPeer(detail.Peer)))
	default:
		c.tracef("event: type=%d detail=%s", eventType, detailJSON)
		c.systemMessage(fmt.Sprintf("event %d: %s", eventType, detailJSON))
	}
}

func (c *chatApp) appendLine(room, line string) {
	c.appendRoomLine(room, chatLine{Text: line})
}

func (c *chatApp) appendActionLine(room, regionID, line string) {
	c.appendRoomLine(room, chatLine{Text: line, RegionID: regionID})
}

func (c *chatApp) appendRoomLine(room string, line chatLine) {
	c.mu.Lock()
	state, ok := c.roomByName[room]
	if !ok {
		state = &roomState{name: room, title: "#" + room, subscribed: true}
		c.roomByName[room] = state
		c.roomOrder = append(c.roomOrder, room)
	}
	state.lines = append(state.lines, line)
	if len(state.lines) > maxRoomLines {
		state.lines = state.lines[len(state.lines)-maxRoomLines:]
	}
	if c.currentRoom != room {
		state.unread++
	}
	c.mu.Unlock()
	c.queueRefresh()
}

func (c *chatApp) updateRoomLine(room, regionID, text string) {
	c.mu.Lock()
	c.updateRoomLineLocked(room, regionID, text)
	c.mu.Unlock()
	c.queueRefresh()
}

func (c *chatApp) updateRoomLineLocked(room, regionID, text string) {
	state, ok := c.roomByName[room]
	if !ok {
		return
	}
	for index := range state.lines {
		if state.lines[index].RegionID == regionID {
			state.lines[index].Text = text
			return
		}
	}
}

func (c *chatApp) systemMessage(line string) {
	c.tracef("system: %s", line)
	c.appendLine(systemRoom, fmt.Sprintf("[%s] system: %s", time.Now().Format("15:04:05"), line))
}

func (c *chatApp) lobbyMessage(line string) {
	c.ensureRoom(defaultRoom, true)
	formatted := fmt.Sprintf("[%s] system: %s", time.Now().Format("15:04:05"), line)
	c.appendLine(defaultRoom, formatted)
}

func (c *chatApp) rememberPeerName(peerID, name string) {
	if peerID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.TrimSpace(name) == "" {
		if _, ok := c.peerNames[peerID]; !ok {
			c.peerNames[peerID] = formatPeer(peerID)
		}
		return
	}
	c.peerNames[peerID] = name
}

func (c *chatApp) displayNameForPeer(peerID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if name := strings.TrimSpace(c.peerNames[peerID]); name != "" {
		return name
	}
	return formatPeer(peerID)
}

func (c *chatApp) setPeerRooms(peerID string, rooms []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if peerID == "" {
		return
	}
	set := make(map[string]bool, len(rooms))
	for _, room := range rooms {
		if normalized, err := normalizeRoom(room); err == nil {
			set[normalized] = true
		}
	}
	c.peerRooms[peerID] = set
}

func (c *chatApp) broadcastPresence() {
	c.tracef("presence: publish rooms=%v", c.subscribedRooms())
	payload, _ := json.Marshal(chatPayload{
		Kind:   "presence",
		Nick:   c.nickname,
		SentAt: time.Now().Format("15:04:05"),
		Rooms:  c.subscribedRooms(),
	})
	code := c.node.Publish(controlRoom, payload)
	if code == mesh.MOSS_OK || code == mesh.MOSS_ERR_NO_PEERS {
		return
	}
	c.systemMessage(fmt.Sprintf("Presence publish failed with code %d.", code))
}

func (c *chatApp) tracef(format string, args ...any) {
	if c.logFile == nil {
		return
	}
	line := fmt.Sprintf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05.000"), fmt.Sprintf(format, args...))
	c.logMu.Lock()
	defer c.logMu.Unlock()
	_, _ = io.WriteString(c.logFile, line)
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

func (c *chatApp) setRoomTitle(room, title string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state, ok := c.roomByName[room]; ok && strings.TrimSpace(title) != "" {
		state.title = title
	}
}

func (c *chatApp) ensureSubscribedRoom(room string) bool {
	c.mu.RLock()
	state, ok := c.roomByName[room]
	alreadySubscribed := ok && state.subscribed
	c.mu.RUnlock()
	if alreadySubscribed {
		c.ensureRoom(room, true)
		return true
	}
	if code := c.node.Subscribe(room); code != mesh.MOSS_OK {
		c.systemMessage(fmt.Sprintf("Subscribe failed for #%s: %d", room, code))
		return false
	}
	c.ensureRoom(room, true)
	return true
}

func (c *chatApp) openDirectRoom(target string) (string, error) {
	peerID, label, err := c.resolvePeerTarget(target)
	if err != nil {
		return "", err
	}
	room := directRoomName(c.localPeerID, peerID)
	if !c.ensureSubscribedRoom(room) {
		return "", fmt.Errorf("failed to join direct room with %s", label)
	}
	c.setRoomTitle(room, "@"+label)
	c.selectRoom(room)
	payload, _ := json.Marshal(chatPayload{
		Kind:   "dm_invite",
		Nick:   c.nickname,
		Room:   room,
		Target: peerID,
		SentAt: time.Now().Format("15:04:05"),
	})
	code := c.node.Publish(controlRoom, payload)
	if code != mesh.MOSS_OK && code != mesh.MOSS_ERR_NO_PEERS {
		return "", fmt.Errorf("direct chat invite failed: %d", code)
	}
	c.systemMessage("Direct chat opened with " + label)
	return room, nil
}

func (c *chatApp) resolvePeerTarget(target string) (string, string, error) {
	needle := strings.ToLower(strings.TrimSpace(target))
	if needle == "" {
		return "", "", fmt.Errorf("target is required")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for peerID, name := range c.peerNames {
		if peerID == c.localPeerID {
			continue
		}
		if strings.EqualFold(name, target) || strings.HasPrefix(strings.ToLower(peerID), needle) || strings.HasPrefix(strings.ToLower(formatPeer(peerID)), needle) {
			return peerID, name, nil
		}
	}
	return "", "", fmt.Errorf("peer %q not found; use /peers first", target)
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
	select {
	case c.refreshCh <- struct{}{}:
	default:
	}
}

func (c *chatApp) queueUI(action func()) {
	if c.ui == nil {
		action()
		return
	}
	c.mu.RLock()
	running := c.running
	c.mu.RUnlock()
	if !running {
		action()
		return
	}
	go c.ui.QueueUpdateDraw(action)
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
	debug := c.debug
	callLabel := c.callState.summary()
	c.mu.RUnlock()

	c.mu.Lock()
	c.syncingRooms = true
	c.mu.Unlock()
	c.rooms.Clear()
	visibleRooms := make([]string, 0, len(roomOrder))
	selectedIndex := 0
	for _, room := range roomOrder {
		if room == controlRoom {
			continue
		}
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
		lines = renderRoomLines(currentState.lines)
	}
	c.messages.SetText(lines)
	c.messages.ScrollToEnd()
	c.members.SetText(strings.Join(c.currentRoomMembers(currentRoom), "\n"))
	c.input.SetLabel(fmt.Sprintf("%s@%s> ", nickname, currentRoom))
	c.status.SetText(fmt.Sprintf(
		"mesh=%s nick=%s room=#%s peers=%d nat=%s relay-ready=%t debug=%t call=%s advertised=%s",
		info.MeshID,
		nickname,
		currentRoom,
		info.PeerCount,
		emptyFallback(info.NATType, "unknown"),
		info.SupernodeReady,
		debug,
		callLabel,
		emptyFallback(info.AdvertisedAddr, net.JoinHostPort("127.0.0.1", strconv.Itoa(info.ListenPort))),
	))
}

func (c *chatApp) currentRoomMembers(room string) []string {
	if room == "" {
		return []string{"(no room)"}
	}
	members := make([]string, 0, 8)
	if room == systemRoom {
		members = append(members, "System")
		c.mu.RLock()
		for peerID, name := range c.peerNames {
			if peerID == c.localPeerID {
				continue
			}
			members = append(members, name)
		}
		c.mu.RUnlock()
		sort.Strings(members[1:])
		return members
	}
	if containsString(c.subscribedRooms(), room) {
		members = append(members, c.nickname+" (you)")
	}
	c.mu.RLock()
	peerNames := make(map[string]string, len(c.peerNames))
	for peerID, name := range c.peerNames {
		peerNames[peerID] = name
	}
	peerRooms := make(map[string]map[string]bool, len(c.peerRooms))
	for peerID, rooms := range c.peerRooms {
		copied := make(map[string]bool, len(rooms))
		for subscribedRoom, ok := range rooms {
			copied[subscribedRoom] = ok
		}
		peerRooms[peerID] = copied
	}
	c.mu.RUnlock()
	for peerID, rooms := range peerRooms {
		if peerID == c.localPeerID || !rooms[room] {
			continue
		}
		members = append(members, emptyFallback(peerNames[peerID], formatPeer(peerID)))
	}
	if len(members) == 0 {
		return []string{"No members"}
	}
	sort.Strings(members[1:])
	return members
}

func (c *chatApp) handleMessageRegion(regionID string) {
	if strings.HasPrefix(regionID, "in-") {
		c.showIncomingAttachmentModal(strings.TrimPrefix(regionID, "in-"))
	}
}

func renderRoomLines(lines []chatLine) string {
	if len(lines) == 0 {
		return "No messages yet."
	}
	var builder strings.Builder
	for index, line := range lines {
		text := tview.Escape(line.Text)
		if line.RegionID != "" {
			builder.WriteString(`["`)
			builder.WriteString(line.RegionID)
			builder.WriteString(`"]`)
			builder.WriteString(text)
			builder.WriteString(`[""]`)
		} else {
			builder.WriteString(text)
		}
		if index != len(lines)-1 {
			builder.WriteByte('\n')
		}
	}
	return builder.String()
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

func directRoomName(localPeerID, remotePeerID string) string {
	a := strings.ToLower(strings.TrimSpace(localPeerID))
	b := strings.ToLower(strings.TrimSpace(remotePeerID))
	if a == "" || b == "" {
		return "dm"
	}
	if a > b {
		a, b = b, a
	}
	a = a[:min(16, len(a))]
	b = b[:min(16, len(b))]
	return "dm-" + a + "-" + b
}

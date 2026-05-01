from __future__ import annotations

import json
import threading

from moss_chat_client import MossClient, RoomState
from moss_chat_format import (
    current_timestamp,
    detect_share_host,
    format_peer,
    normalize_room,
    parse_nickname,
    render_chat_line,
)
from moss_chat_native import DEFAULT_ROOM, EVENT_NAMES, MOSS_ERR_NO_PEERS, SYSTEM_ROOM

class MossChatApp:
    def __init__(
        self,
        *,
        client: MossClient,
        nickname: str,
        initial_rooms: list[str],
    ) -> None:
        self.client = client
        self.nickname = parse_nickname(nickname)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._mesh_info: dict = {}
        self.share_host = detect_share_host()
        self.rooms: dict[str, RoomState] = {
            SYSTEM_ROOM: RoomState(SYSTEM_ROOM, False, "System"),
        }
        self.room_order = [SYSTEM_ROOM]
        for room in initial_rooms:
            self._ensure_room(room, subscribed=True)
        self.current_room = initial_rooms[0] if initial_rooms else DEFAULT_ROOM

        self.client.set_message_handler(self._on_message)
        self.client.set_event_handler(self._on_event)
        self.application = None
        self.rooms_view = None
        self.messages_view = None
        self.status_bar = None
        self.help_bar = None
        self.composer = None

    def _build_application(self) -> None:
        if self.application is not None:
            return

        from prompt_toolkit import Application
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.layout import HSplit, Layout, VSplit
        from prompt_toolkit.styles import Style
        from prompt_toolkit.widgets import Frame, Label, TextArea

        self.rooms_view = TextArea(
            text="",
            focusable=False,
            read_only=True,
            scrollbar=True,
            width=24,
        )
        self.messages_view = TextArea(
            text="",
            focusable=False,
            read_only=True,
            scrollbar=True,
            wrap_lines=True,
        )
        self.status_bar = Label(text="")
        self.help_bar = Label(
            text="Enter send | /join room | /leave | /nick NAME | F2/F3 switch rooms | Ctrl-C quit"
        )
        self.composer = TextArea(
            height=1,
            prompt="> ",
            multiline=False,
            wrap_lines=False,
        )
        self.composer.buffer.accept_handler = self._accept_input

        kb = KeyBindings()

        @kb.add("f2")
        def _prev_room(event) -> None:
            self._cycle_room(-1)

        @kb.add("f3")
        def _next_room(event) -> None:
            self._cycle_room(1)

        @kb.add("c-c")
        def _quit(event) -> None:
            event.app.exit()

        root = HSplit(
            [
                Label(text="Moss Chat Demo", style="class:header"),
                VSplit(
                    [
                        Frame(self.rooms_view, title="Rooms"),
                        Frame(self.messages_view, title="Messages"),
                    ]
                ),
                Frame(self.composer, title="Composer"),
                self.help_bar,
                self.status_bar,
            ]
        )
        self.application = Application(
            layout=Layout(root, focused_element=self.composer),
            key_bindings=kb,
            full_screen=True,
            mouse_support=False,
            style=Style.from_dict(
                {
                    "header": "bg:#263238 #ffffff bold",
                    "frame.label": "bg:#4f6d7a #ffffff bold",
                }
            ),
        )

    def run(self) -> None:
        self._build_application()
        self.client.start()
        for room_name in list(self.room_order):
            if room_name != SYSTEM_ROOM:
                self.client.subscribe(room_name)
        self._system_message(
            f"Connected as {self.nickname}. Active mesh: {self.client.mesh_id}. "
            "Use F2/F3 to switch rooms and /help for commands."
        )
        if not self.client.bootstrap_peers and self.client.trackers == []:
            self._system_message(
                "No bootstrap peers configured. This node stays isolated until another peer dials it or you use /connect HOST:PORT."
            )
        elif self.client.trackers is None:
            self._system_message("Tracker bootstrap is enabled with the default public tracker set.")
        elif self.client.trackers:
            self._system_message(
                f"Tracker bootstrap is enabled with {len(self.client.trackers)} configured tracker(s)."
            )
        if self.share_host:
            self._system_message(
                "For another machine, connect with "
                f"--peer {self.share_host}:{self.client.listen_port or 'PORT'} "
                "(do not use 127.0.0.1 across hosts)."
            )
        else:
            self._system_message(
                "Use a LAN-reachable address or hostname for cross-machine peers, not 127.0.0.1."
            )
        self._refresh_views()
        status_thread = threading.Thread(target=self._status_loop, daemon=True)
        status_thread.start()
        try:
            self.application.run()
        finally:
            self._stop.set()
            self.client.stop()

    def _status_loop(self) -> None:
        while not self._stop.is_set():
            try:
                info = self.client.mesh_info()
            except Exception as exc:
                self._system_message(f"mesh info error: {exc}")
                self._stop.wait(1.0)
                continue
            with self._lock:
                self._mesh_info = info
            self._invalidate()
            self._stop.wait(1.0)

    def _accept_input(self, buffer) -> bool:
        text = buffer.text.strip()
        buffer.text = ""
        if text:
            self._handle_input(text)
        return False

    def _handle_input(self, text: str) -> None:
        if text.startswith("/"):
            self._handle_command(text[1:])
            return
        room_name = self.current_room
        if room_name == SYSTEM_ROOM:
            self._system_message("Switch to a subscribed room before sending chat messages.")
            return
        payload = {
            "kind": "chat",
            "nick": self.nickname,
            "text": text,
            "sent_at": current_timestamp(),
        }
        try:
            code = self.client.publish(room_name, json.dumps(payload).encode("utf-8"))
        except Exception as exc:
            self._system_message(f"publish failed: {exc}")
            return
        if code == MOSS_ERR_NO_PEERS:
            self._system_message(f"Room #{room_name} has no connected peers yet; message stayed local.")

    def _handle_command(self, command_line: str) -> None:
        parts = command_line.strip().split(maxsplit=1)
        command = parts[0].lower() if parts else ""
        argument = parts[1] if len(parts) > 1 else ""

        if command == "help":
            self._system_message(
                "Commands: /join ROOM, /leave [ROOM], /goto ROOM, /nick NAME, /rooms, /status, /diag, /connect HOST:PORT, /quit"
            )
            return
        if command in {"quit", "exit"}:
            if self.application is not None:
                self.application.exit()
            return
        if command == "join":
            if not argument:
                self._system_message("Usage: /join ROOM")
                return
            try:
                room_name = normalize_room(argument)
                self._join_room(room_name)
            except Exception as exc:
                self._system_message(str(exc))
            return
        if command == "goto":
            if not argument:
                self._system_message("Usage: /goto ROOM")
                return
            try:
                room_name = normalize_room(argument)
            except Exception as exc:
                self._system_message(str(exc))
                return
            if room_name not in self.rooms:
                self._system_message(f"Room #{room_name} is not joined yet.")
                return
            self._select_room(room_name)
            return
        if command == "leave":
            target = argument or self.current_room
            try:
                room_name = normalize_room(target)
            except Exception as exc:
                self._system_message(str(exc))
                return
            self._leave_room(room_name)
            return
        if command == "nick":
            if not argument:
                self._system_message("Usage: /nick NAME")
                return
            try:
                self.nickname = parse_nickname(argument)
            except Exception as exc:
                self._system_message(str(exc))
                return
            self._system_message(f"Nickname changed to {self.nickname}.")
            self._invalidate()
            return
        if command == "rooms":
            joined = ", ".join(f"#{name}" for name in self.room_order if name != SYSTEM_ROOM)
            self._system_message(f"Joined rooms: {joined or '(none)'}")
            return
        if command == "status":
            info = self.client.mesh_info()
            peers = info.get("peer_count", 0)
            nat_type = info.get("nat_type", self.client.nat_type())
            self._system_message(
                f"mesh={self.client.mesh_id} room=#{self.current_room} peers={peers} nat={nat_type}"
            )
            return
        if command == "diag":
            self._show_diag()
            return
        if command == "connect":
            if not argument:
                self._system_message("Usage: /connect HOST:PORT")
                return
            try:
                self.client.connect(argument.strip())
            except Exception as exc:
                self._system_message(f"connect failed: {exc}")
                return
            self._system_message(f"Connecting to {argument.strip()}...")
            return
        self._system_message(f"Unknown command: /{command}")

    def _show_diag(self) -> None:
        info = self.client.mesh_info()
        channels = ", ".join(f"#{room}" for room in info.get("channels", [])) or "(none)"
        peers = ", ".join(info.get("peers", [])) or "(none)"
        bootstrap = ", ".join(self.client.bootstrap_peers) or "(none)"
        nat_type = info.get("nat_type", self.client.nat_type())
        listen_port = info.get("listen_port", self.client.listen_port)
        public_key = self.client.public_key_hex[:16]
        share_host = self.share_host or "(unknown)"
        self._system_message(
            f"diag: peers={info.get('peer_count', 0)} listen={share_host}:{listen_port} nat={nat_type} relay={info.get('supernode_ready', False)}"
        )
        self._system_message(f"diag: bootstrap peers={bootstrap}")
        self._system_message(f"diag: connected peers={peers}")
        self._system_message(f"diag: rooms={channels} pubkey={public_key}...")

    def _join_room(self, room_name: str) -> None:
        if room_name == SYSTEM_ROOM:
            self._select_room(SYSTEM_ROOM)
            return
        if room_name not in self.rooms:
            self._ensure_room(room_name, subscribed=True)
        room = self.rooms[room_name]
        if not room.subscribed:
            self.client.subscribe(room_name)
            room.subscribed = True
        self._system_message(f"Joined room #{room_name}.")
        self._select_room(room_name)

    def _leave_room(self, room_name: str) -> None:
        if room_name == SYSTEM_ROOM:
            self._system_message("The system room is always available.")
            return
        room = self.rooms.get(room_name)
        if room is None or not room.subscribed:
            self._system_message(f"Room #{room_name} is not joined.")
            return
        self.client.unsubscribe(room_name)
        room.subscribed = False
        self._system_message(f"Left room #{room_name}.")
        if self.current_room == room_name:
            fallback = next(
                (name for name in self.room_order if name != room_name and self.rooms[name].subscribed),
                SYSTEM_ROOM,
            )
            self._select_room(fallback)
        self._refresh_views()

    def _ensure_room(self, room_name: str, *, subscribed: bool) -> None:
        room_name = normalize_room(room_name)
        if room_name not in self.rooms:
            self.rooms[room_name] = RoomState(room_name, subscribed, f"#{room_name}")
            self.room_order.append(room_name)
        elif subscribed:
            self.rooms[room_name].subscribed = True

    def _select_room(self, room_name: str) -> None:
        with self._lock:
            self.current_room = room_name
            room = self.rooms[room_name]
            room.unread = 0
        self._refresh_views()

    def _cycle_room(self, step: int) -> None:
        with self._lock:
            visible = [name for name in self.room_order if self.rooms[name].subscribed or name == SYSTEM_ROOM]
            if self.current_room not in visible:
                self.current_room = visible[0]
                self.rooms[self.current_room].unread = 0
            else:
                index = visible.index(self.current_room)
                next_index = (index + step) % len(visible)
                self.current_room = visible[next_index]
                self.rooms[self.current_room].unread = 0
        self._refresh_views()

    def _on_message(self, channel: str, sender_hex: str, payload: bytes) -> None:
        room_name = normalize_room(channel)
        self._ensure_room(room_name, subscribed=True)
        line = render_chat_line(sender_hex, self.client.public_key_hex, payload)
        self._append_line(room_name, line)

    def _on_event(self, event_type: int, detail: dict) -> None:
        name = EVENT_NAMES.get(event_type, f"event_{event_type}")
        if name == "peer_joined":
            message = f"Peer joined: {format_peer(detail.get('peer', '?'))} @ {detail.get('addr', '?')}"
        elif name == "peer_left":
            message = f"Peer left: {format_peer(detail.get('peer', '?'))}"
        elif name == "supernode_promoted":
            message = f"Local node became relay-capable ({detail.get('nat_type', 'unknown')})."
        elif name == "supernode_revoked":
            message = f"Local node is no longer relay-capable ({detail.get('nat_type', 'unknown')})."
        elif name == "tracker_announce":
            message = (
                "Tracker returned "
                f"{detail.get('candidate_peers', detail.get('peers', 0))} candidate peers; "
                f"connected now: {detail.get('connected_peers', '?')}."
            )
        elif name == "tracker_failure":
            message = f"Tracker error: {detail.get('error', 'unknown')}"
        elif name == "relay_migrated":
            message = (
                f"Relay session {detail.get('session', '?')} migrated to direct peer "
                f"{format_peer(detail.get('peer', '?'))}."
            )
        else:
            message = f"{name}: {json.dumps(detail, sort_keys=True)}"
        self._system_message(message)

    def _append_line(self, room_name: str, line: str) -> None:
        with self._lock:
            room = self.rooms[room_name]
            room.lines.append(line)
            if len(room.lines) > 500:
                room.lines = room.lines[-500:]
            if room_name != self.current_room:
                room.unread += 1
        self._refresh_views()

    def _system_message(self, line: str) -> None:
        self._append_line(SYSTEM_ROOM, f"[{current_timestamp()}] system: {line}")

    def _refresh_views(self) -> None:
        with self._lock:
            rooms_lines = []
            for name in self.room_order:
                room = self.rooms[name]
                if not room.subscribed and name != SYSTEM_ROOM:
                    continue
                marker = ">" if name == self.current_room else " "
                unread = f" ({room.unread})" if room.unread else ""
                rooms_lines.append(f"{marker} {room.title}{unread}")
            current_lines = list(self.rooms[self.current_room].lines)
            info = dict(self._mesh_info)

        if self.rooms_view is None or self.messages_view is None or self.status_bar is None or self.composer is None:
            return

        self.rooms_view.text = "\n".join(rooms_lines) or "(no rooms)"
        self.messages_view.text = "\n".join(current_lines) or "No messages yet."
        self.messages_view.buffer.cursor_position = len(self.messages_view.text)
        peers = info.get("peer_count", 0)
        nat_type = info.get("nat_type", self.client.nat_type())
        listen_port = info.get("listen_port", self.client.listen_port)
        relay_ready = "yes" if info.get("supernode_ready") else "no"
        self.status_bar.text = (
            f"mesh={self.client.mesh_id} nick={self.nickname} room=#{self.current_room} "
            f"peers={peers} nat={nat_type} relay-ready={relay_ready} port={listen_port}"
        )
        self.composer.prompt = f"{self.nickname}@{self.current_room}> "
        self._invalidate()

    def _invalidate(self) -> None:
        if getattr(self, "application", None) is not None:
            self.application.invalidate()

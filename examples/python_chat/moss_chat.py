from __future__ import annotations

import argparse
import ctypes
import json
import os
import socket
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable


MOSS_OK = 0
MOSS_ERR_NO_PEERS = -6
PUBLIC_KEY_LEN = 32
SYSTEM_ROOM = "system"
DEFAULT_ROOM = "lobby"
DEFAULT_MESH = "moss-chat-demo"
RESERVED_NICKNAMES = {"system", "you"}
MAX_NICKNAME_LEN = 32


MossMessageCallback = ctypes.CFUNCTYPE(
    None,
    ctypes.c_char_p,
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.c_uint32,
)
MossEventCallback = ctypes.CFUNCTYPE(None, ctypes.c_int32, ctypes.c_char_p)
MossKeyStoreLoadCallback = ctypes.CFUNCTYPE(
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.c_uint32,
)
MossKeyStoreSaveCallback = ctypes.CFUNCTYPE(
    None,
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.c_uint32,
)


ERROR_NAMES = {
    0: "ok",
    -1: "invalid handle",
    -2: "already started",
    -3: "not started",
    -4: "invalid channel",
    -5: "message too large",
    -6: "no peers",
    -7: "tracker failure",
    -8: "invalid config",
    -9: "out of memory",
    -10: "connect failed",
}

EVENT_NAMES = {
    1: "peer_joined",
    2: "peer_left",
    3: "supernode_promoted",
    4: "supernode_revoked",
    5: "tracker_announce",
    6: "tracker_failure",
    7: "relay_migrated",
}

_INIT_LOCK = threading.Lock()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_library() -> ctypes.CDLL:
    candidates = [
        repo_root() / "moss.dll",
        repo_root() / "libmoss.so",
        repo_root() / "libmoss.dylib",
    ]
    for candidate in candidates:
        if candidate.exists():
            return ctypes.CDLL(str(candidate))
    names = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(f"moss shared library not found, looked for: {names}")


LIB = None if os.environ.get("MOSS_CHAT_SKIP_LIB") == "1" else load_library()


def bind_function(name: str, argtypes: list[object], restype: object):
    if LIB is None:
        def missing_symbol(*_args):
            raise RuntimeError("moss shared library loading was skipped")

        return missing_symbol
    try:
        fn = getattr(LIB, name)
    except AttributeError as exc:
        raise RuntimeError(
            "moss shared library is missing required symbol "
            f"{name}. Rebuild it with: go build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi"
        ) from exc
    fn.argtypes = argtypes
    fn.restype = restype
    return fn


Moss_Init = bind_function(
    "Moss_Init",
    [ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_char_p],
    ctypes.c_int64,
)
Moss_Start = bind_function("Moss_Start", [ctypes.c_int64], ctypes.c_int32)
Moss_Stop = bind_function("Moss_Stop", [ctypes.c_int64], ctypes.c_int32)
Moss_Connect = bind_function("Moss_Connect", [ctypes.c_int64, ctypes.c_char_p], ctypes.c_int32)
Moss_Subscribe = bind_function("Moss_Subscribe", [ctypes.c_int64, ctypes.c_char_p], ctypes.c_int32)
Moss_Unsubscribe = bind_function("Moss_Unsubscribe", [ctypes.c_int64, ctypes.c_char_p], ctypes.c_int32)
Moss_Publish = bind_function(
    "Moss_Publish",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_uint32],
    ctypes.c_int32,
)
Moss_SetCallback = bind_function("Moss_SetCallback", [ctypes.c_int64, MossMessageCallback], ctypes.c_int32)
Moss_SetEventCallback = bind_function(
    "Moss_SetEventCallback",
    [ctypes.c_int64, MossEventCallback],
    ctypes.c_int32,
)
Moss_SetKeyStore = bind_function("Moss_SetKeyStore", [ctypes.c_void_p, ctypes.c_void_p], ctypes.c_int32)
Moss_GetMeshInfo = bind_function("Moss_GetMeshInfo", [ctypes.c_int64], ctypes.c_void_p)
Moss_GetPublicKey = bind_function(
    "Moss_GetPublicKey",
    [ctypes.c_int64],
    ctypes.POINTER(ctypes.c_uint8),
)
Moss_GetNATType = bind_function("Moss_GetNATType", [ctypes.c_int64], ctypes.c_void_p)
Moss_Free = bind_function("Moss_Free", [ctypes.c_void_p], None)


class MossError(RuntimeError):
    pass


def error_name(code: int) -> str:
    return ERROR_NAMES.get(code, f"unknown error {code}")


def safe_json_load(raw: str) -> dict:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def current_timestamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def normalize_room(name: str) -> str:
    room = name.strip().lower()
    if room.startswith("#"):
        room = room[1:]
    if not room:
        raise ValueError("room name cannot be empty")
    return room


def format_peer(peer_id: str) -> str:
    if len(peer_id) < 10:
        return peer_id
    return f"{peer_id[:8]}..{peer_id[-4:]}"


def one_line(value: object) -> str:
    return str(value).replace("\r", " ").replace("\n", " ").strip()


def clean_nickname(value: object) -> str:
    nick = " ".join(one_line(value).split())
    return nick[:MAX_NICKNAME_LEN]


def parse_nickname(value: str) -> str:
    nick = clean_nickname(value)
    if not nick:
        raise argparse.ArgumentTypeError("nickname cannot be empty")
    if nick.lower() in RESERVED_NICKNAMES:
        raise argparse.ArgumentTypeError(f"nickname {nick!r} is reserved")
    return nick


def parse_psk_hex(value: str | None) -> bytes | None:
    if not value:
        return None
    try:
        raw = bytes.fromhex(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("PSK must be 64 hex characters") from exc
    if len(raw) != PUBLIC_KEY_LEN:
        raise argparse.ArgumentTypeError("PSK must decode to exactly 32 bytes")
    return raw


def sender_label(sender_hex: str, local_peer_hex: str, nick: object | None) -> str:
    if sender_hex and sender_hex == local_peer_hex:
        return "you"
    peer = format_peer(sender_hex) if sender_hex else "unknown-peer"
    clean = clean_nickname(nick) if nick is not None else ""
    if not clean or clean.lower() in RESERVED_NICKNAMES:
        return peer
    return f"{clean} [{peer}]"


def render_chat_line(sender_hex: str, local_peer_hex: str, payload: bytes) -> str:
    raw = payload.decode("utf-8", errors="replace")
    message = safe_json_load(raw)
    text = message.get("text")
    nick = message.get("nick") if text else None
    if not text:
        text = raw
    sent_at = one_line(message.get("sent_at") or current_timestamp())
    return f"[{sent_at}] {sender_label(sender_hex, local_peer_hex, nick)}: {one_line(text)}"


def detect_share_host() -> str | None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            host = sock.getsockname()[0]
    except OSError:
        host = None
    if host and not host.startswith("127."):
        return host
    try:
        candidates = socket.getaddrinfo(socket.gethostname(), None, family=socket.AF_INET)
    except socket.gaierror:
        return None
    for candidate in candidates:
        host = candidate[4][0]
        if host and not host.startswith("127."):
            return host
    return None


@dataclass
class RoomState:
    name: str
    subscribed: bool
    title: str
    unread: int = 0
    lines: list[str] = field(default_factory=list)


class MossClient:
    def __init__(
        self,
        *,
        mesh_id: str,
        listen_port: int,
        peers: list[str],
        identity_path: Path | None,
        psk: bytes | None = None,
        trackers: list[str] | None = None,
        heartbeat_ms: int = 250,
    ) -> None:
        self.mesh_id = mesh_id
        self.listen_port = listen_port
        self.identity_path = identity_path
        self.bootstrap_peers = list(peers)
        self.trackers = None if trackers is None else list(trackers)
        self._message_handler: Callable[[str, str, bytes], None] | None = None
        self._event_handler: Callable[[int, dict], None] | None = None
        self._message_cb = MossMessageCallback(self._handle_message)
        self._event_cb = MossEventCallback(self._handle_event)
        self._keystore_load_cb = None
        self._keystore_save_cb = None

        config = {
            "listen_port": listen_port,
            "static_peers": peers,
            "announce_interval_sec": 1,
            "gossipsub": {"heartbeat_ms": heartbeat_ms},
        }
        if trackers is not None:
            config["trackers"] = trackers

        with _INIT_LOCK:
            self._configure_keystore(identity_path)
            psk_ptr = None
            if psk is not None:
                psk_buf = (ctypes.c_uint8 * len(psk)).from_buffer_copy(psk)
                psk_ptr = psk_buf
            handle = Moss_Init(
                mesh_id.encode("utf-8"),
                psk_ptr,
                json.dumps(config).encode("utf-8"),
            )
        if handle <= 0:
            raise MossError(f"Moss_Init failed: {error_name(int(handle))}")
        self.handle = int(handle)
        self._call(Moss_SetCallback(self.handle, self._message_cb), "Moss_SetCallback")
        self._call(Moss_SetEventCallback(self.handle, self._event_cb), "Moss_SetEventCallback")
        self.public_key_hex = self._get_public_key()

    def _configure_keystore(self, identity_path: Path | None) -> None:
        if identity_path is None:
            code = Moss_SetKeyStore(None, None)
            if code != MOSS_OK:
                raise MossError(f"Moss_SetKeyStore failed: {error_name(int(code))}")
            return

        identity_path.parent.mkdir(parents=True, exist_ok=True)

        @MossKeyStoreLoadCallback
        def load(buffer: ctypes.POINTER(ctypes.c_uint8), capacity: int) -> int:
            if not identity_path.exists():
                return 0
            raw = identity_path.read_bytes()
            if not buffer or capacity == 0:
                return len(raw)
            write_len = min(len(raw), int(capacity))
            ctypes.memmove(buffer, raw, write_len)
            return write_len

        @MossKeyStoreSaveCallback
        def save(data: ctypes.POINTER(ctypes.c_uint8), length: int) -> None:
            if not data or length == 0:
                return
            raw = ctypes.string_at(data, int(length))
            identity_path.write_bytes(raw)

        self._keystore_load_cb = load
        self._keystore_save_cb = save
        code = Moss_SetKeyStore(
            ctypes.cast(load, ctypes.c_void_p),
            ctypes.cast(save, ctypes.c_void_p),
        )
        if code != MOSS_OK:
            raise MossError(f"Moss_SetKeyStore failed: {error_name(int(code))}")

    def _call(self, code: int, opname: str, *, allow_no_peers: bool = False) -> int:
        if allow_no_peers and code == MOSS_ERR_NO_PEERS:
            return code
        if code != MOSS_OK:
            raise MossError(f"{opname} failed: {error_name(int(code))}")
        return code

    def _get_public_key(self) -> str:
        ptr = Moss_GetPublicKey(self.handle)
        if not ptr:
            raise MossError("Moss_GetPublicKey returned null")
        try:
            return ctypes.string_at(ptr, PUBLIC_KEY_LEN).hex()
        finally:
            Moss_Free(ptr)

    def set_message_handler(self, handler: Callable[[str, str, bytes], None]) -> None:
        self._message_handler = handler

    def set_event_handler(self, handler: Callable[[int, dict], None]) -> None:
        self._event_handler = handler

    def start(self) -> None:
        self._call(Moss_Start(self.handle), "Moss_Start")

    def stop(self) -> None:
        if getattr(self, "handle", 0) <= 0:
            return
        Moss_Stop(self.handle)
        self.handle = 0

    def subscribe(self, room: str) -> None:
        self._call(Moss_Subscribe(self.handle, room.encode("utf-8")), "Moss_Subscribe")

    def connect(self, addr: str) -> None:
        self._call(Moss_Connect(self.handle, addr.encode("utf-8")), "Moss_Connect")

    def unsubscribe(self, room: str) -> None:
        self._call(Moss_Unsubscribe(self.handle, room.encode("utf-8")), "Moss_Unsubscribe")

    def publish(self, room: str, payload: bytes) -> int:
        if payload:
            raw = (ctypes.c_uint8 * len(payload)).from_buffer_copy(payload)
            code = Moss_Publish(self.handle, room.encode("utf-8"), raw, len(payload))
        else:
            code = Moss_Publish(self.handle, room.encode("utf-8"), None, 0)
        return self._call(code, "Moss_Publish", allow_no_peers=True)

    def mesh_info(self) -> dict:
        ptr = Moss_GetMeshInfo(self.handle)
        if not ptr:
            return {}
        try:
            raw = ctypes.string_at(ptr).decode("utf-8", errors="replace")
        finally:
            Moss_Free(ptr)
        return safe_json_load(raw)

    def nat_type(self) -> str:
        ptr = Moss_GetNATType(self.handle)
        if not ptr:
            return "unknown"
        try:
            return ctypes.string_at(ptr).decode("utf-8", errors="replace")
        finally:
            Moss_Free(ptr)

    def _handle_message(
        self,
        channel: bytes,
        sender_id: ctypes.POINTER(ctypes.c_uint8),
        data: ctypes.POINTER(ctypes.c_uint8),
        length: int,
    ) -> None:
        handler = self._message_handler
        if handler is None:
            return
        channel_name = channel.decode("utf-8", errors="replace")
        sender_hex = ""
        if sender_id:
            sender_hex = ctypes.string_at(sender_id, PUBLIC_KEY_LEN).hex()
        payload = ctypes.string_at(data, int(length)) if data and length else b""
        handler(channel_name, sender_hex, payload)

    def _handle_event(self, event_type: int, detail_json: bytes) -> None:
        handler = self._event_handler
        if handler is None:
            return
        raw = detail_json.decode("utf-8", errors="replace") if detail_json else "{}"
        handler(int(event_type), safe_json_load(raw))


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive Moss chat demo")
    parser.add_argument("--nickname", required=True, type=parse_nickname, help="Nickname shown in the chat")
    parser.add_argument("--mesh", default=DEFAULT_MESH, help="Mesh ID shared by all chat participants")
    parser.add_argument("--listen-port", type=int, default=0, help="Local listen port for this node")
    parser.add_argument(
        "--peer",
        action="append",
        default=[],
        help="Static peer address to connect to, e.g. 127.0.0.1:41030",
    )
    parser.add_argument(
        "--room",
        action="append",
        default=[DEFAULT_ROOM],
        help="Initial room to join. Can be provided multiple times.",
    )
    parser.add_argument(
        "--tracker",
        action="append",
        default=None,
        help="Override tracker list. Can be provided multiple times.",
    )
    parser.add_argument(
        "--no-trackers",
        action="store_true",
        help="Disable automatic tracker bootstrap and rely only on static peers or inbound dials.",
    )
    parser.add_argument(
        "--identity-file",
        help="Path to persist the Moss identity for this chat profile",
    )
    parser.add_argument(
        "--psk-hex",
        type=parse_psk_hex,
        default=None,
        help="Optional 32-byte pre-shared key as 64 hex characters for private meshes",
    )
    return parser.parse_args()


def resolve_identity_path(args: argparse.Namespace) -> Path:
    if args.identity_file:
        return Path(args.identity_file).resolve()
    safe_name = "".join(ch for ch in args.nickname.lower() if ch.isalnum() or ch in {"-", "_"}) or "user"
    return repo_root() / "examples" / "python_chat" / ".state" / f"{safe_name}.identity"


def main() -> None:
    args = parse_args()
    rooms = []
    for room in args.room:
        room_name = normalize_room(room)
        if room_name not in rooms:
            rooms.append(room_name)
    if not rooms:
        rooms = [DEFAULT_ROOM]

    trackers = args.tracker
    if args.no_trackers:
        trackers = []

    client = MossClient(
        mesh_id=args.mesh,
        listen_port=args.listen_port,
        peers=args.peer,
        trackers=trackers,
        identity_path=resolve_identity_path(args),
        psk=args.psk_hex,
    )
    app = MossChatApp(client=client, nickname=args.nickname, initial_rooms=rooms)
    app.run()


if __name__ == "__main__":
    main()

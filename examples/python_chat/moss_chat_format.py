from __future__ import annotations

import argparse
import json
import socket
from datetime import datetime

from moss_chat_native import MAX_NICKNAME_LEN, PUBLIC_KEY_LEN, RESERVED_NICKNAMES, safe_json_load

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


def resolve_tracker_options(args: argparse.Namespace) -> tuple[list[str] | None, bool]:
    if args.no_trackers:
        return [], False
    if args.tracker is not None:
        return args.tracker, False
    if args.default_trackers:
        return None, True
    return [], False


def build_moss_config(
    *, listen_port: int, peers: list[str], trackers: list[str] | None, heartbeat_ms: int
) -> dict[str, object]:
    config: dict[str, object] = {
        "listen_port": listen_port,
        "static_peers": peers,
        "announce_interval_sec": 1,
        "gossipsub": {"heartbeat_ms": heartbeat_ms},
    }
    if trackers is not None:
        config["trackers"] = trackers
    return config


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

from __future__ import annotations

import argparse
from pathlib import Path

from moss_chat_app import MossChatApp
from moss_chat_client import MossClient
from moss_chat_format import (
    normalize_room,
    parse_nickname,
    parse_psk_hex,
    resolve_tracker_options,
)
from moss_chat_native import DEFAULT_MESH, DEFAULT_ROOM, repo_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive Moss chat demo")
    parser.add_argument(
        "--nickname",
        required=True,
        type=parse_nickname,
        help="Nickname shown in the chat",
    )
    parser.add_argument(
        "--mesh", default=DEFAULT_MESH, help="Mesh ID shared by all chat participants"
    )
    parser.add_argument(
        "--listen-port", type=int, default=0, help="Local listen port for this node"
    )
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
        "--default-trackers",
        action="store_true",
        help="Use the built-in public tracker set. Public no-PSK meshes are discoverable.",
    )
    parser.add_argument(
        "--no-trackers",
        action="store_true",
        help="Disable tracker bootstrap and rely only on static peers or inbound dials.",
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
    safe_name = (
        "".join(ch for ch in args.nickname.lower() if ch.isalnum() or ch in {"-", "_"})
        or "user"
    )
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

    trackers, use_default_trackers = resolve_tracker_options(args)

    client = MossClient(
        mesh_id=args.mesh,
        listen_port=args.listen_port,
        peers=args.peer,
        trackers=trackers,
        use_default_trackers=use_default_trackers,
        identity_path=resolve_identity_path(args),
        psk=args.psk_hex,
    )
    app = MossChatApp(client=client, nickname=args.nickname, initial_rooms=rooms)
    app.run()


if __name__ == "__main__":
    main()

from __future__ import annotations

from moss_chat_app import MossChatApp
from moss_chat_cli import main, parse_args, resolve_identity_path
from moss_chat_client import MossClient, RoomState
from moss_chat_format import (
    build_moss_config,
    clean_nickname,
    current_timestamp,
    detect_share_host,
    format_peer,
    normalize_room,
    one_line,
    parse_nickname,
    parse_psk_hex,
    render_chat_line,
    resolve_tracker_options,
    sender_label,
)
from moss_chat_identity import (
    ensure_private_identity_dir,
    read_private_identity,
    restrict_existing_identity_file,
    secure_identity_open_flags,
    write_private_identity,
)
from moss_chat_native import *

if __name__ == "__main__":
    main()

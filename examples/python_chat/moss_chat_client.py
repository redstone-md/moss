from __future__ import annotations

import ctypes
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from moss_chat_format import build_moss_config
from moss_chat_identity import (
    ensure_private_identity_dir,
    read_private_identity,
    restrict_existing_identity_file,
    write_private_identity,
)
from moss_chat_native import (
    MOSS_ERR_NO_PEERS,
    MOSS_OK,
    PUBLIC_KEY_LEN,
    MossError,
    MossEventCallback,
    MossMessageCallback,
    MossKeyStoreLoadCallback,
    MossKeyStoreSaveCallback,
    Moss_Connect,
    Moss_Free,
    Moss_GetMeshInfo,
    Moss_GetNATType,
    Moss_GetPublicKey,
    Moss_Init,
    Moss_Publish,
    Moss_SetCallback,
    Moss_SetEventCallback,
    Moss_SetKeyStore,
    Moss_Start,
    Moss_Stop,
    Moss_Subscribe,
    Moss_Unsubscribe,
    _INIT_LOCK,
    error_name,
    safe_json_load,
)

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
        use_default_trackers: bool = False,
        heartbeat_ms: int = 250,
    ) -> None:
        self.mesh_id = mesh_id
        self.listen_port = listen_port
        self.identity_path = identity_path
        self.bootstrap_peers = list(peers)
        self.trackers = None if use_default_trackers else list(trackers or [])
        self._message_handler: Callable[[str, str, bytes], None] | None = None
        self._event_handler: Callable[[int, dict], None] | None = None
        self._message_cb = MossMessageCallback(self._handle_message)
        self._event_cb = MossEventCallback(self._handle_event)
        self._keystore_load_cb = None
        self._keystore_save_cb = None

        config = build_moss_config(
            listen_port=listen_port,
            peers=peers,
            trackers=self.trackers,
            heartbeat_ms=heartbeat_ms,
        )

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

        ensure_private_identity_dir(identity_path)
        restrict_existing_identity_file(identity_path)

        @MossKeyStoreLoadCallback
        def load(buffer: ctypes.POINTER(ctypes.c_uint8), capacity: int) -> int:
            raw = read_private_identity(identity_path)
            if not raw:
                return 0
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
            write_private_identity(identity_path, raw)

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

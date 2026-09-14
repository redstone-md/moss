from __future__ import annotations

import ctypes
import json
import os
import threading
from pathlib import Path

MOSS_OK = 0
MOSS_ERR_NO_PEERS = -6
PUBLIC_KEY_LEN = 32
SYSTEM_ROOM = "system"
DEFAULT_ROOM = "lobby"
DEFAULT_MESH = "moss-chat-demo"
RESERVED_NICKNAMES = {"system", "you"}
MAX_NICKNAME_LEN = 32
IDENTITY_DIR_MODE = 0o700
IDENTITY_FILE_MODE = 0o600


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
MossRelayCallback = ctypes.CFUNCTYPE(
    None,
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.c_uint32,
)
MossPacketCallback = ctypes.CFUNCTYPE(
    None,
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.c_uint32,
)

MossScoringCallback = ctypes.CFUNCTYPE(
    ctypes.c_double,
    ctypes.POINTER(ctypes.c_uint8),
    ctypes.c_double,
)
MossStreamCallback = ctypes.CFUNCTYPE(
    None,
    ctypes.c_char_p,
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
    -11: "relay failed",
    -12: "internal",
    -13: "listen failed",
    -14: "not in room",
}

EVENT_NAMES = {
    1: "peer_joined",
    2: "peer_left",
    3: "supernode_promoted",
    4: "supernode_revoked",
    5: "tracker_announce",
    6: "tracker_failure",
    7: "relay_migrated",
    8: "message_delivered",
    9: "message_read",
    10: "typing",
    11: "presence",
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

# --- rooms: one node serving several conversations ---
Moss_JoinRoom = bind_function(
    "Moss_JoinRoom",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_uint32],
    ctypes.c_int32,
)
Moss_LeaveRoom = bind_function("Moss_LeaveRoom", [ctypes.c_int64, ctypes.c_char_p], ctypes.c_int32)
Moss_SubscribeRoom = bind_function(
    "Moss_SubscribeRoom",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.c_char_p],
    ctypes.c_int32,
)
Moss_UnsubscribeRoom = bind_function(
    "Moss_UnsubscribeRoom",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.c_char_p],
    ctypes.c_int32,
)
Moss_PublishRoom = bind_function(
    "Moss_PublishRoom",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_uint32],
    ctypes.c_int32,
)

# --- directed payloads (DMs): direct session first, relay fallback ---
Moss_ConnectToPeer = bind_function("Moss_ConnectToPeer", [ctypes.c_int64, ctypes.c_char_p], ctypes.c_int32)
Moss_RelaySendTo = bind_function(
    "Moss_RelaySendTo",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_int32],
    ctypes.c_int32,
)
Moss_SendToPeer = bind_function(
    "Moss_SendToPeer",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_int32],
    ctypes.c_int32,
)
Moss_PeerRTT = bind_function("Moss_PeerRTT", [ctypes.c_int64, ctypes.c_char_p], ctypes.c_int64)
Moss_SetRelayCallback = bind_function(
    "Moss_SetRelayCallback",
    [ctypes.c_int64, MossRelayCallback],
    ctypes.c_int32,
)
Moss_SetPacketCallback = bind_function(
    "Moss_SetPacketCallback",
    [ctypes.c_int64, MossPacketCallback],
    ctypes.c_int32,
)

# --- streams: ordered per-stream channels over a direct session ---
# Stream ID convention: 0-1 reserved by transport, 100-101 game profile
# defaults, 300 = messenger app-data default (overridable per-app).
Moss_OpenStream = bind_function(
    "Moss_OpenStream",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.c_uint32],
    ctypes.c_int32,
)
Moss_SendStream = bind_function(
    "Moss_SendStream",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint8), ctypes.c_uint32],
    ctypes.c_int32,
)
Moss_OnStream = bind_function(
    "Moss_OnStream",
    [ctypes.c_int64, ctypes.c_uint32, MossStreamCallback],
    ctypes.c_int32,
)

# --- diagnostics / metadata ---
Moss_Version = bind_function("Moss_Version", [], ctypes.c_void_p)
Moss_LastError = bind_function("Moss_LastError", [ctypes.c_int64], ctypes.c_void_p)
Moss_GetNetworkStats = bind_function("Moss_GetNetworkStats", [ctypes.c_int64], ctypes.c_void_p)



Moss_SetScoringCallback = bind_function(
    "Moss_SetScoringCallback",
    [ctypes.c_int64, MossScoringCallback],
    ctypes.c_int32,
)
Moss_EnableAxiom = bind_function(
    "Moss_EnableAxiom",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p],
    ctypes.c_int32,
)
Moss_LogEvent = bind_function(
    "Moss_LogEvent",
    [ctypes.c_int64, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p],
    ctypes.c_int32,
)

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

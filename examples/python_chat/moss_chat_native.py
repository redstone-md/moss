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

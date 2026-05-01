from __future__ import annotations

import os
from pathlib import Path

from moss_chat_native import IDENTITY_DIR_MODE, IDENTITY_FILE_MODE, MossError

def secure_identity_open_flags(flags: int) -> int:
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    return flags


def ensure_private_identity_dir(identity_path: Path) -> None:
    if identity_path.parent.is_symlink():
        raise MossError(f"identity directory must not be a symlink: {identity_path.parent}")
    identity_path.parent.mkdir(parents=True, mode=IDENTITY_DIR_MODE, exist_ok=True)
    try:
        os.chmod(identity_path.parent, IDENTITY_DIR_MODE)
    except OSError:
        if os.name != "nt":
            raise


def read_private_identity(identity_path: Path) -> bytes:
    flags = secure_identity_open_flags(os.O_RDONLY)
    try:
        fd = os.open(identity_path, flags)
    except FileNotFoundError:
        return b""
    except OSError:
        return b""
    with os.fdopen(fd, "rb") as handle:
        return handle.read()


def restrict_existing_identity_file(identity_path: Path) -> None:
    flags = secure_identity_open_flags(os.O_RDONLY)
    try:
        fd = os.open(identity_path, flags)
    except FileNotFoundError:
        return
    except OSError:
        return
    try:
        try:
            os.fchmod(fd, IDENTITY_FILE_MODE)
        except OSError:
            if os.name != "nt":
                raise
    finally:
        os.close(fd)


def write_private_identity(identity_path: Path, raw: bytes) -> None:
    ensure_private_identity_dir(identity_path)
    flags = secure_identity_open_flags(os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    fd = os.open(identity_path, flags, IDENTITY_FILE_MODE)
    try:
        try:
            os.fchmod(fd, IDENTITY_FILE_MODE)
        except OSError:
            if os.name != "nt":
                raise
        with os.fdopen(fd, "wb") as handle:
            fd = -1
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if fd >= 0:
            os.close(fd)

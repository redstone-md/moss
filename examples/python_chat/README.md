# Python Chat Demo

This example is a real terminal chat client built on top of `moss.dll` via `ctypes`.

Features:
- chat rooms mapped to Moss channels
- nicknames
- system event log
- unread counters per room
- persistent Moss identity per profile
- full-screen terminal UI

## Install

From the repository root:

```powershell
go build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi
python -m pip install -r examples\python_chat\requirements.txt
```

## Quick Start

Open two terminals from the repository root.

Terminal 1:

```powershell
python examples\python_chat\moss_chat.py --nickname Alice --listen-port 41030 --room lobby
```

Terminal 2:

```powershell
python examples\python_chat\moss_chat.py --nickname Bob --listen-port 41031 --peer 127.0.0.1:41030 --room lobby
```

For a second machine on the same LAN, use the first machine's hostname or LAN IP instead of `127.0.0.1`:

```powershell
python examples\python_chat\moss_chat.py --nickname Pi --listen-port 41030 --peer rpi1.local:41030 --room lobby
```

Or launch both demo windows automatically:

```powershell
powershell -ExecutionPolicy Bypass -File examples\python_chat\start_local_demo.ps1
```

## Usage

- Type a message and press `Enter` to send it to the current room.
- Use `F2` / `F3` to switch joined rooms.
- Start in `#lobby` by default.

Supported commands:

- `/join ROOM`
- `/goto ROOM`
- `/leave [ROOM]`
- `/nick NAME`
- `/rooms`
- `/status`
- `/diag`
- `/connect HOST:PORT`
- `/help`
- `/quit`

## Notes

- For the local demo, the second client needs `--peer 127.0.0.1:PORT_OF_FIRST_CLIENT`.
- For Raspberry Pi / LAN testing, use `--peer HOSTNAME_OR_LAN_IP:PORT` and make sure the port is allowed by the local firewall.
- A node with no `--peer` and no trackers is isolated until another peer dials it or you run `/connect HOST:PORT`.
- Each nickname gets its own persisted Moss identity in `examples/python_chat/.state/`.
- The app loads the shared library from the repository root: `moss.dll`, `libmoss.so`, or `libmoss.dylib`.

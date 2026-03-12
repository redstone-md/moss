# Moss Chat

`moss-chat` is a native single-binary terminal chat built directly on top of `internal/mesh`.

## Build

```bash
go build -o moss-chat ./cmd/moss-chat
```

On Windows:

```powershell
go build -o moss-chat.exe .\cmd\moss-chat
```

GitHub Actions publishes separate chat artifacts for `macos-amd64` and `macos-arm64`.

## Run

```bash
./moss-chat --nickname Alice --mesh moss-chat-live --listen-port 41036 --room lobby
```

Or just run it without flags and answer the startup prompts:

```bash
./moss-chat
```

Optional flags:

- `--debug` to show tracker/protocol debug events in the `System` room
- `--no-sound` to disable desktop and beep notifications
- `--downloads-dir PATH` to override where attachments are stored

Attachments are interactive:

- `F7` or `/attach PATH` asks for confirmation before broadcasting a file
- receivers click the attachment line in the chat and confirm the download before choosing where to save it
- transfer progress is shown inline in the room

Second peer:

```bash
./moss-chat --nickname Bob --mesh moss-chat-live --listen-port 41051 --room lobby
```

## Commands

- `/join ROOM`
- `/leave [ROOM]`
- `/goto ROOM`
- `/nick NAME`
- `/msg TARGET [TEXT]`
- `/attach PATH`
- `/call TARGET`
- `/answer`
- `/decline`
- `/hangup`
- `/debug`
- `/peers`
- `/rooms`
- `/status`
- `/net`
- `/connect HOST:PORT`
- `/quit`

## Shortcuts

- `F1` help
- `F2` / `F3` switch rooms
- `F4` create or join room
- `F5` open direct chat
- `F6` rename
- `F7` send attachment
- `F8` connect to peer
- `F9` toggle debug
- `F10` call peer
- `Tab` / `Shift-Tab` cycle focus
- `Ctrl-L` focus composer

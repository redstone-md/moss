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

## Run

```bash
./moss-chat --nickname Alice --mesh moss-chat-live --listen-port 41036 --room lobby
```

Or just run it without flags and answer the startup prompts:

```bash
./moss-chat
```

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
- `F8` connect to peer
- `Tab` / `Shift-Tab` cycle focus
- `Ctrl-L` focus composer

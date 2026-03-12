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
- `/rooms`
- `/status`
- `/net`
- `/connect HOST:PORT`
- `/quit`

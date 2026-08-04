# Deploying Moss services

Self-host a **MossScope** (telemetry + web interface) or a **signaling relay**
(for browser peers). Both are static, CGO-free Go binaries in a tiny distroless
image — run them anywhere that takes a container. Configs here target
[Fly.io](https://fly.io), which gives each app free TLS on `*.fly.dev`, so the
scope is reachable over `https` and the relay over `wss` with no extra setup.

Running more independent scopes is good for the network: the interface
cross-checks them, so no single one has to be trusted.

> Run all commands **from the repo root** — the Docker build context must include
> the Go module. The configs pin `[build].dockerfile`, so Fly builds these images
> instead of falling back to its Nixpacks builder (whose placeholder `run-app`
> start command made earlier deploys crash with "Permission denied"). With
> `fly launch`, also pass `--dockerfile` (it may otherwise re-detect Nixpacks),
> and never deploy with `--image` pointing at a previously built Nixpacks image.

## MossScope

The scope config lives at the repo **root** (`fly.toml`) so Fly's GitHub "Deploy
app" button and a bare `fly deploy` both find it. It builds
`deploy/Dockerfile.scope`.

One binary does three jobs: it is an ordinary node on the shared substrate, it
relays for the network, and it serves the MossScope interface compiled into it.
Nothing is mounted — the bundle is embedded, so a deploy is one file.

### First deploy, in order

```bash
fly ips allocate-v4 --app <your-app>   # once: the raw peer port needs its own IP
fly deploy                             # uses ./fly.toml + deploy/Dockerfile.scope
fly certs add scope.example.org        # once, then point DNS at Fly's anycast address
curl -s https://<your-app>.fly.dev/api/health
```

`/api/health` answers with the build version and whether the UI was compiled in.
It is what the platform health check watches — deliberately NOT `/api/stats`,
which reflects the mesh and is legitimately empty when the network is quiet.

### What it serves

| Path | What |
|---|---|
| `/` | the interface (Network view by default) |
| `/api/manifest` | epoch length, ε, k, capabilities, sibling scopes |
| `/api/epochs` | the epoch hash chain — immutable, cached for a year |
| `/api/stats`, `/api/chain`, `/api/events` | kept so pre-scope consumers keep working |
| `/explorer.html` | redirect to `/`, so old links survive |

`serve` is the PUBLIC mode and carries **no inspection routes at all** — they are
not registered, not merely denied (`cmd/moss-scope/mux_test.go` asserts it).
Debugging a node is loopback-only and lives behind `moss-scope attach`.

### Relaying

The scope can only be promoted to a relaying SuperNode if its peer port (4001,
pinned in the Dockerfile) is reachable inbound, which is what the dedicated IPv4
above is for. Fly's anycast address only proxies `[http_service]`.

## Signaling relay

```bash
fly launch --no-deploy --copy-config --config deploy/fly.signal.toml --dockerfile deploy/Dockerfile.signal
fly deploy -a <your-app> --config deploy/fly.signal.toml --dockerfile deploy/Dockerfile.signal
```

Use `wss://<your-app>.fly.dev/signal` as the signaling URL for browser peers.

## A note on relay / supernode nodes

A gateway only *reads* the network. To run a publicly-reachable **relay /
supernode** that helps other peers traverse NAT, the node needs its mesh
TCP/UDP ports reachable from the internet. On Fly that means a dedicated IP and
UDP services (`fly ips allocate-v4`, plus `[[services]]` for the UDP/TCP mesh
port) — heavier than the gateway above. Most operators want the gateway; reach
for a relay only when you specifically want to donate connectivity capacity.

## The deprecated gateway

`moss-gateway` still builds (`make gateway`, `deploy/Dockerfile.gateway`) and is
kept only so existing deployments keep running. It does what `moss-scope serve`
does minus the interface; new deployments should use the scope.

## Recovering a crash-looping app

If an earlier deploy built with Nixpacks, its machines crash-loop on `run-app`.
Redeploy with the root config (it pins the Dockerfile):

```bash
fly deploy -a <your-app>
```

If machines are stuck (`max restart count`, rate-limit spam), reset them:

```bash
fly machine list -a <your-app>
fly machine destroy <id> -a <your-app> --force   # for each bad machine
# or start completely clean:
fly apps destroy <your-app>
```

## Plain Docker

```bash
docker build -f deploy/Dockerfile.gateway -t moss-gateway .
docker run -p 8787:8787 moss-gateway

docker build -f deploy/Dockerfile.signal -t moss-signal .
docker run -p 8788:8788 moss-signal
```

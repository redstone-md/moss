# Deploying Moss services

Self-host a Moss **gateway** (telemetry, read by the explorer) or a **signaling
relay** (for browser peers). Both are static, CGO-free Go binaries in a tiny
distroless image — run them anywhere that takes a container. Configs here target
[Fly.io](https://fly.io), which gives each app free TLS on `*.fly.dev`, so the
gateway is reachable over `https` and the relay over `wss` with no extra setup.

Running more independent gateways is good for the network: the explorer
cross-checks them, so no single one has to be trusted.

> Run all commands **from the repo root** — the Docker build context must include
> the Go module. Always pass **`--dockerfile`** so Fly builds these images and
> does not fall back to its Nixpacks builder (whose placeholder `run-app` start
> command is what made earlier deploys crash with "Permission denied"). And
> never deploy with `--image` pointing at a previously built Nixpacks image.

## Gateway

Create the app once, then deploy with the matching config + Dockerfile and the
name Fly gave you:

```bash
fly launch --no-deploy --copy-config --config deploy/fly.gateway.toml --dockerfile deploy/Dockerfile.gateway
fly deploy -a <your-app> --config deploy/fly.gateway.toml --dockerfile deploy/Dockerfile.gateway
```

Then open the explorer and add `https://<your-app>.fly.dev` in the gateways box,
or share a deep link: `https://moss.surf/explorer.html?gateways=https://<your-app>.fly.dev`.

The gateway joins the mesh as an ordinary member and serves `/api/stats`,
`/api/chain`, and `/api/events`. It needs only outbound connectivity to read
telemetry; Fly's default networking is enough.

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

## Recovering a crash-looping app

If an earlier deploy built with Nixpacks, its machines crash-loop on `run-app`.
Redeploy forcing the Dockerfile:

```bash
fly deploy -a <your-app> --config deploy/fly.gateway.toml --dockerfile deploy/Dockerfile.gateway
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

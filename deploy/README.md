# Deploying Moss services

Self-host a Moss **gateway** (telemetry, read by the explorer) or a **signaling
relay** (for browser peers). Both are static, CGO-free Go binaries in a tiny
distroless image — run them anywhere that takes a container. Configs here target
[Fly.io](https://fly.io), which gives each app free TLS on `*.fly.dev`, so the
gateway is reachable over `https` and the relay over `wss` with no extra setup.

Running more independent gateways is good for the network: the explorer
cross-checks them, so no single one has to be trusted.

## Gateway

```bash
fly launch --no-deploy --copy-config --config deploy/fly.gateway.toml
fly deploy --config deploy/fly.gateway.toml
```

Then open the explorer and add `https://<your-app>.fly.dev` in the gateways box,
or share a deep link: `https://moss.surf/explorer.html?gateways=https://<your-app>.fly.dev`.

The gateway joins the mesh as an ordinary member and serves `/api/stats`,
`/api/chain`, and `/api/events`. It needs only outbound connectivity to read
telemetry; Fly's default networking is enough.

## Signaling relay

```bash
fly deploy --config deploy/fly.signal.toml
```

Use `wss://<your-app>.fly.dev/signal` as the signaling URL for browser peers.

## A note on relay / supernode nodes

A gateway only *reads* the network. To run a publicly-reachable **relay /
supernode** that helps other peers traverse NAT, the node needs its mesh
TCP/UDP ports reachable from the internet. On Fly that means a dedicated IP and
UDP services (`fly ips allocate-v4`, plus `[[services]]` for the UDP/TCP mesh
port) — heavier than the gateway above. Most operators want the gateway; reach
for a relay only when you specifically want to donate connectivity capacity.

## Plain Docker

```bash
docker build -f deploy/Dockerfile -t moss .
docker run -p 8787:8787 moss                      # gateway (default)
docker run -p 8788:8788 moss /usr/local/bin/moss-signal -addr 0.0.0.0:8788
```

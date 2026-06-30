# Mossscan — decentralized network explorer

A static, self-hostable web app that renders live Moss network telemetry —
estimated node count, bandwidth, NAT/degree distributions, and a simulated
network topology — and **verifies it in your browser**.

It is one window among many, not infrastructure: anyone can host this folder,
and because telemetry snapshots are reproducible and hash-chained, a viewer
cannot lie. The explorer checks chain continuity and cross-checks several
independent gateways; disagreement is shown, never hidden.

## How it works

```
 browser (this SPA)
   ├─ moss.wasm ............ pure verifier: chain continuity, cross-gateway
   │                          agreement, deterministic topology simulation
   ├─ fetch /api/stats ..... current aggregate snapshot (per gateway)
   ├─ fetch /api/chain ..... finalized epoch-digest hash chain
   └─ EventSource /api/events  live snapshot push (SSE)
        ▲
        │ read-only, CORS, aggregate-only (no addresses/identities)
   moss-gateway (one or more, run by anyone)
        └─ ordinary Moss node with telemetry enabled
```

The topology graph is a **simulation** seeded by the epoch digest — it is not
the real wiring and exposes no peer addresses. Every explorer renders the
identical picture for a given epoch.

## Build & run

```bash
# 1. Build the wasm verifier + stage wasm_exec.js into this folder
make explorer

# 2. Run at least one gateway (an ordinary node with telemetry on)
make gateway && ./bin/moss-gateway -mesh moss -http 127.0.0.1:8787

# 3. Serve this folder over HTTP (wasm needs a real http origin)
cd explorer && python -m http.server 8080
# open http://localhost:8080  and enter your gateway URL(s)
```

For real trustlessness, point the explorer at **multiple independent gateways**
(comma-separated). The verdict banner turns green only when the chain verifies
and all gateways agree.

## Notes

- `moss.wasm` is a build artifact (git-ignored); run `make explorer` to produce it.
- Telemetry is opt-in on each node (`telemetry.enabled`), so a gateway must run
  with it enabled (the `moss-gateway` binary does this by default).
- This explorer needs no WebRTC: it reads over plain HTTP/SSE. Running a full
  Moss peer *inside* the browser (for mosh-web) is a separate, larger effort.

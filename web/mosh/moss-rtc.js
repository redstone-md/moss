// moss-rtc.js — browser WebRTC + signaling glue for a Moss node.
//
// Responsibilities (the browser-API-heavy half that stays in JS):
//   - connect to a moss-signal server and join a room (mesh id)
//   - establish RTCPeerConnections + ordered DataChannels with other peers
//   - hand each opened DataChannel to the wasm node via mossAttachDataChannel
//
// The wasm node then runs Noise + gossip over the channel. To avoid "glare",
// the newcomer always initiates to peers already present; existing peers wait
// for the offer.

export class MossRTC {
  constructor({ signalUrl, room, selfId, iceServers }) {
    this.signalUrl = signalUrl;
    this.room = room;
    this.selfId = selfId;
    this.iceServers = iceServers || [{ urls: "stun:stun.l.google.com:19302" }];
    this.peers = new Map(); // peerId -> RTCPeerConnection
    this.ws = null;
  }

  start() {
    const url = `${this.signalUrl}?room=${encodeURIComponent(this.room)}&id=${encodeURIComponent(this.selfId)}`;
    this.ws = new WebSocket(url);
    this.ws.onmessage = (ev) => this._onSignal(JSON.parse(ev.data));
    this.ws.onclose = () => { for (const pc of this.peers.values()) pc.close(); this.peers.clear(); };
  }

  _send(obj) { this.ws.send(JSON.stringify(obj)); }

  async _onSignal(msg) {
    switch (msg.type) {
      case "peers": // peers already in the room — we initiate to each
        for (const id of msg.peers) this._createPeer(id, true);
        break;
      case "join": // a newcomer arrived — they will initiate to us; wait
        break;
      case "offer": {
        const pc = this._createPeer(msg.from, false);
        await pc.setRemoteDescription({ type: "offer", sdp: msg.data.sdp });
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        this._send({ to: msg.from, type: "answer", data: { sdp: answer.sdp } });
        break;
      }
      case "answer": {
        const pc = this.peers.get(msg.from);
        if (pc) await pc.setRemoteDescription({ type: "answer", sdp: msg.data.sdp });
        break;
      }
      case "candidate": {
        const pc = this.peers.get(msg.from);
        if (pc && msg.data) await pc.addIceCandidate(msg.data).catch(() => {});
        break;
      }
      case "leave": {
        const pc = this.peers.get(msg.from);
        if (pc) { pc.close(); this.peers.delete(msg.from); }
        break;
      }
    }
  }

  _createPeer(peerId, initiator) {
    let pc = this.peers.get(peerId);
    if (pc) return pc;
    pc = new RTCPeerConnection({ iceServers: this.iceServers });
    this.peers.set(peerId, pc);

    pc.onicecandidate = (e) => {
      if (e.candidate) this._send({ to: peerId, type: "candidate", data: e.candidate.toJSON() });
    };
    pc.onconnectionstatechange = () => {
      if (["failed", "closed", "disconnected"].includes(pc.connectionState)) {
        this.peers.delete(peerId);
      }
    };

    if (initiator) {
      const dc = pc.createDataChannel("moss", { ordered: true });
      this._wireChannel(dc, peerId, true);
      pc.createOffer()
        .then((offer) => pc.setLocalDescription(offer).then(() =>
          this._send({ to: peerId, type: "offer", data: { sdp: offer.sdp } })));
    } else {
      pc.ondatachannel = (e) => this._wireChannel(e.channel, peerId, false);
    }
    return pc;
  }

  _wireChannel(dc, peerId, initiator) {
    dc.binaryType = "arraybuffer";
    dc.onopen = () => {
      // initiator here MUST match the Go handshake role: the offerer (which
      // created the channel) is the Noise client.
      const err = mossAttachDataChannel(dc, initiator, peerId);
      if (err) console.error("attach failed:", err);
    };
  }
}

// loadMossNode boots the wasm runtime and resolves once mossNodeStart exists.
export async function loadMossNode(wasmUrl = "moss-node.wasm") {
  const go = new Go();
  let res;
  try {
    res = await WebAssembly.instantiateStreaming(fetch(wasmUrl), go.importObject);
  } catch (e) {
    // Fallback when the static server doesn't send application/wasm MIME.
    const bytes = await (await fetch(wasmUrl)).arrayBuffer();
    res = await WebAssembly.instantiate(bytes, go.importObject);
  }
  go.run(res.instance); // registers mossNodeStart/Subscribe/Publish/etc.
  // go.run never returns (select{}), so give the callbacks a tick to register.
  await new Promise((r) => setTimeout(r, 0));
}

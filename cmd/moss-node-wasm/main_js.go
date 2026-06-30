//go:build js && wasm

// Command moss-node-wasm is a full Moss peer that runs inside the browser over
// WebRTC. JavaScript owns RTCPeerConnection/ICE and signaling; this wasm module
// runs the encrypted Noise transport, gossip pub/sub, and telemetry over the
// DataChannels JS hands it. It is the runtime mosh-web is built on.
//
// Exposed JS globals:
//
//	mossNodeStart(meshID, pskHex)            -> "" on success, else error text
//	mossSubscribe(channel)                   -> ""|error
//	mossPublish(channel, text)               -> ""|error
//	mossAttachDataChannel(channel, initiator, label)
//	mossOnMessage(fn(channel, senderHex, text))
//	mossStats()                              -> telemetry report JSON
//	mossNodeStop()
package main

import (
	"encoding/hex"
	"syscall/js"

	"moss/internal/mesh"
)

var node *mesh.Node

func main() {
	g := js.Global()
	g.Set("mossNodeStart", js.FuncOf(nodeStart))
	g.Set("mossSubscribe", js.FuncOf(subscribe))
	g.Set("mossPublish", js.FuncOf(publish))
	g.Set("mossAttachDataChannel", js.FuncOf(attachDataChannel))
	g.Set("mossOnMessage", js.FuncOf(onMessage))
	g.Set("mossStats", js.FuncOf(stats))
	g.Set("mossNodeStop", js.FuncOf(nodeStop))
	select {}
}

func nodeStart(_ js.Value, args []js.Value) any {
	if node != nil {
		return "already started"
	}
	if len(args) < 1 {
		return "missing mesh id"
	}
	meshID := args[0].String()
	var psk []byte
	if len(args) >= 2 && args[1].Truthy() {
		decoded, err := hex.DecodeString(args[1].String())
		if err != nil {
			return "invalid psk hex: " + err.Error()
		}
		psk = decoded
	}
	cfg := mesh.DefaultConfig()
	cfg.Trackers = nil // browser cannot reach UDP/HTTP trackers
	cfg.Telemetry = mesh.TelemetryConfig{Enabled: true, EpochSec: 300, KAnon: 3}

	n, err := mesh.NewNode(meshID, psk, cfg)
	if err != nil {
		return "create node: " + err.Error()
	}
	if code := n.StartWebRTC(); code != mesh.MOSS_OK {
		return "start node: error code"
	}
	node = n
	return ""
}

func subscribe(_ js.Value, args []js.Value) any {
	if node == nil || len(args) < 1 {
		return "node not started"
	}
	if code := node.Subscribe(args[0].String()); code != mesh.MOSS_OK {
		return "subscribe failed"
	}
	return ""
}

func publish(_ js.Value, args []js.Value) any {
	if node == nil || len(args) < 2 {
		return "node not started"
	}
	if code := node.Publish(args[0].String(), []byte(args[1].String())); code != mesh.MOSS_OK {
		return "publish failed"
	}
	return ""
}

// attachDataChannel(channel js, initiator bool, label string)
func attachDataChannel(_ js.Value, args []js.Value) any {
	if node == nil || len(args) < 2 {
		return "node not started"
	}
	label := ""
	if len(args) >= 3 {
		label = args[2].String()
	}
	node.AttachDataChannel(args[0], args[1].Bool(), label)
	return ""
}

// onMessage(fn) registers a JS callback invoked as fn(channel, senderHex, text)
// for every delivered pub/sub message.
func onMessage(_ js.Value, args []js.Value) any {
	if node == nil || len(args) < 1 {
		return "node not started"
	}
	cb := args[0]
	node.SetMessageCallback(func(channel string, sender [32]byte, data []byte) {
		cb.Invoke(channel, hex.EncodeToString(sender[:]), string(data))
	})
	return ""
}

func stats(_ js.Value, _ []js.Value) any {
	if node == nil {
		return "{}"
	}
	return node.StatsJSON()
}

func nodeStop(_ js.Value, _ []js.Value) any {
	if node != nil {
		node.Stop()
		node = nil
	}
	return ""
}

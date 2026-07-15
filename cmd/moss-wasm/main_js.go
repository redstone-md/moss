//go:build js && wasm

// Command moss-wasm is the browser-side verifier for the Moss network explorer.
//
// It compiles to WebAssembly and exposes pure, trust-minimizing primitives to
// JavaScript: hash-chain continuity verification, multi-gateway cross-checking,
// and the deterministic topology simulation. Because these run inside the
// browser, the explorer verifies the telemetry it renders instead of trusting
// any gateway it fetched from.
//
// It intentionally does NOT embed a full mesh node: peer transport (sockets) is
// unavailable in the browser. The explorer obtains snapshots over HTTP/SSE from
// one or more gateways and verifies them here.
package main

import (
	"encoding/json"
	"syscall/js"

	"github.com/redstone-md/moss/internal/observe"
)

func main() {
	js.Global().Set("mossVerifyChain", js.FuncOf(verifyChain))
	js.Global().Set("mossCrossCheck", js.FuncOf(crossCheck))
	js.Global().Set("mossSimulateTree", js.FuncOf(simulateTree))
	select {} // keep the Go runtime alive for the registered callbacks
}

// verifyChain(pointsJSON string) -> {"ok":bool,"error":string}
func verifyChain(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return result(false, "missing argument")
	}
	var points []observe.EpochPoint
	if err := json.Unmarshal([]byte(args[0].String()), &points); err != nil {
		return result(false, "invalid points json: "+err.Error())
	}
	if err := observe.VerifyContinuity(points); err != nil {
		return result(false, err.Error())
	}
	return result(true, "")
}

// crossCheck(byGatewayJSON string) -> {"<epoch>":bool,...}
func crossCheck(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return "{}"
	}
	var byGateway map[string][]observe.EpochPoint
	if err := json.Unmarshal([]byte(args[0].String()), &byGateway); err != nil {
		return "{}"
	}
	out, _ := json.Marshal(observe.CrossCheck(byGateway))
	return string(out)
}

// simulateTree(paramsJSON string) -> SimNode[] JSON
func simulateTree(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return "[]"
	}
	var p observe.TreeParams
	if err := json.Unmarshal([]byte(args[0].String()), &p); err != nil {
		return "[]"
	}
	out, _ := json.Marshal(observe.SimulateTree(p))
	return string(out)
}

func result(ok bool, errMsg string) string {
	out, _ := json.Marshal(map[string]any{"ok": ok, "error": errMsg})
	return string(out)
}

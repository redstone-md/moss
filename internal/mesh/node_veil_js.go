//go:build js

package mesh

import "context"

// startVeilBearer is a no-op on js/wasm: the Veil "Reality" bearer pulls
// in uTLS + crypto/tls and only masks the native TCP relay leg, which the
// browser build does not run. Browser peers use the WebRTC bearer instead
// (node_webrtc_js.go).
func (n *Node) startVeilBearer(ctx context.Context) {}

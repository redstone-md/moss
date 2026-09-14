package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef int64_t MossHandle;

typedef void (*MossMessageCallback)(const char* channel,
                                     const uint8_t* sender_id,
                                     const uint8_t* data, uint32_t len);

typedef void (*MossEventCallback)(int32_t event_type,
                                   const char* detail_json);

typedef double (*MossScoringCallback)(const uint8_t* peer_id,
                                       double base_score);
typedef uint32_t (*MossKeyStoreLoadCallback)(uint8_t* buffer,
                                             uint32_t capacity);
typedef void (*MossKeyStoreSaveCallback)(const uint8_t* data,
                                         uint32_t len);

static inline void callMessageCallback(MossMessageCallback cb,
                                       const char* channel,
                                       const uint8_t* sender_id,
                                       const uint8_t* data,
                                       uint32_t len) {
  cb(channel, sender_id, data, len);
}

static inline void callEventCallback(MossEventCallback cb,
                                     int32_t event_type,
                                     const char* detail_json) {
  cb(event_type, detail_json);
}

static inline double callScoringCallback(MossScoringCallback cb,
                                         const uint8_t* peer_id,
                                         double base_score) {
  return cb(peer_id, base_score);
}

static inline uint32_t callKeyStoreLoad(MossKeyStoreLoadCallback cb,
                                        uint8_t* buffer,
                                        uint32_t capacity) {
  return cb(buffer, capacity);
}

static inline void callKeyStoreSave(MossKeyStoreSaveCallback cb,
                                    const uint8_t* data,
                                    uint32_t len) {
  cb(data, len);
}

typedef void (*MossRelayCallback)(const uint8_t* sender_id,
                                  const uint8_t* data,
                                  uint32_t length);

static inline void callRelayCallback(MossRelayCallback cb,
                                     const uint8_t* sender_id,
                                     const uint8_t* data,
                                     uint32_t length) {
    cb(sender_id, data, length);
}

typedef void (*MossPacketCallback)(const uint8_t* sender_id,
                                   const uint8_t* data,
                                   uint32_t length);

static inline void callPacketCallback(MossPacketCallback cb,
                                     const uint8_t* sender_id,
                                     const uint8_t* data,
                                     uint32_t length) {
    cb(sender_id, data, length);
}

typedef void (*MossStreamCallback)(const char* peer_id,
                                   const uint8_t* data,
                                   uint32_t length);

static inline void callStreamCallback(MossStreamCallback cb,
                                     const char* peer_id,
                                     const uint8_t* data,
                                     uint32_t length) {
    cb(peer_id, data, length);
}

typedef void (*MossAsyncCompletionCallback)(uint64_t job_id,
                                             int32_t result);

static inline void callAsyncCompletionCallback(MossAsyncCompletionCallback cb,
                                                uint64_t job_id,
                                                int32_t result) {
    cb(job_id, result);
}
*/
import "C"

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/transport"
)

var (
	handleCounter   atomic.Int64
	asyncJobCounter atomic.Uint64
	registryMu      sync.RWMutex
	registry        = make(map[int64]*mesh.Node)
	// ffiStates carries the FFI-layer per-handle dispatch state the relayed-
	// stream fallback needs: the app's packet callback (so the chain can
	// forward non-stream payloads) and the stream callbacks that ride the
	// relay path. Kept beside registry under the same RWMutex so Moss_Stop
	// tears both down atomically with the handle entry.
	ffiStates    = make(map[int64]*ffiState)
	keystoreMu   sync.RWMutex
	keystoreLoad C.MossKeyStoreLoadCallback
	keystoreSave C.MossKeyStoreSaveCallback

	loadIdentityBytes = func() ([]byte, error) {
		keystoreMu.RLock()
		load := keystoreLoad
		keystoreMu.RUnlock()
		if load == nil {
			return nil, nil
		}
		size := C.callKeyStoreLoad(load, nil, 0)
		if size == 0 {
			return nil, nil
		}
		if err := validateKeystoreProbeSize(uint32(size)); err != nil {
			return nil, err
		}
		buffer := C.malloc(C.size_t(size))
		if buffer == nil {
			return nil, errors.New("keystore load allocation failed")
		}
		defer C.free(buffer)
		read := C.callKeyStoreLoad(load, (*C.uint8_t)(buffer), size)
		if read == 0 {
			return nil, nil
		}
		if err := validateKeystoreReadSize(uint32(read), uint32(size)); err != nil {
			return nil, err
		}
		return C.GoBytes(buffer, C.int(read)), nil
	}
	saveIdentityBytes = func(raw []byte) error {
		keystoreMu.RLock()
		save := keystoreSave
		keystoreMu.RUnlock()
		if save == nil || len(raw) == 0 {
			return nil
		}
		ptr := C.CBytes(raw)
		defer C.free(ptr)
		C.callKeyStoreSave(save, (*C.uint8_t)(ptr), C.uint32_t(len(raw)))
		return nil
	}
)

// buildVersion is stamped at link time by the release workflow
// (-ldflags "-X main.buildVersion=v0.8.17"). A library built any other way says
// so rather than claiming a version it cannot know.
var buildVersion = "dev"

const relayFFITimeout = 5 * time.Second

func validateKeystoreProbeSize(size uint32) error {
	if size > uint32(mcrypto.IdentityEncodedSize) {
		return errors.New("keystore load size exceeds identity encoding size")
	}
	return nil
}

func validateKeystoreReadSize(read, capacity uint32) error {
	if read > capacity {
		return errors.New("keystore load read exceeds buffer capacity")
	}
	if read > uint32(mcrypto.IdentityEncodedSize) {
		return errors.New("keystore load read exceeds identity encoding size")
	}
	return nil
}

func main() {}

//export Moss_Init
func Moss_Init(meshID *C.char, psk *C.uint8_t, config *C.char) C.MossHandle {
	if meshID == nil {
		return C.MossHandle(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	return C.MossHandle(initNode(C.GoString(meshID), pskBytes(psk), cString(config)))
}

//export Moss_Start
func Moss_Start(handle C.MossHandle) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	return C.int32_t(node.Start())
}

//export Moss_Stop
func Moss_Stop(handle C.MossHandle) C.int32_t {
	registryMu.Lock()
	node, ok := registry[int64(handle)]
	if ok {
		delete(registry, int64(handle))
		delete(ffiStates, int64(handle))
	}
	registryMu.Unlock()
	if !ok {
		return C.int32_t(mesh.MOSS_ERR_INVALID_HANDLE)
	}
	return C.int32_t(node.Stop())
}

//export Moss_Subscribe
func Moss_Subscribe(handle C.MossHandle, channel *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	return C.int32_t(node.Subscribe(C.GoString(channel)))
}

// Moss_JoinRoom, Moss_SubscribeRoom, Moss_PublishRoom and Moss_UnsubscribeRoom
// let ONE node serve several rooms. A host that gave each conversation its own
// room had to start a node per conversation, and node identity is per process,
// so every one of those nodes presented the same peer id from a different port
// — remote peers keep one session per identity and closed the rest on arrival.
//
// The room-less calls above are unchanged and still mean "this node's own
// room", so a host that does not care never sees any of this. Callers older
// than this build simply lack the symbols; treat a missing one as "this moss
// cannot share a node".

//export Moss_JoinRoom
func Moss_JoinRoom(handle C.MossHandle, meshID *C.char, psk *C.uint8_t, pskLength C.uint32_t) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if meshID == nil {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	var secret []byte
	if psk != nil && pskLength > 0 {
		secret = bytesFromPointer(psk, int(pskLength))
	}
	return C.int32_t(node.JoinRoom(C.GoString(meshID), secret))
}

//export Moss_LeaveRoom
func Moss_LeaveRoom(handle C.MossHandle, meshID *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if meshID == nil {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	return C.int32_t(node.LeaveRoom(C.GoString(meshID)))
}

//export Moss_SubscribeRoom
func Moss_SubscribeRoom(handle C.MossHandle, meshID *C.char, channel *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	return C.int32_t(node.SubscribeRoom(C.GoString(meshID), C.GoString(channel)))
}

//export Moss_UnsubscribeRoom
func Moss_UnsubscribeRoom(handle C.MossHandle, meshID *C.char, channel *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	return C.int32_t(node.UnsubscribeRoom(C.GoString(meshID), C.GoString(channel)))
}

//export Moss_PublishRoom
func Moss_PublishRoom(handle C.MossHandle, meshID *C.char, channel *C.char, data *C.uint8_t, length C.uint32_t) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if code := validatePublishPayloadPointer(unsafe.Pointer(data), uint32(length), node.MaxMessageSizeBytes()); code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	payload := bytesFromPointer(data, int(length))
	return C.int32_t(node.PublishRoom(C.GoString(meshID), C.GoString(channel), payload))
}

//export Moss_Connect
func Moss_Connect(handle C.MossHandle, addr *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	return C.int32_t(node.Connect(C.GoString(addr)))
}

//export Moss_ConnectToPeer
func Moss_ConnectToPeer(handle C.MossHandle, peerID *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if peerID == nil {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	return C.int32_t(node.ConnectToPeer(C.GoString(peerID)))
}

//export Moss_Unsubscribe
func Moss_Unsubscribe(handle C.MossHandle, channel *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	return C.int32_t(node.Unsubscribe(C.GoString(channel)))
}

//export Moss_Publish
func Moss_Publish(handle C.MossHandle, channel *C.char, data *C.uint8_t, length C.uint32_t) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if code := validatePublishPayloadPointer(unsafe.Pointer(data), uint32(length), node.MaxMessageSizeBytes()); code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	payload := bytesFromPointer(data, int(length))
	return C.int32_t(node.Publish(C.GoString(channel), payload))
}

//export Moss_SetCallback
func Moss_SetCallback(handle C.MossHandle, cb C.MossMessageCallback) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if cb == nil {
		node.SetMessageCallback(nil)
		return C.int32_t(mesh.MOSS_OK)
	}
	node.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		channelC := C.CString(channel)
		senderC := C.CBytes(senderID[:])
		dataC := C.CBytes(data)
		C.callMessageCallback(cb, channelC, (*C.uint8_t)(senderC), (*C.uint8_t)(dataC), C.uint32_t(len(data)))
		C.free(unsafe.Pointer(channelC))
		C.free(senderC)
		C.free(dataC)
	})
	return C.int32_t(mesh.MOSS_OK)
}

//export Moss_SetEventCallback
func Moss_SetEventCallback(handle C.MossHandle, cb C.MossEventCallback) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if cb == nil {
		node.SetEventCallback(nil)
		return C.int32_t(mesh.MOSS_OK)
	}
	node.SetEventCallback(func(eventType int32, detailJSON string) {
		detailC := C.CString(detailJSON)
		C.callEventCallback(cb, C.int32_t(eventType), detailC)
		C.free(unsafe.Pointer(detailC))
	})
	return C.int32_t(mesh.MOSS_OK)
}

//export Moss_SetScoringCallback
func Moss_SetScoringCallback(handle C.MossHandle, cb C.MossScoringCallback) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if cb == nil {
		node.SetScoringCallback(nil)
		return C.int32_t(mesh.MOSS_OK)
	}
	node.SetScoringCallback(func(peerID [32]byte, baseScore float64) float64 {
		peerC := C.CBytes(peerID[:])
		defer C.free(peerC)
		return float64(C.callScoringCallback(cb, (*C.uint8_t)(peerC), C.double(baseScore)))
	})
	return C.int32_t(mesh.MOSS_OK)
}

//export Moss_RelaySendTo
func Moss_RelaySendTo(handle C.MossHandle, targetPeerID *C.char, data *C.uint8_t, length C.int32_t) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if targetPeerID == nil || length < 0 {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	payload := bytesFromPointer(data, int(length))
	if err := node.RelaySendTo(C.GoString(targetPeerID), payload, relayFFITimeout); err != nil {
		return C.int32_t(mesh.MOSS_ERR_RELAY_FAILED)
	}
	return C.int32_t(mesh.MOSS_OK)
}

//export Moss_SetRelayCallback
func Moss_SetRelayCallback(handle C.MossHandle, cb C.MossRelayCallback) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if cb == nil {
		node.SetRelayCallback(nil)
		return C.int32_t(mesh.MOSS_OK)
	}
	node.SetRelayCallback(func(senderID [32]byte, data []byte) {
		senderC := C.CBytes(senderID[:])
		dataC := C.CBytes(data)
		C.callRelayCallback(cb, (*C.uint8_t)(senderC), (*C.uint8_t)(dataC), C.uint32_t(len(data)))
		C.free(senderC)
		C.free(dataC)
	})
	return C.int32_t(mesh.MOSS_OK)
}

// Moss_SendToPeer delivers a directed payload to one peer: over the direct
// session when one exists, else via the relay path with the same 5-second
// budget as Moss_RelaySendTo. The receiver sees it through the packet
// callback (Moss_SetPacketCallback), which also catches relayed payloads.
// Size gate matches Publish's (Security.MaxMessageSizeBytes).
//
//export Moss_SendToPeer
func Moss_SendToPeer(handle C.MossHandle, targetPeerID *C.char, data *C.uint8_t, length C.int32_t) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if targetPeerID == nil || length < 0 {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	if code := validatePublishPayloadPointer(unsafe.Pointer(data), uint32(length), node.MaxMessageSizeBytes()); code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	payload := bytesFromPointer(data, int(length))
	if err := node.SendToPeer(C.GoString(targetPeerID), payload, relayFFITimeout); err != nil {
		return C.int32_t(mesh.MOSS_ERR_RELAY_FAILED)
	}
	return C.int32_t(mesh.MOSS_OK)
}

// Moss_SendToPeerAsync is the non-blocking form of Moss_SendToPeer: the
// same routing (direct session first, relay fallback) and the same 5-second
// relay budget, but the send runs on a detached goroutine and the outcome
// is reported through the completion callback instead of the return value.
// The payload is copied before the call returns, so the caller may free the
// buffer immediately. Returns a job ID (never 0, never reused) or 0 when
// the call is refused up front (unknown handle, NULL peer, negative
// length, oversize payload, or NULL callback) — a refusal never fires the
// callback.
//
// The completion fires exactly once, from a Go runtime thread, possibly
// concurrent with other callbacks; its result code is MOSS_OK (0) or
// MOSS_ERR_RELAY_FAILED (-11). Do not call Moss_Stop from inside the
// callback. Completion is not guaranteed after Moss_Stop: if the handle is
// gone when the send resolves, the callback is dropped rather than invoked
// on a torn-down host.
//
//export Moss_SendToPeerAsync
func Moss_SendToPeerAsync(handle C.MossHandle, targetPeerID *C.char, data *C.uint8_t, length C.int32_t, cb C.MossAsyncCompletionCallback) C.uint64_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK || cb == nil {
		return 0
	}
	if targetPeerID == nil || length < 0 {
		return 0
	}
	if code := validatePublishPayloadPointer(unsafe.Pointer(data), uint32(length), node.MaxMessageSizeBytes()); code != mesh.MOSS_OK {
		return 0
	}
	payload := bytesFromPointer(data, int(length))
	return C.uint64_t(startAsyncSend(node, C.GoString(targetPeerID), payload, asyncDeliver(int64(handle), cb)))
}

// Moss_RelaySendToAsync is the non-blocking form of Moss_RelaySendTo: the
// explicit relay path with the same 5-second budget, run on a detached
// goroutine with the outcome reported through the completion callback. The
// same contract as Moss_SendToPeerAsync applies: payload copied up front,
// job IDs never 0 or reused, completion exactly once from a Go runtime
// thread, dropped (not fired) when the handle was stopped in between.
//
//export Moss_RelaySendToAsync
func Moss_RelaySendToAsync(handle C.MossHandle, targetPeerID *C.char, data *C.uint8_t, length C.int32_t, cb C.MossAsyncCompletionCallback) C.uint64_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK || cb == nil {
		return 0
	}
	if targetPeerID == nil || length < 0 {
		return 0
	}
	if code := validatePublishPayloadPointer(unsafe.Pointer(data), uint32(length), node.MaxMessageSizeBytes()); code != mesh.MOSS_OK {
		return 0
	}
	payload := bytesFromPointer(data, int(length))
	return C.uint64_t(startAsyncRelaySend(node, C.GoString(targetPeerID), payload, asyncDeliver(int64(handle), cb)))
}

// Moss_PeerRTT returns the last measured round-trip time to a peer in
// nanoseconds — the same value peer selection sorts by. Zero when the peer
// is unknown or not yet probed.
//
//export Moss_PeerRTT
func Moss_PeerRTT(handle C.MossHandle, peerID *C.char) C.int64_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return 0
	}
	if peerID == nil {
		return 0
	}
	return C.int64_t(node.PeerRTT(C.GoString(peerID)).Nanoseconds())
}

// Moss_SetPacketCallback registers the unified sink for directed payloads:
// it receives both direct packets (Moss_SendToPeer over a direct session)
// and raw relayed payloads — plus relayed stream payloads only when no
// stream handler is registered for their stream (see Moss_OnStream).
// The legacy relay callback (Moss_SetRelayCallback) still fires for relayed
// payloads while no packet callback is registered. Pass NULL to clear.
//
// On a handle that has ever registered a stream handler this call also
// installs the FFI dispatch chain (see installFFIDispatchChain); ordering
// with Moss_OnStream does not matter, both entries land in the same chain.
//
//export Moss_SetPacketCallback
func Moss_SetPacketCallback(handle C.MossHandle, cb C.MossPacketCallback) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	registryMu.Lock()
	state, ok := ffiStates[int64(handle)]
	if ok {
		state.appPacketCb = cb
	}
	registryMu.Unlock()
	if !ok {
		return C.int32_t(mesh.MOSS_ERR_INVALID_HANDLE)
	}
	if cb == nil && !ffiDispatchChainActive(int64(handle)) {
		// No stream handlers either: clearing restores the node's slot to
		// its pristine state, keeping the legacy relay callback path alive.
		node.SetPacketCallback(nil)
		return C.int32_t(mesh.MOSS_OK)
	}
	installFFIDispatchChain(int64(handle), node)
	return C.int32_t(mesh.MOSS_OK)
}

// ffiDispatchChainActive reports whether the chain serves any purpose: the
// relayed-stream fallback or the app packet callback.
func ffiDispatchChainActive(handle int64) bool {
	registryMu.RLock()
	defer registryMu.RUnlock()
	state := ffiStates[handle]
	if state == nil {
		return false
	}
	if state.appPacketCb != nil {
		return true
	}
	for _, cb := range state.streamCbs {
		if cb != nil {
			return true
		}
	}
	return false
}

// Moss_OpenStream makes sure a reader goroutine drains streamID on the direct
// session with peerID, dialing the peer first if unknown. Stream 0 (raw) and
// 1 (gossip) are reserved by the transport and rejected as invalid config.
// A relayed peer succeeds: nothing needs pre-opening there — the relay
// session opens lazily on the first Moss_SendStream fallback (see
// Moss_SendStream) — and the stream handler registered with Moss_OnStream
// catches its payloads from either path.
//
//export Moss_OpenStream
func Moss_OpenStream(handle C.MossHandle, peerID *C.char, streamID C.uint32_t) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if peerID == nil {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	code = node.OpenStream(C.GoString(peerID), transport.StreamID(streamID))
	if code == mesh.MOSS_ERR_RELAY_FAILED {
		// Relayed peer: the transport mux cannot carry the stream, but the
		// FFI fallback (Moss_SendStream) can. Registering a handler with
		// Moss_OnStream is what makes the relayed side receive; this call
		// has nothing else to prepare.
		return C.int32_t(mesh.MOSS_OK)
	}
	return C.int32_t(code)
}

// Moss_SendStream writes data to streamID on the direct session with peerID.
// Fast path: no discovery, no dialing — a hot loop must not stall on
// overlays. Use Moss_OpenStream first for peers you have not connected to.
// Size gate matches Publish's. A relayed peer falls back to the relay path:
// the payload is wrapped with the stream fallback header (magic + streamID)
// and delivered via RelaySendTo; the receiving FFI layer unwraps it and
// dispatches to the Moss_OnStream handler for the stream. Returns
// MOSS_ERR_RELAY_FAILED (-11) only when neither path could deliver.
//
//export Moss_SendStream
func Moss_SendStream(handle C.MossHandle, peerID *C.char, streamID C.uint32_t, data *C.uint8_t, length C.uint32_t) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if peerID == nil {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	if code := validatePublishPayloadPointer(unsafe.Pointer(data), uint32(uint32(length)), node.MaxMessageSizeBytes()); code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	payload := bytesFromPointer(data, int(length))
	code = node.SendStream(C.GoString(peerID), transport.StreamID(streamID), payload)
	if code != mesh.MOSS_ERR_RELAY_FAILED {
		return C.int32_t(code)
	}
	// Relayed peer: wrap and ride a relayed DM. The wrapped size already
	// passed the MaxMessageSizeBytes gate above, which is stricter than
	// the relay path's own payload cap, so no further size check is needed.
	wrapped := wrapStreamFallbackPayload(uint32(streamID), payload)
	if err := node.RelaySendTo(C.GoString(peerID), wrapped, relayFFITimeout); err != nil {
		return C.int32_t(mesh.MOSS_ERR_RELAY_FAILED)
	}
	return C.int32_t(mesh.MOSS_OK)
}

// Moss_OnStream registers the handler for streamID; packets arrive on it
// from the moment of registration (per-peer readers spawn as peers connect
// or send). Register before sending traffic: the handler is snapshotted
// when a reader spawns, so re-registering replaces the entry for future
// readers but does not retro-fit already-running ones. The runtime has no
// unregister — re-register with a no-op handler instead of expecting to
// clear it. Stream 0 (raw) and 1 (gossip) are reserved; NULL is rejected.
//
// The handler is registered twice: node.OnStream carries the direct-session
// path, and the FFI state map carries the relayed path — a relayed sender's
// payloads arrive as wrapped relayed DMs, which the FFI dispatch chain (see
// installFFIDispatchChain) unwraps and routes to the same C callback with
// the same shape (hex peer id string + payload bytes). Installing the chain
// here means the receiving side works even when the host never registered a
// packet callback.
//
//export Moss_OnStream
func Moss_OnStream(handle C.MossHandle, streamID C.uint32_t, cb C.MossStreamCallback) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if cb == nil {
		return C.int32_t(node.OnStream(transport.StreamID(streamID), nil))
	}
	code = node.OnStream(transport.StreamID(streamID), func(peerID string, data []byte) {
		peerC := C.CString(peerID)
		dataC := C.CBytes(data)
		C.callStreamCallback(cb, peerC, (*C.uint8_t)(dataC), C.uint32_t(len(data)))
		C.free(unsafe.Pointer(peerC))
		C.free(dataC)
	})
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	registryMu.Lock()
	if state := ffiStates[int64(handle)]; state != nil {
		state.streamCbs[uint32(streamID)] = cb
	}
	registryMu.Unlock()
	installFFIDispatchChain(int64(handle), node)
	return C.int32_t(mesh.MOSS_OK)
}

//export Moss_SetKeyStore
func Moss_SetKeyStore(load C.MossKeyStoreLoadCallback, save C.MossKeyStoreSaveCallback) C.int32_t {
	keystoreMu.Lock()
	defer keystoreMu.Unlock()
	keystoreLoad = load
	keystoreSave = save
	return C.int32_t(mesh.MOSS_OK)
}

//export Moss_GetMeshInfo
func Moss_GetMeshInfo(handle C.MossHandle) *C.char {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return nil
	}
	return C.CString(node.MeshInfoJSON())
}

//export Moss_GetPublicKey
func Moss_GetPublicKey(handle C.MossHandle) *C.uint8_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return nil
	}
	key := node.PublicKey()
	ptr := C.CBytes(key[:])
	return (*C.uint8_t)(ptr)
}

//export Moss_GetNATType
func Moss_GetNATType(handle C.MossHandle) *C.char {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return nil
	}
	return C.CString(node.NATType())
}

// Moss_LastError returns the human-readable reason for the most recent operation
// on this handle that failed with a coarse error code — chiefly the underlying
// OS bind error behind MOSS_ERR_LISTEN_FAILED (-13), which is what surfaces when
// Go's netpoller cannot bind sockets under an older Wine/Proton. Returns an
// allocated C string (free with Moss_Free), or NULL if the handle is unknown.
// Call it before Moss_Stop, which removes the handle from the registry.
//
// Moss_Version returns the version this library was built at, as a newly
// allocated C string (free with Moss_Free). Release builds carry their tag;
// anything else reports "dev".
//
// A host loads moss by path at runtime, so nothing stops an old library from
// sitting next to a new host — and the symptoms of that are transport bugs the
// host cannot diagnose. This lets a host say which library it got instead of
// guessing. Callers must treat a missing symbol as "older than v0.8.17".
//
//export Moss_Version
func Moss_Version() *C.char {
	return C.CString(buildVersion)
}

//export Moss_LastError
func Moss_LastError(handle C.MossHandle) *C.char {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return nil
	}
	return C.CString(node.LastError())
}

// Moss_EnableAxiom turns on the opt-in Axiom error/log sink. token is an
// ingest-only Axiom token, dataset the target dataset, endpoint the Axiom base
// URL ("" → cloud default https://api.axiom.co), and service a host identifier
// (e.g. "gse-4576510", "mosh-0.6.5"). A node ships nothing until this is called.
//
//export Moss_EnableAxiom
func Moss_EnableAxiom(handle C.MossHandle, token *C.char, dataset *C.char, endpoint *C.char, service *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	node.EnableAxiom(C.GoString(token), C.GoString(dataset), C.GoString(endpoint), C.GoString(service))
	return C.int32_t(mesh.MOSS_OK)
}

// Moss_LogEvent ships a structured event through the Axiom sink (no-op when
// disabled). level is "error"|"warn"|"info", kind a short slug, message free
// text, and fieldsJSON an optional JSON object of extra context ("" for none).
//
//export Moss_LogEvent
func Moss_LogEvent(handle C.MossHandle, level *C.char, kind *C.char, message *C.char, fieldsJSON *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	var fields map[string]any
	if fj := C.GoString(fieldsJSON); fj != "" {
		_ = json.Unmarshal([]byte(fj), &fields)
	}
	node.LogEvent(C.GoString(level), C.GoString(kind), C.GoString(message), fields)
	return C.int32_t(mesh.MOSS_OK)
}

//export Moss_GetNetworkStats
func Moss_GetNetworkStats(handle C.MossHandle) *C.char {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return nil
	}
	stats := node.StatsJSON()
	if stats == "" {
		// Telemetry is disabled for this node; return an empty JSON object so
		// callers get valid, releasable JSON rather than NULL.
		stats = "{}"
	}
	return C.CString(stats)
}

//export Moss_Free
func Moss_Free(ptr unsafe.Pointer) {
	if ptr != nil {
		C.free(ptr)
	}
}

func getNode(handle int64) (*mesh.Node, int32) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	node, ok := registry[handle]
	if !ok {
		return nil, mesh.MOSS_ERR_INVALID_HANDLE
	}
	return node, mesh.MOSS_OK
}

func cString(value *C.char) string {
	if value == nil {
		return ""
	}
	return C.GoString(value)
}

func pskBytes(psk *C.uint8_t) []byte {
	if psk == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(psk), 32)
}

func bytesFromPointer(data *C.uint8_t, length int) []byte {
	if data == nil || length == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(data), C.int(length))
}

func validatePublishPayloadPointer(data unsafe.Pointer, length uint32, maxLength int) int32 {
	if length == 0 {
		return mesh.MOSS_OK
	}
	if data == nil {
		return mesh.MOSS_ERR_CONFIG_INVALID
	}
	if length > uint32(maxLength) || length > uint32(math.MaxInt32) {
		return mesh.MOSS_ERR_MESSAGE_TOO_LARGE
	}
	return mesh.MOSS_OK
}

// ---------------------------------------------------------------------
// Streams over relayed peers: fallback wire format + dispatch chain.
//
// The transport mux only carries streams on a direct Noise session; a relayed
// peer has no session, so node.SendStream/OpenStream answer -11 for it. The
// FFI layer turns that refusal into a fallback: the stream payload rides a
// relayed DM (node.RelaySendTo) under a tiny additive header
//
//	magic (4 bytes: 'M','S','s','1') || streamID (4 bytes, big-endian) || data
//
// and the receiving side — which sees relayed DMs through the node's packet
// callback — unwraps the header and dispatches to the FFI stream callback
// registered for that streamID. Non-wrapped payloads forward to the app's
// packet callback untouched, so Moss_SendToPeer semantics are preserved.

// streamFallbackMagic prefixes every wrapped stream payload sent over the
// relay path. It is an 8-byte-frame header reservation, not a versioned
// protocol: an application payload whose first four bytes happen to collide
// is indistinguishable and gets misdispatched, so apps with binary protocols
// that can start with these bytes should avoid sending them as plain relayed
// DMs while stream fallback is in play.
const streamFallbackMagic = "MSs1"

// streamFallbackHeaderLen is the size of magic + big-endian streamID.
const streamFallbackHeaderLen = 8

// wrapStreamFallbackPayload frames payload for the relay path.
func wrapStreamFallbackPayload(streamID uint32, payload []byte) []byte {
	wrapped := make([]byte, streamFallbackHeaderLen+len(payload))
	copy(wrapped, streamFallbackMagic)
	wrapped[4] = byte(streamID >> 24)
	wrapped[5] = byte(streamID >> 16)
	wrapped[6] = byte(streamID >> 8)
	wrapped[7] = byte(streamID)
	copy(wrapped[streamFallbackHeaderLen:], payload)
	return wrapped
}

// unwrapStreamFallbackPayload extracts the streamID and payload from a
// relayed DM, reporting whether the payload carries the stream fallback
// header at all.
func unwrapStreamFallbackPayload(data []byte) (streamID uint32, payload []byte, ok bool) {
	if len(data) < streamFallbackHeaderLen || string(data[:4]) != streamFallbackMagic {
		return 0, nil, false
	}
	streamID = uint32(data[4])<<24 | uint32(data[5])<<16 | uint32(data[6])<<8 | uint32(data[7])
	return streamID, data[streamFallbackHeaderLen:], true
}

// ffiState is the FFI-layer per-handle dispatch state for the stream
// fallback. streamCbs holds the C callbacks registered via Moss_OnStream so
// the relay path can dispatch wrapped payloads to them with the same shape
// the direct path (node.OnStream) delivers; appPacketCb holds the host's
// packet callback so non-wrapped payloads keep flowing to the application.
// Both are read on the node's dispatch goroutine and written under
// registryMu, so every read snapshots under the same lock.
type ffiState struct {
	streamCbs   map[uint32]C.MossStreamCallback
	appPacketCb C.MossPacketCallback
}

// ffiStateFor snapshots the per-handle dispatch state.
func ffiStateFor(handle int64) *ffiState {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return ffiStates[handle]
}

// installFFIDispatchChain makes the FFI unwrap chain the node's packet
// callback. Idempotent: once installed it stays for the handle's lifetime
// (Moss_Stop removes the whole handle), because the chain reads its
// callbacks from ffiState, not from the closure — later Moss_OnStream /
// Moss_SetPacketCallback calls only mutate ffiState.
//
// The chain replaces the node-level callback slot, so the legacy relay
// callback (Moss_SetRelayCallback) stops firing once a stream handler is
// registered: dispatchLoop prefers the packet callback. Mixing
// Moss_SetRelayCallback with relayed streams on one handle is unsupported.
func installFFIDispatchChain(handle int64, node *mesh.Node) {
	node.SetPacketCallback(func(senderID [32]byte, data []byte) {
		state := ffiStateFor(handle)
		if state == nil {
			// Handle torn down (Moss_Stop); nothing to dispatch to.
			return
		}
		if streamID, payload, ok := unwrapStreamFallbackPayload(data); ok {
			registryMu.RLock()
			cb := state.streamCbs[streamID]
			registryMu.RUnlock()
			if cb == nil {
				// A wrapped payload for a stream nobody listens on is
				// indistinguishable from an app payload that happens to
				// carry the magic; without a handler there is nothing
				// sensible to do with it either way — drop it.
				return
			}
			peerC := C.CString(hex.EncodeToString(senderID[:]))
			dataC := C.CBytes(payload)
			C.callStreamCallback(cb, peerC, (*C.uint8_t)(dataC), C.uint32_t(len(payload)))
			C.free(unsafe.Pointer(peerC))
			C.free(dataC)
			return
		}
		registryMu.RLock()
		cb := state.appPacketCb
		registryMu.RUnlock()
		if cb == nil {
			return
		}
		senderC := C.CBytes(senderID[:])
		dataC := C.CBytes(data)
		C.callPacketCallback(cb, (*C.uint8_t)(senderC), (*C.uint8_t)(dataC), C.uint32_t(len(data)))
		C.free(senderC)
		C.free(dataC)
	})
}

// ---------------------------------------------------------------------
// Async directed sends.
//
// Moss_SendToPeerAsync / Moss_RelaySendToAsync hand the blocking work to a
// detached goroutine and report the outcome through a completion callback,
// so a host UI thread never stalls on the relay path's 5-second budget. The
// payload is copied before the goroutine spawns, so the caller may free the
// buffer immediately; the job ID is unique per call and never 0. Completion
// fires exactly once, from a Go runtime thread, possibly concurrent with
// other callbacks. If the host calls Moss_Stop before the send resolves, the
// handle check fails and the callback is dropped rather than invoked on a
// torn-down host.

// asyncOutcomeCode maps a send error to the coarse code the completion
// callback reports.
func asyncOutcomeCode(err error) int32 {
	if err == nil {
		return mesh.MOSS_OK
	}
	return mesh.MOSS_ERR_RELAY_FAILED
}

// startAsyncSend is the shared body of Moss_SendToPeerAsync: it validates the
// arguments, reserves a job ID, and spawns a detached one-shot goroutine that
// runs the send with the same 5-second budget as the synchronous call, then
// reports the outcome through deliver exactly once. Returns the job ID, or 0
// when the arguments refuse the job (nothing spawned, deliver never called).
// Splitting deliver from the send lets tests drive this without cgo plumbing
// and lets the wrappers pre-validate the C arguments first.
func startAsyncSend(node *mesh.Node, peerID string, payload []byte, deliver func(jobID uint64, code int32)) uint64 {
	if node == nil || peerID == "" || deliver == nil {
		return 0
	}
	jobID := asyncJobCounter.Add(1)
	go func() {
		err := node.SendToPeer(peerID, payload, relayFFITimeout)
		deliver(jobID, asyncOutcomeCode(err))
	}()
	return jobID
}

// startAsyncRelaySend mirrors startAsyncSend for the explicit relay path.
func startAsyncRelaySend(node *mesh.Node, peerID string, payload []byte, deliver func(jobID uint64, code int32)) uint64 {
	if node == nil || peerID == "" || deliver == nil {
		return 0
	}
	jobID := asyncJobCounter.Add(1)
	go func() {
		err := node.RelaySendTo(peerID, payload, relayFFITimeout)
		deliver(jobID, asyncOutcomeCode(err))
	}()
	return jobID
}

// asyncDeliver returns the deliver closure the exported wrappers use: it
// handle-checks before invoking the C callback, so a completion for a handle
// the host already stopped is dropped instead of calling into freed memory.
func asyncDeliver(handle int64, cb C.MossAsyncCompletionCallback) func(jobID uint64, code int32) {
	if cb == nil {
		return nil
	}
	return func(jobID uint64, code int32) {
		if _, lookup := getNode(handle); lookup != mesh.MOSS_OK {
			return
		}
		C.callAsyncCompletionCallback(cb, C.uint64_t(jobID), C.int32_t(code))
	}
}

// axiomConfig mirrors the Axiom keys of the public Go config. The Go API turns
// the sink on inside moss.NewNode, but the FFI builds a node straight from
// mesh, so it must honour the same keys itself — and did not. Every host that
// set them (both desktop clients do, believing "moss ships nothing unless these
// are set") therefore shipped nothing at all: mesh.ParseConfig carries no Axiom
// fields, so the keys were dropped without a word and the entire client fleet
// stayed invisible while the Go-native spores reported fine.
type axiomConfig struct {
	Token    string `json:"axiom_token"`
	Dataset  string `json:"axiom_dataset"`
	Endpoint string `json:"axiom_endpoint"`
	Service  string `json:"axiom_service"`
}

// parseAxiomConfig reports the sink settings and whether they are usable at
// all: shipping needs both a token and a dataset.
func parseAxiomConfig(raw string) (axiomConfig, bool) {
	var ax axiomConfig
	if raw == "" {
		return ax, false
	}
	if err := json.Unmarshal([]byte(raw), &ax); err != nil {
		return ax, false
	}
	return ax, ax.Token != "" && ax.Dataset != ""
}

func initNode(meshID string, psk []byte, config string) int64 {
	cfg, err := mesh.ParseConfig(config)
	if err != nil {
		return int64(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	identity, err := resolveIdentity()
	if err != nil {
		return int64(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	node, err := mesh.NewNodeWithIdentity(meshID, psk, cfg, identity)
	if err != nil {
		return int64(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	// Enable before the caller can Start, so a bind failure on the very first
	// start — the Wine/Proton case this sink exists for — is reported instead of
	// dying with the node.
	if ax, ok := parseAxiomConfig(config); ok {
		node.EnableAxiom(ax.Token, ax.Dataset, ax.Endpoint, ax.Service)
	}
	handle := handleCounter.Add(1)
	registryMu.Lock()
	registry[handle] = node
	ffiStates[handle] = &ffiState{streamCbs: make(map[uint32]C.MossStreamCallback)}
	registryMu.Unlock()
	return handle
}

func resolveIdentity() (*mcrypto.Identity, error) {
	raw, err := loadIdentityBytes()
	if err != nil {
		return nil, err
	}
	if len(raw) != 0 {
		identity, err := mcrypto.DecodeIdentity(raw)
		if err == nil {
			return identity, nil
		}
	}
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		return nil, err
	}
	if err := saveIdentityBytes(identity.Encode()); err != nil {
		return nil, err
	}
	return identity, nil
}

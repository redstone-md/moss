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
*/
import "C"

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/mesh"
)

const relayFFITimeout = 5 * time.Second

var (
	handleCounter atomic.Int64
	registryMu    sync.RWMutex
	registry      = make(map[int64]*mesh.Node)
	keystoreMu    sync.RWMutex
	keystoreLoad  C.MossKeyStoreLoadCallback
	keystoreSave  C.MossKeyStoreSaveCallback

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

//export Moss_Connect
func Moss_Connect(handle C.MossHandle, addr *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	return C.int32_t(node.Connect(C.GoString(addr)))
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
//export Moss_LastError
func Moss_LastError(handle C.MossHandle) *C.char {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return nil
	}
	return C.CString(node.LastError())
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
	handle := handleCounter.Add(1)
	registryMu.Lock()
	registry[handle] = node
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

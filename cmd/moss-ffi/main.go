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
*/
import "C"

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"moss/internal/mesh"
)

var (
	handleCounter atomic.Int64
	registryMu    sync.RWMutex
	registry      = make(map[int64]*mesh.Node)
)

func main() {}

//export Moss_Init
func Moss_Init(meshID *C.char, psk *C.uint8_t, config *C.char) C.MossHandle {
	if meshID == nil {
		return C.MossHandle(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	cfg, err := mesh.ParseConfig(cString(config))
	if err != nil {
		return C.MossHandle(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	node, err := mesh.NewNode(C.GoString(meshID), pskBytes(psk), cfg)
	if err != nil {
		return C.MossHandle(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	handle := handleCounter.Add(1)
	registryMu.Lock()
	registry[handle] = node
	registryMu.Unlock()
	return C.MossHandle(handle)
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

package main

/*
#include <stdint.h>
typedef int64_t MossHandle;
*/
import "C"

import (
	"encoding/json"

	"github.com/redstone-md/moss/internal/mesh"
)

// Moss_SetRoomPeerDiscovery is optional and additive. mode=0 restores automatic
// discovery (peerIDsJSON="[]"); mode=1 takes a nonempty JSON array of peer IDs.
// This is a discovery hint, not an ACL. The caller retains ConnectToPeer targets.
//
//export Moss_SetRoomPeerDiscovery
func Moss_SetRoomPeerDiscovery(handle C.MossHandle, room *C.char, mode C.int32_t, peerIDsJSON *C.char) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	if room == nil || peerIDsJSON == nil {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	var peers []string
	if err := json.Unmarshal([]byte(C.GoString(peerIDsJSON)), &peers); err != nil || peers == nil {
		return C.int32_t(mesh.MOSS_ERR_CONFIG_INVALID)
	}
	return C.int32_t(node.SetRoomPeerDiscovery(C.GoString(room), mesh.RoomPeerDiscovery(mode), peers))
}

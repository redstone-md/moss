package main

/*
#include <stdint.h>
typedef int64_t MossHandle;
*/
import "C"

import "github.com/redstone-md/moss/internal/mesh"

//export Moss_SetActivityMode
func Moss_SetActivityMode(handle C.MossHandle, mode C.int32_t) C.int32_t {
	node, code := getNode(int64(handle))
	if code != mesh.MOSS_OK {
		return C.int32_t(code)
	}
	return C.int32_t(node.SetActivityMode(mesh.ActivityMode(mode)))
}

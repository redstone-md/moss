import ctypes
from pathlib import Path
from ctypes import c_char_p, c_int32, c_int64, c_uint8, c_uint32, c_void_p


LIB = ctypes.CDLL(str((Path(__file__).resolve().parents[2] / "moss.dll")))

MossMessageCallback = ctypes.CFUNCTYPE(None, c_char_p, ctypes.POINTER(c_uint8), ctypes.POINTER(c_uint8), c_uint32)

LIB.Moss_Init.argtypes = [c_char_p, ctypes.POINTER(c_uint8), c_char_p]
LIB.Moss_Init.restype = c_int64
LIB.Moss_Start.argtypes = [c_int64]
LIB.Moss_Start.restype = c_int32
LIB.Moss_Stop.argtypes = [c_int64]
LIB.Moss_Stop.restype = c_int32
LIB.Moss_Subscribe.argtypes = [c_int64, c_char_p]
LIB.Moss_Subscribe.restype = c_int32
LIB.Moss_Publish.argtypes = [c_int64, c_char_p, ctypes.POINTER(c_uint8), c_uint32]
LIB.Moss_Publish.restype = c_int32
LIB.Moss_SetCallback.argtypes = [c_int64, MossMessageCallback]
LIB.Moss_SetCallback.restype = c_int32
LIB.Moss_GetMeshInfo.argtypes = [c_int64]
LIB.Moss_GetMeshInfo.restype = c_void_p
LIB.Moss_Free.argtypes = [c_void_p]


@MossMessageCallback
def on_message(channel, sender_id, data, length):
    del sender_id
    print(f"message on {channel.decode()}: {bytes(data[:length]).decode()}")


def main():
    handle = LIB.Moss_Init(b"demo-mesh", None, b'{"trackers":[],"listen_port":41030}')
    if handle <= 0:
        raise RuntimeError(f"Moss_Init failed: {handle}")

    LIB.Moss_SetCallback(handle, on_message)
    LIB.Moss_Start(handle)
    LIB.Moss_Subscribe(handle, b"alpha")
    payload = (c_uint8 * 17)(*b"hello from Python")
    LIB.Moss_Publish(handle, b"alpha", payload, len(payload))

    info_ptr = LIB.Moss_GetMeshInfo(handle)
    if info_ptr:
        print(ctypes.string_at(info_ptr).decode())
        LIB.Moss_Free(info_ptr)

    LIB.Moss_Stop(handle)


if __name__ == "__main__":
    main()

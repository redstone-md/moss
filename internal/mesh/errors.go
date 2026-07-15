package mesh

const (
	MOSS_OK                    int32 = 0
	MOSS_ERR_INVALID_HANDLE    int32 = -1
	MOSS_ERR_ALREADY_STARTED   int32 = -2
	MOSS_ERR_NOT_STARTED       int32 = -3
	MOSS_ERR_INVALID_CHANNEL   int32 = -4
	MOSS_ERR_MESSAGE_TOO_LARGE int32 = -5
	MOSS_ERR_NO_PEERS          int32 = -6
	MOSS_ERR_TRACKER_FAIL      int32 = -7
	MOSS_ERR_CONFIG_INVALID    int32 = -8
	MOSS_ERR_OUT_OF_MEMORY     int32 = -9
	MOSS_ERR_CONNECT_FAILED    int32 = -10
	MOSS_ERR_RELAY_FAILED      int32 = -11 // relay send failed (no route, session open failed, or send error)
	MOSS_ERR_INTERNAL          int32 = -12 // unexpected internal failure (e.g. room-seal crypto error)
	MOSS_ERR_LISTEN_FAILED     int32 = -13 // could not bind the tcp/udp listener (e.g. port in use, or Go's netpoller can't bind under Wine/Proton). Moss_LastError has the underlying OS error.
)

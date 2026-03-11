#include <stdint.h>
#include <stdio.h>
#include "../../moss.h"

static void on_message(const char* channel, const uint8_t* sender_id, const uint8_t* data, uint32_t len) {
    (void)sender_id;
    printf("message on %s: %.*s\n", channel, (int)len, data);
}

int main(void) {
    const char* config = "{\"trackers\":[],\"listen_port\":41010}";
    MossHandle handle = Moss_Init("demo-mesh", NULL, config);
    if (handle <= 0) {
        fprintf(stderr, "Moss_Init failed: %lld\n", (long long)handle);
        return 1;
    }

    Moss_SetCallback(handle, on_message);
    if (Moss_Start(handle) != 0) {
        fprintf(stderr, "Moss_Start failed\n");
        return 1;
    }

    Moss_Subscribe(handle, "alpha");
    Moss_Publish(handle, "alpha", (const uint8_t*)"hello from C", 12);

    const char* info = Moss_GetMeshInfo(handle);
    if (info != NULL) {
        printf("mesh info: %s\n", info);
        Moss_Free((void*)info);
    }

    Moss_Stop(handle);
    return 0;
}

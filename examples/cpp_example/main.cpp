#include <cstdint>
#include <cstdio>
#include "../../moss.h"

static void on_message(const char* channel, const uint8_t* sender_id, const uint8_t* data, uint32_t len) {
    (void)sender_id;
    std::printf("cpp message on %s: %.*s\n", channel, static_cast<int>(len), data);
}

int main() {
    char mesh_id[] = "demo-mesh";
    char config[] = "{\"trackers\":[],\"listen_port\":41020}";
    char channel[] = "alpha";
    MossHandle handle = Moss_Init(mesh_id, nullptr, config);
    if (handle <= 0) {
        std::fprintf(stderr, "Moss_Init failed: %lld\n", static_cast<long long>(handle));
        return 1;
    }

    Moss_SetCallback(handle, on_message);
    Moss_Start(handle);
    Moss_Subscribe(handle, channel);
    uint8_t payload[] = "hello from C++";
    Moss_Publish(handle, channel, payload, sizeof(payload) - 1);

    const char* info = Moss_GetMeshInfo(handle);
    if (info != nullptr) {
        std::printf("mesh info: %s\n", info);
        Moss_Free((void*)info);
    }

    Moss_Stop(handle);
    return 0;
}

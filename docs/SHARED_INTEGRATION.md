# Moss Shared Integration Guide

This guide is for application developers embedding the `moss` shared library into an existing product.

Use this together with [docs/API.md](./API.md):

- `API.md` is the exact exported FFI surface
- this document is the practical integration guide: packaging, lifecycle, memory ownership, callbacks, and JNI patterns

## What You Ship

`cmd/moss-ffi` builds a native shared library plus a generated C header:

```bash
# Linux
go build -buildmode=c-shared -o libmoss.so ./cmd/moss-ffi

# Windows
go build -buildmode=c-shared -o moss.dll ./cmd/moss-ffi

# macOS Intel
CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 \
  go build -buildmode=c-shared -o libmoss.dylib ./cmd/moss-ffi

# macOS Apple Silicon
CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 \
  go build -buildmode=c-shared -o libmoss.dylib ./cmd/moss-ffi
```

Build outputs:

- Windows: `moss.dll` and `moss.h`
- Linux/macOS: `libmoss.so` or `libmoss.dylib` and `libmoss.h`

The generated header is the source of truth for types and callback signatures.

## Integration Model

Typical host lifecycle:

1. Load the native library.
2. Register global keystore callbacks if you want persistent identity.
3. Call `Moss_Init(meshId, psk, configJson)`.
4. Register message and event callbacks on the returned handle.
5. Call `Moss_Start(handle)`.
6. Subscribe to channels with `Moss_Subscribe`.
7. Publish with `Moss_Publish`.
8. Call `Moss_Stop(handle)` during shutdown.

Recommended call order:

```text
Moss_SetKeyStore(...)      optional, global
Moss_Init(...)
Moss_SetCallback(...)
Moss_SetEventCallback(...)
Moss_SetScoringCallback(...)   optional, per-handle
Moss_Start(...)
Moss_Subscribe(...)
Moss_Publish(...)
Moss_Stop(...)
```

## Memory Ownership Rules

These rules are the most important part of a correct integration.

Host-owned memory:

- `mesh_id`
- `psk`
- `config`
- publish payload buffers passed into `Moss_Publish`

Moss-owned memory returned to the host:

- `Moss_GetMeshInfo`
- `Moss_GetPublicKey`
- `Moss_GetNATType`

Anything returned by those functions must be released with:

```c
Moss_Free(ptr);
```

Never free Moss-owned memory with:

- `free`
- `delete`
- `Marshal.FreeHGlobal`
- `ctypes` manual deallocator
- `JNI Release*` functions

Always use `Moss_Free`.

## Threading and Callback Semantics

Callbacks are invoked from Moss runtime goroutines. Do not assume they run on your UI thread.

Practical rule:

- treat all callbacks as background-thread callbacks
- copy any incoming data you need
- forward work onto your application event loop / main thread yourself

This matters for:

- Java UI frameworks
- Android main looper
- .NET UI frameworks
- Python UI frameworks
- game engines

Do not do heavy blocking work directly inside callbacks.

## Config Strategy

`Moss_Init` accepts a JSON config string. The most common host pattern is:

- keep your app config in your own native format
- render only the Moss-relevant subset to JSON
- pass that JSON into `Moss_Init`

Example:

```json
{
  "listen_port": 41030,
  "trackers": [
    "udp://tracker.opentrackr.org:1337/announce"
  ],
  "static_peers": [],
  "gossipsub": {
    "heartbeat_ms": 1000
  },
  "nat": {
    "upnp_enabled": false,
    "natpmp_enabled": false,
    "pcp_enabled": false
  }
}
```

Notes:

- omit `trackers` to use the built-in default tracker set
- pass `"trackers": []` to disable tracker bootstrap explicitly
- set NAT port mapping flags to `true` only when the app explicitly wants the router to expose the Moss listener
- partial nested objects are supported

## Packaging by Platform

### Windows

Ship:

- `moss.dll`
- your application executable

Recommended layout:

```text
MyApp.exe
moss.dll
```

### Linux

Ship:

- `libmoss.so`

Recommended options:

- place it next to the executable and set `rpath`
- or install into a known library directory and set loader paths explicitly

### macOS

Ship the architecture-correct `libmoss.dylib`:

- Intel Macs: `darwin/amd64`
- Apple Silicon: `darwin/arm64`

For production packaging, codesigning and bundle-relative loader paths are the host app's responsibility.

## Minimal C Host Pattern

```c
#include "moss.h"
#include <stdio.h>
#include <string.h>

static void on_message(const char* channel,
                       const uint8_t* sender_id,
                       const uint8_t* data,
                       uint32_t len) {
  (void)sender_id;
  printf("message on %s: %.*s\n", channel, (int)len, (const char*)data);
}

static void on_event(int32_t event_type, const char* detail_json) {
  printf("event %d: %s\n", (int)event_type, detail_json);
}

int main(void) {
  const char* config = "{\"listen_port\":41030}";
  MossHandle handle = Moss_Init("my-mesh", NULL, config);
  if (handle < 0) {
    return 1;
  }

  Moss_SetCallback(handle, on_message);
  Moss_SetEventCallback(handle, on_event);
  Moss_Start(handle);
  Moss_Subscribe(handle, "lobby");

  const char* text = "hello";
  Moss_Publish(handle, "lobby", (const uint8_t*)text, (uint32_t)strlen(text));

  Moss_Stop(handle);
  return 0;
}
```

## JNI Integration

For Java, the correct model is:

1. load `moss` and your JNI bridge library
2. keep Moss behind a thin native bridge
3. convert JNI calls to `Moss_*`
4. forward callbacks back into Java through cached method IDs

Do not call `Moss_*` directly from Java using raw FFM/JNA/JNR unless you are willing to own native callback complexity and pointer lifetime details. JNI is the safer path for a production integration.

### Recommended Architecture

```text
Java/Kotlin app
  -> JNI bridge you own
    -> moss shared library
```

Why this is the right boundary:

- Java gets a simple object-oriented API
- your JNI layer owns callback marshaling
- your JNI layer can post callbacks onto the JVM thread/executor you choose
- native handle lifetime stays explicit

### Java Side

Example Java wrapper:

```java
package com.example.moss;

public final class MossNode implements AutoCloseable {
    static {
        System.loadLibrary("moss_jni");
    }

    private long handle;

    public MossNode(String meshId, String configJson) {
        this.handle = nativeInit(meshId, configJson);
        if (this.handle <= 0) {
            throw new IllegalStateException("Moss init failed: " + this.handle);
        }
    }

    public void start() {
        check(nativeStart(handle), "start");
    }

    public void subscribe(String channel) {
        check(nativeSubscribe(handle, channel), "subscribe");
    }

    public void publish(String channel, byte[] payload) {
        check(nativePublish(handle, channel, payload), "publish");
    }

    public void setListener(MossListener listener) {
        nativeSetListener(handle, listener);
    }

    public String meshInfoJson() {
        return nativeGetMeshInfo(handle);
    }

    @Override
    public void close() {
        if (handle != 0) {
            nativeStop(handle);
            handle = 0;
        }
    }

    private static void check(int code, String op) {
        if (code != 0) {
            throw new IllegalStateException("Moss " + op + " failed: " + code);
        }
    }

    private static native long nativeInit(String meshId, String configJson);
    private static native int nativeStart(long handle);
    private static native int nativeStop(long handle);
    private static native int nativeSubscribe(long handle, String channel);
    private static native int nativePublish(long handle, String channel, byte[] payload);
    private static native void nativeSetListener(long handle, MossListener listener);
    private static native String nativeGetMeshInfo(long handle);
}
```

Listener contract:

```java
package com.example.moss;

public interface MossListener {
    void onMessage(String channel, byte[] senderId, byte[] payload);
    void onEvent(int eventType, String detailJson);
}
```

### JNI Bridge

At minimum your bridge needs to:

- store `MossHandle`
- hold a `JavaVM*`
- keep a global ref to the Java listener
- cache `jmethodID` for `onMessage` and `onEvent`
- attach callback threads to the JVM before calling back into Java

JNI bridge sketch:

```c
#include <jni.h>
#include <stdint.h>
#include <string.h>
#include "moss.h"

static JavaVM* g_vm = NULL;
static jobject g_listener = NULL;
static jmethodID g_onMessage = NULL;
static jmethodID g_onEvent = NULL;

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
  (void)reserved;
  g_vm = vm;
  return JNI_VERSION_1_8;
}

static JNIEnv* get_env(int* did_attach) {
  *did_attach = 0;
  JNIEnv* env = NULL;
  if ((*g_vm)->GetEnv(g_vm, (void**)&env, JNI_VERSION_1_8) == JNI_OK) {
    return env;
  }
  if ((*g_vm)->AttachCurrentThread(g_vm, (void**)&env, NULL) == JNI_OK) {
    *did_attach = 1;
    return env;
  }
  return NULL;
}

static void release_env(int did_attach) {
  if (did_attach) {
    (*g_vm)->DetachCurrentThread(g_vm);
  }
}

static void on_message_cb(const char* channel,
                          const uint8_t* sender_id,
                          const uint8_t* data,
                          uint32_t len) {
  int did_attach = 0;
  JNIEnv* env = get_env(&did_attach);
  if (env == NULL || g_listener == NULL) {
    return;
  }

  jstring jChannel = (*env)->NewStringUTF(env, channel);
  jbyteArray jSender = (*env)->NewByteArray(env, 32);
  jbyteArray jPayload = (*env)->NewByteArray(env, (jsize)len);

  (*env)->SetByteArrayRegion(env, jSender, 0, 32, (const jbyte*)sender_id);
  (*env)->SetByteArrayRegion(env, jPayload, 0, (jsize)len, (const jbyte*)data);
  (*env)->CallVoidMethod(env, g_listener, g_onMessage, jChannel, jSender, jPayload);

  (*env)->DeleteLocalRef(env, jChannel);
  (*env)->DeleteLocalRef(env, jSender);
  (*env)->DeleteLocalRef(env, jPayload);
  release_env(did_attach);
}

static void on_event_cb(int32_t event_type, const char* detail_json) {
  int did_attach = 0;
  JNIEnv* env = get_env(&did_attach);
  if (env == NULL || g_listener == NULL) {
    return;
  }

  jstring jDetail = (*env)->NewStringUTF(env, detail_json);
  (*env)->CallVoidMethod(env, g_listener, g_onEvent, (jint)event_type, jDetail);
  (*env)->DeleteLocalRef(env, jDetail);
  release_env(did_attach);
}
```

### JNI Methods

Example JNI entrypoints:

```c
JNIEXPORT jlong JNICALL
Java_com_example_moss_MossNode_nativeInit(JNIEnv* env, jclass cls, jstring meshId, jstring configJson) {
  (void)cls;
  const char* mesh = (*env)->GetStringUTFChars(env, meshId, NULL);
  const char* cfg = configJson ? (*env)->GetStringUTFChars(env, configJson, NULL) : NULL;
  MossHandle handle = Moss_Init(mesh, NULL, cfg);
  if (cfg) {
    (*env)->ReleaseStringUTFChars(env, configJson, cfg);
  }
  (*env)->ReleaseStringUTFChars(env, meshId, mesh);
  return (jlong)handle;
}

JNIEXPORT void JNICALL
Java_com_example_moss_MossNode_nativeSetListener(JNIEnv* env, jclass cls, jlong handle, jobject listener) {
  (void)cls;
  if (g_listener) {
    (*env)->DeleteGlobalRef(env, g_listener);
    g_listener = NULL;
  }
  g_listener = (*env)->NewGlobalRef(env, listener);

  jclass listenerCls = (*env)->GetObjectClass(env, listener);
  g_onMessage = (*env)->GetMethodID(env, listenerCls, "onMessage", "(Ljava/lang/String;[B[B)V");
  g_onEvent = (*env)->GetMethodID(env, listenerCls, "onEvent", "(ILjava/lang/String;)V");

  Moss_SetCallback((MossHandle)handle, on_message_cb);
  Moss_SetEventCallback((MossHandle)handle, on_event_cb);
}
```

### JNI Ownership Rules

Critical rules:

- keep the Java listener as a global ref, not a local ref
- delete and replace the old global ref when swapping listeners
- attach native callback threads to the JVM before invoking Java
- detach them afterwards if you attached them
- copy callback payloads into Java-owned arrays before returning

### Android Notes

For Android, the JNI pattern is the same, but you also need to think about:

- where you store identity blobs
- background service lifecycle
- foreground service requirements for long-running connectivity
- packaging per ABI

Typical ABI output matrix:

- `arm64-v8a`
- optionally `x86_64` for emulator builds


Keystore, scoring, failure-mode, wrapper, and example details continue in [SHARED_INTEGRATION-ADVANCED.md](SHARED_INTEGRATION-ADVANCED.md).

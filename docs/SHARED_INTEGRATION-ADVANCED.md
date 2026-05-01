# Moss Shared Integration Guide - Advanced Topics

## Keystore Integration

If you want stable peer identity across restarts, register `Moss_SetKeyStore` before `Moss_Init`.

Recommended behavior:

- host loads previously saved identity blob into a byte buffer
- host returns length from the load callback
- host persists new identity from the save callback

This should live in your app's durable storage layer, not a temp directory.

## Scoring Callback Integration

`Moss_SetScoringCallback` is optional. Use it only if your host app has a real policy reason to override peer score.

Good use cases:

- prefer known corporate relays
- deprioritize low-trust peers
- integrate host-level health signals

Bad use cases:

- arbitrary score randomization
- blocking inside the scoring callback
- network I/O inside the callback

## Common Failure Modes

### `Moss_Init` returns a negative value

Most common causes:

- invalid JSON config
- invalid listen port
- identity restore failure through host keystore callbacks

### Messages arrive but UI breaks

Cause:

- callback invoked on background thread

Fix:

- dispatch callback work to your UI/main thread

### Memory leak in diagnostics path

Cause:

- forgetting `Moss_Free` for `Moss_GetMeshInfo`, `Moss_GetPublicKey`, or `Moss_GetNATType`

### Callback crashes on Java/.NET side

Cause:

- host callback object got garbage-collected or freed
- wrong callback signature
- callback touching UI objects from a non-UI thread

## Recommended Host Wrapper API

Do not expose raw C pointers throughout your application.

Wrap Moss behind a small host-native API:

- `create(meshId, config)`
- `start()`
- `stop()`
- `subscribe(channel)`
- `unsubscribe(channel)`
- `publish(channel, bytes)`
- `connect(addr)`
- `meshInfoJson()`
- `natType()`
- `setListener(listener)`

Keep the rest of your app unaware of `MossHandle`, raw buffers, and `Moss_Free`.

## Existing Examples

Reference examples in this repository:

- C: [`examples/c_example`](../examples/c_example)
- C++: [`examples/cpp_example`](../examples/cpp_example)
- C#: [`examples/csharp_example`](../examples/csharp_example)
- Python minimal: [`examples/python_example`](../examples/python_example)
- Python chat: [`examples/python_chat`](../examples/python_chat)
- Rust: [`examples/rust_example`](../examples/rust_example)

Use those for exact symbol usage. Use this document for production integration structure.

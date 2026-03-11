use std::ffi::{c_char, c_void, CString};

type MossHandle = i64;

type MossMessageCallback = Option<
    unsafe extern "C" fn(channel: *const c_char, sender_id: *const u8, data: *const u8, len: u32),
>;

unsafe extern "C" {
    fn Moss_Init(mesh_id: *const c_char, psk: *const u8, config: *const c_char) -> MossHandle;
    fn Moss_Start(handle: MossHandle) -> i32;
    fn Moss_Stop(handle: MossHandle) -> i32;
    fn Moss_Subscribe(handle: MossHandle, channel: *const c_char) -> i32;
    fn Moss_Publish(handle: MossHandle, channel: *const c_char, data: *const u8, len: u32) -> i32;
    fn Moss_SetCallback(handle: MossHandle, cb: MossMessageCallback) -> i32;
    fn Moss_GetMeshInfo(handle: MossHandle) -> *const c_char;
    fn Moss_Free(ptr: *mut c_void);
}

unsafe extern "C" fn on_message(channel: *const c_char, _sender_id: *const u8, data: *const u8, len: u32) {
    let channel = unsafe { std::ffi::CStr::from_ptr(channel) }.to_string_lossy();
    let bytes = unsafe { std::slice::from_raw_parts(data, len as usize) };
    println!("rust message on {channel}: {}", String::from_utf8_lossy(bytes));
}

fn main() {
    let mesh_id = CString::new("demo-mesh").unwrap();
    let config = CString::new(r#"{"trackers":[],"listen_port":41040}"#).unwrap();
    let channel = CString::new("alpha").unwrap();
    let payload = b"hello from Rust";

    let handle = unsafe { Moss_Init(mesh_id.as_ptr(), std::ptr::null(), config.as_ptr()) };
    if handle <= 0 {
        panic!("Moss_Init failed: {handle}");
    }

    unsafe {
        Moss_SetCallback(handle, Some(on_message));
        Moss_Start(handle);
        Moss_Subscribe(handle, channel.as_ptr());
        Moss_Publish(handle, channel.as_ptr(), payload.as_ptr(), payload.len() as u32);

        let info_ptr = Moss_GetMeshInfo(handle);
        if !info_ptr.is_null() {
            println!("{}", std::ffi::CStr::from_ptr(info_ptr).to_string_lossy());
            Moss_Free(info_ptr as *mut c_void);
        }

        Moss_Stop(handle);
    }
}

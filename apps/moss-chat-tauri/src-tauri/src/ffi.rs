use std::{
    env,
    ffi::{c_char, c_void, CStr, CString},
    path::{Path, PathBuf},
};

use libloading::Library;
use serde::Deserialize;

type MossHandle = i64;
type MossInit = unsafe extern "C" fn(*const c_char, *const u8, *const c_char) -> MossHandle;
type MossStart = unsafe extern "C" fn(MossHandle) -> i32;
type MossStop = unsafe extern "C" fn(MossHandle) -> i32;
type MossSubscribe = unsafe extern "C" fn(MossHandle, *const c_char) -> i32;
type MossGetMeshInfo = unsafe extern "C" fn(MossHandle) -> *mut c_char;
type MossGetNatType = unsafe extern "C" fn(MossHandle) -> *mut c_char;
type MossFree = unsafe extern "C" fn(*mut c_void);

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MeshInfo {
    pub mesh_id: String,
    pub listen_port: u16,
    pub peer_count: usize,
    pub peers: Vec<String>,
    pub channels: Vec<String>,
    pub nat_type: String,
    pub public_key: String,
    pub supernode_ready: bool,
}

pub struct MossLibrary {
    _lib: Library,
    path: PathBuf,
    init: MossInit,
    start: MossStart,
    stop: MossStop,
    subscribe: MossSubscribe,
    get_mesh_info: MossGetMeshInfo,
    get_nat_type: MossGetNatType,
    free: MossFree,
}

impl MossLibrary {
    pub fn load() -> Result<Self, String> {
        let candidates = shared_candidates();
        for candidate in candidates {
            if !candidate.exists() {
                continue;
            }
            let loaded = unsafe { Self::load_from(candidate) };
            if let Ok(library) = loaded {
                return Ok(library);
            }
        }
        Err(format!(
            "shared runtime not found; set MOSS_SHARED_PATH or place {} next to the desktop binary",
            library_file_name()
        ))
    }

    pub fn path_display(&self) -> String {
        self.path.display().to_string()
    }

    pub fn init_handle(&self, mesh_id: &str, config: &str) -> Result<MossHandle, String> {
        let mesh_id = CString::new(mesh_id).map_err(|_| "mesh id contains NUL byte".to_string())?;
        let config = CString::new(config).map_err(|_| "config contains NUL byte".to_string())?;
        let handle = unsafe { (self.init)(mesh_id.as_ptr(), std::ptr::null(), config.as_ptr()) };
        if handle <= 0 {
            return Err(format!("Moss_Init failed: {}", error_message(handle as i32)));
        }
        Ok(handle)
    }

    pub fn start(&self, handle: MossHandle) -> Result<(), String> {
        let code = unsafe { (self.start)(handle) };
        if code != 0 {
            return Err(format!("Moss_Start failed: {}", error_message(code)));
        }
        Ok(())
    }

    pub fn stop(&self, handle: MossHandle) -> Result<(), String> {
        let code = unsafe { (self.stop)(handle) };
        if code != 0 {
            return Err(format!("Moss_Stop failed: {}", error_message(code)));
        }
        Ok(())
    }

    pub fn subscribe(&self, handle: MossHandle, channel: &str) -> Result<(), String> {
        let channel =
            CString::new(channel).map_err(|_| "channel contains NUL byte".to_string())?;
        let code = unsafe { (self.subscribe)(handle, channel.as_ptr()) };
        if code != 0 {
            return Err(format!("Moss_Subscribe failed: {}", error_message(code)));
        }
        Ok(())
    }

    pub fn mesh_info(&self, handle: MossHandle) -> Result<MeshInfo, String> {
        let raw = unsafe { (self.get_mesh_info)(handle) };
        if raw.is_null() {
            return Err("Moss_GetMeshInfo returned null".to_string());
        }
        let payload = unsafe { CStr::from_ptr(raw) }
            .to_string_lossy()
            .into_owned();
        unsafe { (self.free)(raw.cast::<c_void>()) };
        serde_json::from_str(&payload).map_err(|err| format!("invalid mesh info json: {err}"))
    }

    pub fn nat_type(&self, handle: MossHandle) -> Result<String, String> {
        let raw = unsafe { (self.get_nat_type)(handle) };
        if raw.is_null() {
            return Err("Moss_GetNATType returned null".to_string());
        }
        let payload = unsafe { CStr::from_ptr(raw) }
            .to_string_lossy()
            .into_owned();
        unsafe { (self.free)(raw.cast::<c_void>()) };
        Ok(payload)
    }

    unsafe fn load_from(path: PathBuf) -> Result<Self, String> {
        let lib = Library::new(&path)
            .map_err(|err| format!("failed to load {}: {err}", path.display()))?;
        let init = *lib
            .get::<MossInit>(b"Moss_Init\0")
            .map_err(|err| format!("missing Moss_Init: {err}"))?;
        let start = *lib
            .get::<MossStart>(b"Moss_Start\0")
            .map_err(|err| format!("missing Moss_Start: {err}"))?;
        let stop = *lib
            .get::<MossStop>(b"Moss_Stop\0")
            .map_err(|err| format!("missing Moss_Stop: {err}"))?;
        let subscribe = *lib
            .get::<MossSubscribe>(b"Moss_Subscribe\0")
            .map_err(|err| format!("missing Moss_Subscribe: {err}"))?;
        let get_mesh_info = *lib
            .get::<MossGetMeshInfo>(b"Moss_GetMeshInfo\0")
            .map_err(|err| format!("missing Moss_GetMeshInfo: {err}"))?;
        let get_nat_type = *lib
            .get::<MossGetNatType>(b"Moss_GetNATType\0")
            .map_err(|err| format!("missing Moss_GetNATType: {err}"))?;
        let free = *lib
            .get::<MossFree>(b"Moss_Free\0")
            .map_err(|err| format!("missing Moss_Free: {err}"))?;
        Ok(Self {
            _lib: lib,
            path,
            init,
            start,
            stop,
            subscribe,
            get_mesh_info,
            get_nat_type,
            free,
        })
    }
}

fn library_file_name() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        "moss.dll"
    }
    #[cfg(target_os = "macos")]
    {
        "libmoss.dylib"
    }
    #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
    {
        "libmoss.so"
    }
}

fn push_candidate(candidates: &mut Vec<PathBuf>, path: PathBuf) {
    if !candidates.iter().any(|candidate| candidate == &path) {
        candidates.push(path);
    }
}

fn shared_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(explicit) = env::var("MOSS_SHARED_PATH") {
        push_candidate(&mut candidates, PathBuf::from(explicit));
    }
    if let Ok(exe) = env::current_exe() {
        if let Some(dir) = exe.parent() {
            push_candidate(&mut candidates, dir.join(library_file_name()));
        }
    }
    if let Ok(cwd) = env::current_dir() {
        push_candidate(&mut candidates, cwd.join(library_file_name()));
        let repo_root = cwd
            .ancestors()
            .find(|candidate| candidate.join("go.mod").exists())
            .unwrap_or(Path::new("."));
        push_candidate(&mut candidates, repo_root.join(library_file_name()));
    }
    candidates
}

fn error_message(code: i32) -> &'static str {
    match code {
        0 => "ok",
        -1 => "invalid handle",
        -2 => "already started",
        -3 => "not started",
        -4 => "invalid channel",
        -5 => "message too large",
        -6 => "publish failed",
        -7 => "subscribe failed",
        -8 => "config invalid",
        -9 => "connect failed",
        _ => "unknown error",
    }
}

/// Current Unix wall-clock milliseconds, including in Window and Worker contexts.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn unix_millis() -> i64 {
    js_sys::Date::now() as i64
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub fn unix_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

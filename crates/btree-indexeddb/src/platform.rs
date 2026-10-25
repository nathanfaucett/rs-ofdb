use js_sys::Promise;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen(module = "/src/indexeddb.js")]
extern "C" {
    #[wasm_bindgen(catch, js_name = open)]
    pub fn open(name: &str, kind: &str) -> Result<Promise, JsValue>;
    pub fn close(database: &JsValue);
    #[wasm_bindgen(catch, js_name = ensureOpen)]
    pub fn ensure_open(database: &JsValue) -> Result<(), JsValue>;
    #[wasm_bindgen(catch)]
    pub fn snapshot(database: &JsValue) -> Result<Promise, JsValue>;
    #[wasm_bindgen(catch)]
    pub fn commit(
        database: &JsValue,
        revision: &str,
        tables: &JsValue,
        records: &JsValue,
    ) -> Result<Promise, JsValue>;
}

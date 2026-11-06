use ofdb_sql::{Client, Error, ErrorKind, Migration, MigrationError, MigrationPhase};
use serde_json::{Value as Json, json};
use wasm_bindgen::JsValue;

pub(crate) async fn migrate(client: &Client, text: &str) -> Result<String, JsValue> {
    let plan = parse_plan(text).map_err(to_js)?;
    let result = client.migrate(&plan).await.map_err(to_js)?;
    Ok(json!({
        "currentVersion": result.current_version,
        "appliedVersions": result.applied_versions,
    })
    .to_string())
}

fn parse_plan(text: &str) -> Result<Vec<Migration>, MigrationError> {
    let invalid = || MigrationError {
        version: None,
        phase: MigrationPhase::Plan,
        source: Error::new(ErrorKind::Validation, "invalid migration input"),
    };
    let value: Json = serde_json::from_str(text).map_err(|_| invalid())?;
    value
        .as_array()
        .ok_or_else(invalid)?
        .iter()
        .map(|item| {
            let version = item
                .get("version")
                .and_then(Json::as_u64)
                .and_then(|version| u32::try_from(version).ok())
                .ok_or_else(invalid)?;
            let description = item
                .get("description")
                .and_then(Json::as_str)
                .ok_or_else(invalid)?;
            let sql = item.get("sql").and_then(Json::as_str).ok_or_else(invalid)?;
            Ok(Migration::new(version, description, sql))
        })
        .collect()
}

fn to_js(error: MigrationError) -> JsValue {
    let value: JsValue = js_sys::Error::new(&error.to_string()).into();
    let details = json!({
        "version": error.version,
        "phase": format!("{:?}", error.phase),
        "queryKind": format!("{:?}", error.source.kind),
        "outcome": if error.commit_outcome_unknown() { Some("unknown") } else { None },
    });
    let _ = js_sys::Reflect::set(
        &value,
        &JsValue::from_str("migration"),
        &JsValue::from_str(&details.to_string()),
    );
    value
}

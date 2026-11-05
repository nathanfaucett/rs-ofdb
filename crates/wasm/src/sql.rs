use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::future::FutureExt;
use ofdb_kv::{encode_value, parse_value};
use ofdb_sql::{Client, Database, QueryParams, QueryResult, Transaction};
#[cfg(feature = "sync")]
use ofdb_sql::{
    SessionConfig as OfdbSqlSessionConfig, SyncDirection as OfdbSqlSyncDirection,
    SyncError as OfdbSqlSyncErrorEnum, SyncResult as OfdbSqlSyncResult,
    SyncRole as OfdbSqlSyncRole, SyncTransport as OfdbSqlSyncTransport,
};
use serde_json::Value as Json;
use wasm_bindgen::JsError;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::value::{parse_json, write_json};

#[wasm_bindgen]
pub struct SqlDatabase {
    database: Database,
}

#[wasm_bindgen]
impl SqlDatabase {
    /// Open a SQL database backed by IndexedDB.
    pub async fn open(name: String) -> Result<SqlDatabase, JsError> {
        let database = Database::open_indexeddb(&name).await?;
        Ok(Self { database })
    }

    /// Create a query handle sharing this database's storage.
    pub fn client(&self) -> SqlClient {
        SqlClient {
            client: self.database.client(),
        }
    }

    /// Synchronize with a remote peer using the given transport.
    #[cfg(feature = "sync")]
    pub async fn synchronize(
        &self,
        transport: &mut JsSqlSyncTransport,
        config: SqlSessionConfig,
        role: SqlSyncRole,
    ) -> Result<SqlSyncResult, JsSqlSyncError> {
        self.database
            .synchronize(transport, &config.into(), role.into())
            .await
            .map_err(JsSqlSyncError::from)
            .map(SqlSyncResult::from)
    }

    /// Synchronize with a remote peer, checking caller authorization before each batch.
    #[cfg(feature = "sync")]
    pub async fn synchronize_checked(
        &self,
        transport: &mut JsSqlSyncTransport,
        config: SqlSessionConfig,
        role: SqlSyncRole,
        check: js_sys::Function,
    ) -> Result<SqlSyncResult, JsSqlSyncError> {
        let check_fn = move || {
            let promise = check.call0(&JsValue::NULL).unwrap();
            let promise: js_sys::Promise = promise.dyn_into().unwrap();
            JsFuture::from(promise).then(|result| async move {
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(format!("{:?}", e)),
                }
            })
        };
        self.database
            .synchronize_checked(transport, &config.into(), role.into(), check_fn)
            .await
            .map_err(JsSqlSyncError::from)
            .map(SqlSyncResult::from)
    }
}

/// Sync role: Initiator or Responder.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlSyncRole {
    Initiator = 0,
    Responder = 1,
}

impl From<SqlSyncRole> for OfdbSqlSyncRole {
    fn from(role: SqlSyncRole) -> Self {
        match role {
            SqlSyncRole::Initiator => OfdbSqlSyncRole::Initiator,
            SqlSyncRole::Responder => OfdbSqlSyncRole::Responder,
        }
    }
}

impl From<OfdbSqlSyncRole> for SqlSyncRole {
    fn from(role: OfdbSqlSyncRole) -> Self {
        match role {
            OfdbSqlSyncRole::Initiator => SqlSyncRole::Initiator,
            OfdbSqlSyncRole::Responder => SqlSyncRole::Responder,
        }
    }
}

/// Sync direction.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlSyncDirection {
    Bidirectional = 0,
    SendOnly = 1,
    ReceiveOnly = 2,
}

impl From<SqlSyncDirection> for OfdbSqlSyncDirection {
    fn from(dir: SqlSyncDirection) -> Self {
        match dir {
            SqlSyncDirection::Bidirectional => OfdbSqlSyncDirection::Bidirectional,
            SqlSyncDirection::SendOnly => OfdbSqlSyncDirection::SendOnly,
            SqlSyncDirection::ReceiveOnly => OfdbSqlSyncDirection::ReceiveOnly,
        }
    }
}

impl From<OfdbSqlSyncDirection> for SqlSyncDirection {
    fn from(dir: OfdbSqlSyncDirection) -> Self {
        match dir {
            OfdbSqlSyncDirection::Bidirectional => SqlSyncDirection::Bidirectional,
            OfdbSqlSyncDirection::SendOnly => SqlSyncDirection::SendOnly,
            OfdbSqlSyncDirection::ReceiveOnly => SqlSyncDirection::ReceiveOnly,
        }
    }
}

/// Sync configuration.
#[wasm_bindgen]
pub struct SqlSessionConfig {
    inner: OfdbSqlSessionConfig,
}

#[wasm_bindgen]
impl SqlSessionConfig {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: OfdbSqlSessionConfig::default(),
        }
    }

    #[wasm_bindgen(getter)]
    pub fn direction(&self) -> SqlSyncDirection {
        self.inner.direction.into()
    }

    #[wasm_bindgen(setter)]
    pub fn set_direction(&mut self, value: SqlSyncDirection) {
        self.inner.direction = value.into();
    }

    #[wasm_bindgen(getter)]
    pub fn max_units_per_frame(&self) -> usize {
        self.inner.max_units_per_frame
    }

    #[wasm_bindgen(setter)]
    pub fn set_max_units_per_frame(&mut self, value: usize) {
        self.inner.max_units_per_frame = value;
    }

    #[wasm_bindgen(getter)]
    pub fn max_session_bytes(&self) -> usize {
        self.inner.max_session_bytes
    }

    #[wasm_bindgen(setter)]
    pub fn set_max_session_bytes(&mut self, value: usize) {
        self.inner.max_session_bytes = value;
    }
}

impl Default for SqlSessionConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl From<SqlSessionConfig> for OfdbSqlSessionConfig {
    fn from(config: SqlSessionConfig) -> Self {
        config.inner
    }
}

impl From<OfdbSqlSessionConfig> for SqlSessionConfig {
    fn from(config: OfdbSqlSessionConfig) -> Self {
        Self { inner: config }
    }
}

/// Sync result.
#[wasm_bindgen]
pub struct SqlSyncResult {
    inner: OfdbSqlSyncResult,
}

#[wasm_bindgen]
impl SqlSyncResult {
    #[wasm_bindgen(getter)]
    pub fn received_units(&self) -> usize {
        self.inner.received_units
    }

    #[wasm_bindgen(getter)]
    pub fn sent_units(&self) -> usize {
        self.inner.sent_units
    }

    #[wasm_bindgen(getter)]
    pub fn received_changes(&self) -> usize {
        self.inner.received_changes
    }

    #[wasm_bindgen(getter)]
    pub fn sent_changes(&self) -> usize {
        self.inner.sent_changes
    }

    #[wasm_bindgen(getter)]
    pub fn received_snapshots(&self) -> usize {
        self.inner.received_snapshots
    }

    #[wasm_bindgen(getter)]
    pub fn sent_snapshots(&self) -> usize {
        self.inner.sent_snapshots
    }
}

impl From<OfdbSqlSyncResult> for SqlSyncResult {
    fn from(inner: OfdbSqlSyncResult) -> Self {
        Self { inner }
    }
}

/// Sync error.
#[wasm_bindgen]
pub struct JsSqlSyncError {
    inner: OfdbSqlSyncErrorEnum<JsSqlSyncTransportError>,
}

#[wasm_bindgen]
impl JsSqlSyncError {
    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.inner.to_string()
    }

    #[wasm_bindgen(getter)]
    pub fn kind(&self) -> JsSqlSyncErrorKind {
        match &self.inner {
            OfdbSqlSyncErrorEnum::PreApply(_) => JsSqlSyncErrorKind::PreApply,
            OfdbSqlSyncErrorEnum::Engine(_) => JsSqlSyncErrorKind::Engine,
            OfdbSqlSyncErrorEnum::Transport(_) => JsSqlSyncErrorKind::Transport,
            OfdbSqlSyncErrorEnum::Staging(_) => JsSqlSyncErrorKind::Staging,
            OfdbSqlSyncErrorEnum::InvalidConfiguration => JsSqlSyncErrorKind::InvalidConfiguration,
            OfdbSqlSyncErrorEnum::Protocol(_) => JsSqlSyncErrorKind::Protocol,
            OfdbSqlSyncErrorEnum::MessageTooLarge { .. } => JsSqlSyncErrorKind::MessageTooLarge,
            OfdbSqlSyncErrorEnum::SessionTooLarge { .. } => JsSqlSyncErrorKind::SessionTooLarge,
            OfdbSqlSyncErrorEnum::IncompatibleProtocol(_) => {
                JsSqlSyncErrorKind::IncompatibleProtocol
            }
            OfdbSqlSyncErrorEnum::UnexpectedMessage => JsSqlSyncErrorKind::UnexpectedMessage,
            OfdbSqlSyncErrorEnum::RemoteAbort(_) => JsSqlSyncErrorKind::RemoteAbort,
        }
    }
}

impl From<OfdbSqlSyncErrorEnum<JsSqlSyncTransportError>> for JsSqlSyncError {
    fn from(inner: OfdbSqlSyncErrorEnum<JsSqlSyncTransportError>) -> Self {
        Self { inner }
    }
}

/// Sync error kind.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsSqlSyncErrorKind {
    PreApply = 0,
    Engine = 1,
    Transport = 2,
    Staging = 3,
    InvalidConfiguration = 4,
    Protocol = 5,
    MessageTooLarge = 6,
    SessionTooLarge = 7,
    IncompatibleProtocol = 8,
    UnexpectedMessage = 9,
    RemoteAbort = 10,
}

/// Transport error for JS-implemented transports.
#[wasm_bindgen]
pub struct JsSqlSyncTransportError {
    message: String,
}

#[wasm_bindgen]
impl JsSqlSyncTransportError {
    #[wasm_bindgen(constructor)]
    pub fn new(message: String) -> Self {
        Self { message }
    }

    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }
}

impl std::fmt::Display for JsSqlSyncTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::fmt::Debug for JsSqlSyncTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsSqlSyncTransportError({})", self.message)
    }
}

impl std::error::Error for JsSqlSyncTransportError {}

/// Sync transport trait that can be implemented in JavaScript.
#[wasm_bindgen]
pub struct JsSqlSyncTransport {
    inner: JsValue,
}

#[wasm_bindgen]
impl JsSqlSyncTransport {
    #[wasm_bindgen(constructor)]
    pub fn new(transport: JsValue) -> Self {
        Self { inner: transport }
    }
}

impl OfdbSqlSyncTransport for JsSqlSyncTransport {
    type Error = JsSqlSyncTransportError;

    fn receive(&mut self) -> impl Future<Output = Result<Vec<u8>, Self::Error>> {
        let transport = self.inner.clone();
        async move {
            let recv_fn = js_sys::Reflect::get(&transport, &"receive".into()).map_err(|_| {
                JsSqlSyncTransportError::new("transport has no receive method".into())
            })?;
            let recv_fn: js_sys::Function = recv_fn
                .dyn_into()
                .map_err(|_| JsSqlSyncTransportError::new("receive is not a function".into()))?;

            let promise = recv_fn.call0(&transport).map_err(|e| {
                JsSqlSyncTransportError::new(format!("receive call failed: {:?}", e))
            })?;
            let promise: js_sys::Promise = promise.dyn_into().map_err(|_| {
                JsSqlSyncTransportError::new("receive did not return a Promise".into())
            })?;

            let result = JsFuture::from(promise)
                .await
                .map_err(|e| JsSqlSyncTransportError::new(format!("receive failed: {:?}", e)))?;

            let array = js_sys::Uint8Array::from(result);
            let mut vec = vec![0u8; array.length() as usize];
            array.copy_to(&mut vec);
            Ok(vec)
        }
    }

    fn send(&mut self, frame: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>> {
        let transport = self.inner.clone();
        async move {
            let send_fn = js_sys::Reflect::get(&transport, &"send".into())
                .map_err(|_| JsSqlSyncTransportError::new("transport has no send method".into()))?;
            let send_fn: js_sys::Function = send_fn
                .dyn_into()
                .map_err(|_| JsSqlSyncTransportError::new("send is not a function".into()))?;

            let array = js_sys::Uint8Array::from(frame.as_slice());
            let promise = send_fn
                .call1(&transport, &array)
                .map_err(|e| JsSqlSyncTransportError::new(format!("send call failed: {:?}", e)))?;
            let promise: js_sys::Promise = promise.dyn_into().map_err(|_| {
                JsSqlSyncTransportError::new("send did not return a Promise".into())
            })?;

            JsFuture::from(promise)
                .await
                .map_err(|e| JsSqlSyncTransportError::new(format!("send failed: {:?}", e)))?;

            Ok(())
        }
    }
}

#[wasm_bindgen]
pub struct SqlClient {
    client: Client,
}

#[wasm_bindgen]
impl SqlClient {
    /// Execute SQL text with optional bound parameters and return results as JSON.
    ///
    /// Parameters are a JSON array for positional placeholders or a JSON object for
    /// named placeholders. Every value uses the owner's lossless tagged codec.
    /// The result is a JSON array of `{ columns, rows }` objects with tagged cells.
    pub async fn execute(&self, sql: String, params: Option<String>) -> Result<String, JsError> {
        let params = match params {
            Some(params) => Some(parse_params(&params)?),
            None => None,
        };
        let results = self.client.execute_sql(&sql, params.as_ref()).await?;
        write_json(&results_to_json(&results))
    }

    /// Start a transaction that holds statements until commit or rollback.
    pub async fn transaction(&self) -> Result<SqlTransaction, JsError> {
        let tx = self.client.transaction().await?;
        Ok(SqlTransaction { tx })
    }

    /// Return a clone of this client with a request deadline in milliseconds.
    pub fn with_deadline(&self, ms: u64) -> SqlClient {
        SqlClient {
            client: self.client.clone().with_deadline(Duration::from_millis(ms)),
        }
    }

    /// Return an independent clone sharing the same storage.
    pub fn clone_client(&self) -> SqlClient {
        SqlClient {
            client: self.client.clone(),
        }
    }
}

#[wasm_bindgen]
pub struct SqlTransaction {
    tx: Transaction,
}

#[wasm_bindgen]
impl SqlTransaction {
    /// Execute SQL text within the transaction.
    pub async fn execute(
        &mut self,
        sql: String,
        params: Option<String>,
    ) -> Result<String, JsError> {
        let params = match params {
            Some(params) => Some(parse_params(&params)?),
            None => None,
        };
        let results = self.tx.execute_sql(&sql, params.as_ref()).await?;
        write_json(&results_to_json(&results))
    }

    pub async fn commit(self) -> Result<(), JsError> {
        self.tx.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), JsError> {
        self.tx.rollback().await?;
        Ok(())
    }
}

fn results_to_json(results: &[QueryResult]) -> Json {
    Json::Array(
        results
            .iter()
            .map(|result| {
                let columns = result
                    .columns
                    .iter()
                    .map(|column| Json::String(column.name.clone()))
                    .collect::<Vec<_>>();
                let rows = result
                    .rows
                    .iter()
                    .map(|row| Json::Array(row.values.iter().cloned().map(encode_value).collect()))
                    .collect::<Vec<_>>();
                Json::Object(
                    [
                        ("columns".to_owned(), Json::Array(columns)),
                        ("rows".to_owned(), Json::Array(rows)),
                    ]
                    .into_iter()
                    .collect(),
                )
            })
            .collect(),
    )
}

/// Parse bound parameters supplied as JSON by the TypeScript layer.
fn parse_params(text: &str) -> Result<QueryParams, JsError> {
    match parse_json(text)? {
        Json::Array(values) => Ok(QueryParams::Positional(
            values
                .iter()
                .map(parse_value)
                .collect::<Result<_, _>>()
                .map_err(|error| JsError::new(&error))?,
        )),
        Json::Object(values) => Ok(QueryParams::Named(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), parse_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, String>>()
                .map_err(|error| JsError::new(&error))?,
        )),
        other => Err(JsError::new(&format!(
            "query parameters must be a JSON array or object, got {other}"
        ))),
    }
}

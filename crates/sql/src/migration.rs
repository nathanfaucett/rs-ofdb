use alloc::{string::String, vec, vec::Vec};
use core::fmt;

use sha2::{Digest, Sha256};

use crate::{
    Client, Error, ErrorKind, FromRow, FromRowError, QueryParams, Row, Transaction, Translator,
    Value, ValueType, decode, value,
};

const CREATE_HISTORY: &str = "CREATE TABLE _ofdb_migrations (\
    id UUID PRIMARY KEY, \
    version INTEGER, \
    description TEXT, \
    fingerprint BLOB, \
    execution_time INTEGER\
)";

const HISTORY_COLUMNS: [&str; 4] = ["version", "description", "fingerprint", "execution_time"];

/// One forward SQL batch. Supply the complete, unchanged history in ascending
/// version order. Versions must be unique and strictly increasing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    pub version: u32,
    pub description: String,
    pub sql: String,
}

impl Migration {
    pub fn new(version: u32, description: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            version,
            description: description.into(),
            sql: sql.into(),
        }
    }
}

/// One applied migration, decoded from a `_ofdb_migrations` history row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationRecord {
    pub version: u32,
    pub description: String,
    pub fingerprint: Vec<u8>,
    pub execution_time: i64,
}

impl FromRow for MigrationRecord {
    fn from_row(row: &Row, columns: &[&str]) -> Result<Self, FromRowError> {
        let version = decode::<i64>(value(row, columns, "version")?, "version")?;
        let version = u32::try_from(version).map_err(|_| FromRowError::InvalidValue {
            column: String::from("version"),
            expected: ValueType::Integer,
            actual: ValueType::Integer,
        })?;
        Ok(Self {
            version,
            description: decode(value(row, columns, "description")?, "description")?,
            fingerprint: decode(value(row, columns, "fingerprint")?, "fingerprint")?,
            execution_time: decode(value(row, columns, "execution_time")?, "execution_time")?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationResult {
    pub current_version: u32,
    pub applied_versions: Vec<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationPhase {
    Plan,
    Begin,
    History,
    Execute,
    Record,
    Commit,
}

/// Migration context with the original structured query error.
/// Display excludes SQL and data. A failed commit has an unknown durable outcome.
#[derive(Clone, PartialEq, Eq)]
pub struct MigrationError {
    pub version: Option<u32>,
    pub phase: MigrationPhase,
    pub source: Error,
}

impl MigrationError {
    fn new(version: Option<u32>, phase: MigrationPhase, source: Error) -> Self {
        Self {
            version,
            phase,
            source,
        }
    }

    fn invalid(version: Option<u32>, phase: MigrationPhase) -> Self {
        Self::new(
            version,
            phase,
            Error::new(ErrorKind::Validation, "invalid migration plan or history"),
        )
    }

    pub fn commit_outcome_unknown(&self) -> bool {
        self.phase == MigrationPhase::Commit
    }
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "migration {:?} failed during {:?} ({:?})",
            self.version, self.phase, self.source.kind
        )
    }
}

impl fmt::Debug for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl core::error::Error for MigrationError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(any(
    feature = "in-memory",
    feature = "redb",
    feature = "indexeddb",
    feature = "remote",
    feature = "sync"
))]
fn now_millis() -> i64 {
    platform::unix_millis()
}

#[cfg(not(any(
    feature = "in-memory",
    feature = "redb",
    feature = "indexeddb",
    feature = "remote",
    feature = "sync"
)))]
fn now_millis() -> i64 {
    0
}

struct Planned {
    fingerprint: Vec<u8>,
}

impl Client {
    /// Explicitly apply pending migrations, one transaction per version.
    /// No retry is performed. Opening a database never calls this operation.
    pub async fn migrate(
        &self,
        migrations: &[Migration],
    ) -> Result<MigrationResult, MigrationError> {
        let mut planned = Vec::with_capacity(migrations.len());
        for (index, migration) in migrations.iter().enumerate() {
            let ordered = index == 0 || migration.version > migrations[index - 1].version;
            let plan = self
                .run(async {
                    if !ordered
                        || migration.description.is_empty()
                        || sql_translator::SqlTranslator
                            .translate(&migration.sql)
                            .await
                            .map_err(|error| {
                                Error::new(
                                    ErrorKind::Validation,
                                    alloc::format!("Translate error: {error}"),
                                )
                            })?
                            .is_empty()
                    {
                        return Err(Error::new(
                            ErrorKind::Validation,
                            "invalid migration plan or history",
                        ));
                    }
                    Ok(Planned {
                        fingerprint: Sha256::digest(migration.sql.as_bytes()).to_vec(),
                    })
                })
                .await
                .map_err(|source| {
                    MigrationError::new(Some(migration.version), MigrationPhase::Plan, source)
                })?;
            planned.push(plan);
        }
        let current_version = migrations.last().map_or(0, |migration| migration.version);
        let mut result = MigrationResult {
            current_version,
            applied_versions: Vec::new(),
        };
        loop {
            let mut tx = self
                .transaction()
                .await
                .map_err(|source| MigrationError::new(None, MigrationPhase::Begin, source))?;
            let decision = read_history(&mut tx, migrations, &planned).await;
            let (exists, applied) = match decision {
                Ok(history) => history,
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(error);
                }
            };
            if applied == migrations.len() {
                tx.rollback()
                    .await
                    .map_err(|source| MigrationError::new(None, MigrationPhase::History, source))?;
                return Ok(result);
            }
            let migration = &migrations[applied];
            let pending = apply(&mut tx, migrations, &planned, exists, applied).await;
            if let Err(error) = pending {
                let _ = tx.rollback().await;
                return Err(error);
            }
            tx.commit().await.map_err(|source| {
                MigrationError::new(Some(migration.version), MigrationPhase::Commit, source)
            })?;
            result.applied_versions.push(migration.version);
            if migration.version == current_version {
                return Ok(result);
            }
        }
    }
}

async fn read_history(
    tx: &mut Transaction,
    migrations: &[Migration],
    planned: &[Planned],
) -> Result<(bool, usize), MigrationError> {
    let context = |source| MigrationError::new(None, MigrationPhase::History, source);
    let tables = tx
        .execute_sql(
            "SELECT name FROM tables WHERE name = '_ofdb_migrations'",
            None,
        )
        .await
        .map_err(context)?;
    let tables = tables.first().ok_or_else(|| {
        context(Error::new(
            ErrorKind::Protocol,
            "missing migration table result",
        ))
    })?;
    if tables.rows.is_empty() {
        return Ok((false, 0));
    }
    let fields = tx.execute_sql("SELECT name, type, default, primary_key FROM table_fields WHERE table = '_ofdb_migrations' ORDER BY position", None).await.map_err(context)?;
    let expected = vec![
        Row::new(vec![
            Value::from("id"),
            Value::Type(ValueType::Uuid),
            Value::Null,
            Value::Bool(true),
        ]),
        Row::new(vec![
            Value::from("version"),
            Value::Type(ValueType::Integer),
            Value::Null,
            Value::Bool(false),
        ]),
        Row::new(vec![
            Value::from("description"),
            Value::Type(ValueType::Text),
            Value::Null,
            Value::Bool(false),
        ]),
        Row::new(vec![
            Value::from("fingerprint"),
            Value::Type(ValueType::Blob),
            Value::Null,
            Value::Bool(false),
        ]),
        Row::new(vec![
            Value::from("execution_time"),
            Value::Type(ValueType::Integer),
            Value::Null,
            Value::Bool(false),
        ]),
    ];
    let fields = fields.first().ok_or_else(|| {
        context(Error::new(
            ErrorKind::Protocol,
            "missing migration fields result",
        ))
    })?;
    if tables.rows.len() != 1 || fields.rows != expected {
        return Err(MigrationError::invalid(None, MigrationPhase::History));
    }
    let history = tx
        .execute_sql(
            "SELECT version, description, fingerprint, execution_time FROM _ofdb_migrations ORDER BY version",
            None,
        )
        .await
        .map_err(context)?;
    let history = history.first().ok_or_else(|| {
        context(Error::new(
            ErrorKind::Protocol,
            "missing migration history result",
        ))
    })?;
    for (index, row) in history.rows.iter().enumerate() {
        let record = MigrationRecord::from_row(row, &HISTORY_COLUMNS).map_err(|_| {
            MigrationError::invalid(
                migrations.get(index).map(|m| m.version),
                MigrationPhase::History,
            )
        })?;
        let Some(migration) = migrations.get(index) else {
            return Err(MigrationError::invalid(
                Some(record.version),
                MigrationPhase::History,
            ));
        };
        let Some(plan) = planned.get(index) else {
            return Err(MigrationError::invalid(
                Some(record.version),
                MigrationPhase::History,
            ));
        };
        if record.version != migration.version
            || record.description != migration.description
            || record.fingerprint != plan.fingerprint
        {
            return Err(MigrationError::invalid(
                Some(record.version),
                MigrationPhase::History,
            ));
        }
    }
    Ok((true, history.rows.len()))
}

async fn apply(
    tx: &mut Transaction,
    migrations: &[Migration],
    planned: &[Planned],
    exists: bool,
    applied: usize,
) -> Result<(), MigrationError> {
    let migration = &migrations[applied];
    let context = |phase, source| MigrationError::new(Some(migration.version), phase, source);
    if !exists {
        tx.execute_sql(CREATE_HISTORY, None)
            .await
            .map_err(|source| context(MigrationPhase::History, source))?;
    }
    let start = now_millis();
    tx.execute_sql(&migration.sql, None)
        .await
        .map_err(|source| context(MigrationPhase::Execute, source))?;
    let execution_time = now_millis().saturating_sub(start);
    // Migration SQL must not change runner-owned history.
    let history = read_history(tx, &migrations[..applied], &planned[..applied])
        .await
        .map_err(|error| context(MigrationPhase::Record, error.source))?;
    if history != (true, applied) {
        return Err(MigrationError::invalid(
            Some(migration.version),
            MigrationPhase::Record,
        ));
    }
    let params = QueryParams::Positional(vec![
        Value::Integer(i64::from(migration.version)),
        Value::from(migration.description.as_str()),
        Value::Blob(planned[applied].fingerprint.clone()),
        Value::Integer(execution_time),
    ]);
    tx.execute_sql(
        "INSERT INTO _ofdb_migrations (version, description, fingerprint, execution_time) VALUES ($1, $2, $3, $4)",
        Some(&params),
    )
    .await
    .map_err(|source| context(MigrationPhase::Record, source))?;
    Ok(())
}

use alloc::{format, string::String, vec, vec::Vec};
use core::fmt;

use sha2::{Digest, Sha256};

use crate::{Client, Error, ErrorKind, QueryParams, Row, Transaction, Value, ValueType};

const CREATE_HISTORY: &str =
    "CREATE TABLE _ofdb_migrations (version INTEGER PRIMARY KEY, fingerprint TEXT)";

/// One forward SQL batch. Supply the complete, unchanged history starting at one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    pub version: u32,
    pub sql: String,
}

impl Migration {
    pub fn new(version: u32, sql: impl Into<String>) -> Self {
        Self {
            version,
            sql: sql.into(),
        }
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

impl Client {
    /// Explicitly apply pending migrations, one transaction per version.
    /// No retry is performed. Opening a database never calls this operation.
    pub async fn migrate(
        &self,
        migrations: &[Migration],
    ) -> Result<MigrationResult, MigrationError> {
        let mut fingerprints = Vec::with_capacity(migrations.len());
        for (index, migration) in migrations.iter().enumerate() {
            if usize::try_from(migration.version).ok() != index.checked_add(1)
                || migration.sql.trim().is_empty()
            {
                return Err(MigrationError::invalid(
                    Some(migration.version),
                    MigrationPhase::Plan,
                ));
            }
            fingerprints.push(format!("{:x}", Sha256::digest(migration.sql.as_bytes())));
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
            let decision = read_history(&mut tx, &fingerprints).await;
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
            let pending = apply(&mut tx, migration, &fingerprints, exists, applied).await;
            if let Err(error) = pending {
                let _ = tx.rollback().await;
                return Err(error);
            }
            tx.commit().await.map_err(|source| {
                MigrationError::new(Some(migration.version), MigrationPhase::Commit, source)
            })?;
            result.applied_versions.push(migration.version);
        }
    }
}

async fn read_history(
    tx: &mut Transaction,
    fingerprints: &[String],
) -> Result<(bool, usize), MigrationError> {
    let context = |source| MigrationError::new(None, MigrationPhase::History, source);
    let tables = tx
        .execute_sql(
            "SELECT name FROM tables WHERE name = '_ofdb_migrations'",
            None,
        )
        .await
        .map_err(context)?;
    if tables[0].rows.is_empty() {
        return Ok((false, 0));
    }
    let fields = tx.execute_sql("SELECT name, type, default, primary_key FROM table_fields WHERE table = '_ofdb_migrations' ORDER BY position", None).await.map_err(context)?;
    let expected = vec![
        Row::new(vec![
            Value::from("version"),
            Value::Type(ValueType::Integer),
            Value::Null,
            Value::Bool(true),
        ]),
        Row::new(vec![
            Value::from("fingerprint"),
            Value::Type(ValueType::Text),
            Value::Null,
            Value::Bool(false),
        ]),
    ];
    if tables[0].rows.len() != 1 || fields[0].rows != expected {
        return Err(MigrationError::invalid(None, MigrationPhase::History));
    }
    let history = tx
        .execute_sql(
            "SELECT version, fingerprint FROM _ofdb_migrations ORDER BY version",
            None,
        )
        .await
        .map_err(context)?;
    for (index, row) in history[0].rows.iter().enumerate() {
        let version = u32::try_from(index + 1).ok();
        let expected = fingerprints.get(index).map(|fingerprint| {
            vec![
                Value::Integer((index + 1) as i64),
                Value::from(fingerprint.as_str()),
            ]
        });
        if expected.as_ref() != Some(&row.values) {
            return Err(MigrationError::invalid(version, MigrationPhase::History));
        }
    }
    Ok((true, history[0].rows.len()))
}

async fn apply(
    tx: &mut Transaction,
    migration: &Migration,
    fingerprints: &[String],
    exists: bool,
    applied: usize,
) -> Result<(), MigrationError> {
    let context = |phase, source| MigrationError::new(Some(migration.version), phase, source);
    if !exists {
        tx.execute_sql(CREATE_HISTORY, None)
            .await
            .map_err(|source| context(MigrationPhase::History, source))?;
    }
    tx.execute_sql(&migration.sql, None)
        .await
        .map_err(|source| context(MigrationPhase::Execute, source))?;
    // Migration SQL must not change runner-owned history.
    let history = read_history(tx, fingerprints).await?;
    if history != (true, applied) {
        return Err(MigrationError::invalid(
            Some(migration.version),
            MigrationPhase::Record,
        ));
    }
    let params = QueryParams::Positional(vec![
        Value::Integer(i64::from(migration.version)),
        Value::from(fingerprints[applied].as_str()),
    ]);
    tx.execute_sql(
        "INSERT INTO _ofdb_migrations (version, fingerprint) VALUES ($1, $2)",
        Some(&params),
    )
    .await
    .map_err(|source| context(MigrationPhase::Record, source))?;
    Ok(())
}

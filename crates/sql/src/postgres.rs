use std::{str::FromStr, sync::Arc};

use query::{QueryError, QueryErrorKind, QueryResult, QueryResultColumn, Statement};
use tokio::task::JoinHandle;
use tokio_postgres::{
    Config, NoTls,
    types::{ToSql, Type},
};
use value::{Row, Value};

use crate::PostgresStatement;

#[derive(Clone)]
pub struct PostgresBackend {
    config: Arc<Config>,
}

impl PostgresBackend {
    pub async fn connect(config: &str) -> Result<Self, QueryError> {
        let config = Config::from_str(config)
            .map_err(|error| QueryError::new(QueryErrorKind::Validation, error.to_string()))?;
        if config.get_ssl_mode() != tokio_postgres::config::SslMode::Disable {
            return Err(QueryError::new(
                QueryErrorKind::Validation,
                "PostgreSQL requires explicit sslmode=disable; use a Unix socket or a trusted TLS tunnel",
            ));
        }
        let backend = Self {
            config: Arc::new(config),
        };
        backend.transaction().await?.rollback().await?;
        Ok(backend)
    }

    pub async fn transaction(&self) -> Result<PostgresTransaction, QueryError> {
        let (client, connection) = self.config.connect(NoTls).await.map_err(error)?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let transaction = PostgresTransaction {
            client,
            driver,
            failed: false,
        };
        transaction
            .client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .map_err(error)?;
        Ok(transaction)
    }

    pub async fn validate_keys(&self, table: &str, keys: &[Row]) -> Result<(), QueryError> {
        let transaction = self.transaction().await?;
        transaction.validate_keys(table, keys).await?;
        transaction.rollback().await
    }

    pub async fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        let mut transaction = self.transaction().await?;
        let result = transaction.execute(statements).await?;
        transaction.commit().await?;
        Ok(result)
    }
}

pub struct PostgresTransaction {
    client: tokio_postgres::Client,
    driver: JoinHandle<()>,
    failed: bool,
}

impl Drop for PostgresTransaction {
    fn drop(&mut self) {
        // A dedicated connection makes cancellation and drop close the transaction, never reuse it.
        self.driver.abort();
    }
}

impl PostgresTransaction {
    pub fn begin_translation(&mut self) -> Result<(), QueryError> {
        if self.failed {
            return Err(QueryError::new(
                QueryErrorKind::Rejected,
                "PostgreSQL transaction is aborted",
            ));
        }
        self.failed = true;
        Ok(())
    }

    pub fn finish_translation(&mut self) {
        self.failed = false;
    }

    pub async fn validate_keys(&self, table: &str, keys: &[Row]) -> Result<(), QueryError> {
        if self.failed {
            return Err(QueryError::new(
                QueryErrorKind::Rejected,
                "PostgreSQL transaction is aborted",
            ));
        }
        if keys.is_empty() {
            return Ok(());
        }
        let table = format!("\"{}\"", table.replace('"', "\"\""));
        let columns = self.client.query(
            "SELECT a.atttypid FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, position) JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum WHERE i.indrelid = to_regclass($1) AND i.indisprimary ORDER BY k.position",
            &[&table],
        ).await.map_err(error)?;
        if columns.is_empty() {
            return Err(QueryError::new(
                QueryErrorKind::Validation,
                "PostgreSQL table or primary key does not exist",
            ));
        }
        let types = columns
            .iter()
            .map(|column| {
                let oid: u32 = column.get(0);
                Type::from_oid(oid)
                    .ok_or_else(|| crate::postgres_statement::unsupported("primary key type"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            parameters(&key.values, &types)?;
        }
        // PostgreSQL serializes writes; it has no retained CRDT concurrent values.
        Ok(())
    }

    pub async fn execute(
        &mut self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        if self.failed {
            return Err(QueryError::new(
                QueryErrorKind::Rejected,
                "PostgreSQL transaction is aborted",
            ));
        }
        // Mark failed before await so cancellation cannot leave a committable partial batch.
        self.failed = true;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            let compiled = PostgresStatement::translate(&statement)?;
            let prepared = self.client.prepare(&compiled.sql).await.map_err(error)?;
            let params = parameters(&compiled.params, prepared.params())?;
            let refs = params
                .iter()
                .map(|value| value.as_ref() as &(dyn ToSql + Sync))
                .collect::<Vec<_>>();
            let rows = self.client.query(&prepared, &refs).await.map_err(error)?;
            let columns = prepared
                .columns()
                .iter()
                .map(|column| QueryResultColumn {
                    name: column.name().into(),
                    source_table: None,
                    source_column: None,
                })
                .collect();
            let rows = rows.iter().map(decode_row).collect::<Result<Vec<_>, _>>()?;
            results.push(QueryResult::new_with_columns(rows, columns));
        }
        self.failed = false;
        Ok(results)
    }

    pub async fn commit(self) -> Result<(), QueryError> {
        if self.failed {
            return Err(QueryError::new(
                QueryErrorKind::Rejected,
                "PostgreSQL transaction is aborted",
            ));
        }
        self.client.batch_execute("COMMIT").await.map_err(error)
    }

    pub async fn rollback(self) -> Result<(), QueryError> {
        self.client.batch_execute("ROLLBACK").await.map_err(error)
    }
}

type Parameter = Box<dyn ToSql + Sync + Send>;

fn parameters(values: &[Value], types: &[Type]) -> Result<Vec<Parameter>, QueryError> {
    if values.len() != types.len() {
        return Err(QueryError::new(
            QueryErrorKind::Validation,
            "PostgreSQL parameter count mismatch",
        ));
    }
    values
        .iter()
        .zip(types)
        .map(|(value, kind)| parameter(value, kind))
        .collect()
}

fn parameter(value: &Value, kind: &Type) -> Result<Parameter, QueryError> {
    let invalid = || {
        QueryError::new(
            QueryErrorKind::Validation,
            format!("incompatible PostgreSQL parameter type: {kind}"),
        )
    };
    if *value == Value::Null {
        return Ok(match *kind {
            Type::BOOL => Box::new(None::<bool>),
            Type::INT2 => Box::new(None::<i16>),
            Type::INT4 => Box::new(None::<i32>),
            Type::INT8 => Box::new(None::<i64>),
            Type::FLOAT4 => Box::new(None::<f32>),
            Type::FLOAT8 => Box::new(None::<f64>),
            Type::UUID => Box::new(None::<uuid::Uuid>),
            Type::BYTEA => Box::new(None::<Vec<u8>>),
            Type::JSON | Type::JSONB => Box::new(None::<serde_json::Value>),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR => Box::new(None::<String>),
            _ => return Err(invalid()),
        });
    }
    match (value, kind) {
        (Value::Integer(value), &Type::INT8) => Ok(Box::new(*value)),
        (Value::Integer(value), &Type::INT4) => {
            Ok(Box::new(i32::try_from(*value).map_err(|_| invalid())?))
        }
        (Value::Integer(value), &Type::INT2) => {
            Ok(Box::new(i16::try_from(*value).map_err(|_| invalid())?))
        }
        (Value::Bool(value), &Type::BOOL) => Ok(Box::new(*value)),
        (Value::Bool(value), &Type::INT8) => Ok(Box::new(i64::from(*value))),
        (Value::Integer(value), &Type::BOOL) if *value == 0 || *value == 1 => {
            Ok(Box::new(*value == 1))
        }
        (Value::Float(value), &Type::FLOAT8) => Ok(Box::new(*value)),
        (Value::Float(value), &Type::FLOAT4) => Ok(Box::new(*value as f32)),
        (Value::Uuid(value), &Type::UUID) => Ok(Box::new(*value)),
        (Value::Text(value), &Type::UUID) => Ok(Box::new(
            uuid::Uuid::parse_str(value).map_err(|_| invalid())?,
        )),
        (Value::Uuid(value), &Type::TEXT) => Ok(Box::new(value.to_string())),
        (Value::Text(value), &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR) => {
            Ok(Box::new(value.clone()))
        }
        (Value::Blob(value), &Type::BYTEA) => Ok(Box::new(value.clone())),
        (Value::Json(value), &Type::JSON | &Type::JSONB) => Ok(Box::new(
            serde_json::from_str::<serde_json::Value>(&value.to_string()).map_err(|_| invalid())?,
        )),
        _ => Err(invalid()),
    }
}

fn decode_row(row: &tokio_postgres::Row) -> Result<Row, QueryError> {
    let mut values = Vec::with_capacity(row.len());
    for (index, column) in row.columns().iter().enumerate() {
        macro_rules! decode {
            ($type:ty, $convert:expr) => {
                row.try_get::<_, Option<$type>>(index)
                    .map_err(error)?
                    .map($convert)
                    .unwrap_or(Value::Null)
            };
        }
        values.push(match *column.type_() {
            Type::BOOL => decode!(bool, Value::Bool),
            Type::INT2 => decode!(i16, |v| Value::Integer(i64::from(v))),
            Type::INT4 => decode!(i32, |v| Value::Integer(i64::from(v))),
            Type::INT8 => decode!(i64, Value::Integer),
            Type::FLOAT4 => decode!(f32, |v| Value::Float(f64::from(v))),
            Type::FLOAT8 => decode!(f64, Value::Float),
            Type::UUID => decode!(uuid::Uuid, Value::Uuid),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR => decode!(String, Value::Text),
            Type::BYTEA => decode!(Vec<u8>, Value::Blob),
            Type::JSON | Type::JSONB => decode!(serde_json::Value, |v| Value::Json(v.into())),
            _ => return Err(crate::postgres_statement::unsupported("result type")),
        });
    }
    Ok(Row::new(values))
}

fn error(error: tokio_postgres::Error) -> QueryError {
    let kind = if error.as_db_error().is_some() {
        QueryErrorKind::Rejected
    } else {
        QueryErrorKind::Transport
    };
    QueryError::new(kind, error.to_string())
}

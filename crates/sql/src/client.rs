use std::{time::Duration, vec::Vec};

use client::Client as SqlClient;
#[cfg(feature = "sql")]
use query::QueryParams;
use query::{QueryError, QueryErrorKind, QueryResult, Statement};

/// A query-only remote SQL client.
///
/// It does not expose embedded transactions:
/// ```compile_fail
/// fn no_transaction(client: &ofdb_sql::Client) {
///     let _ = client.transaction();
/// }
/// ```
///
/// It does not expose sync setup:
/// ```compile_fail
/// fn no_sync(client: &ofdb_sql::Client) {
///     let _ = client.synchronize();
/// }
/// ```
///
/// The SQL facade does not export KV handles:
/// ```compile_fail
/// use ofdb_sql::KvDatabase;
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    inner: SqlClient,
    request_deadline: Option<Duration>,
}

impl Client {
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.request_deadline = Some(deadline);
        self
    }

    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self, QueryError> {
        Ok(Self {
            inner: SqlClient::tcp(endpoint).await?,
            request_deadline: None,
        })
    }

    #[cfg(all(unix, feature = "remote"))]
    pub async fn connect_unix(path: impl Into<std::path::PathBuf>) -> Result<Self, QueryError> {
        Ok(Self {
            inner: SqlClient::unix(path).await?,
            request_deadline: None,
        })
    }

    pub async fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        if statements.is_empty() {
            return Err(QueryError::new(
                QueryErrorKind::Validation,
                "statement batch must not be empty",
            ));
        }
        match self.request_deadline {
            Some(deadline) => tokio::time::timeout(deadline, self.inner.execute(statements))
                .await
                .map_err(|_| {
                    QueryError::new(QueryErrorKind::Timeout, "request deadline exceeded")
                })?,
            None => self.inner.execute(statements).await,
        }
    }

    #[cfg(feature = "sql")]
    pub async fn execute_sql(
        &self,
        sql: &str,
        params: Option<&QueryParams>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        use query::Translator;

        let statements = sql_translator::SqlTranslator
            .translate_with_params(sql, params)
            .await
            .map_err(|error| QueryError::new(QueryErrorKind::Validation, error.to_string()))?;
        self.execute(statements).await
    }
}

use core::future::Future;
use std::{fmt, time::Duration, vec::Vec};

#[cfg(feature = "remote")]
use client::Client as SqlClient;
use query::{QueryError, QueryErrorKind, QueryParams, QueryResult, Statement, Translator};
use value::{FromRow, Row, Value};

/// A query client for embedded or remote databases.
///
/// Use [`Client::transaction`] for a transaction on either backend. A configured deadline
/// requires a Tokio runtime with time enabled. Without a deadline, embedded queries do not need
/// a Tokio runtime. A timeout does not guarantee rollback.
///
/// `Database` is not a query or transaction handle:
/// ```compile_fail
/// fn no_database_query(database: &ofdb_sql::Database) {
///     let _ = database.execute(Vec::new());
///     let _ = database.transaction();
/// }
/// ```
///
/// It does not expose hosting or sync:
/// ```compile_fail
/// fn no_control(client: &ofdb_sql::Client) {
///     let _ = client.serve_tcp;
///     let _ = client.synchronize;
/// }
/// ```
///
/// It has no public database accessor:
/// ```compile_fail
/// fn no_database(client: &ofdb_sql::Client) {
///     let _ = client.database();
/// }
/// ```
#[derive(Clone)]
pub struct Client {
    backend: Backend,
    request_deadline: Option<Duration>,
}

#[derive(Clone)]
enum Backend {
    #[cfg(any(feature = "redb", feature = "in-memory"))]
    Embedded(crate::Database),
    #[cfg(feature = "remote")]
    Remote(SqlClient),
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field(
                "backend",
                &match self.backend {
                    #[cfg(any(feature = "redb", feature = "in-memory"))]
                    Backend::Embedded(_) => "Embedded",
                    #[cfg(feature = "remote")]
                    Backend::Remote(_) => "Remote",
                },
            )
            .field("request_deadline", &self.request_deadline)
            .finish()
    }
}

/// A SQL transaction created by [`Client::transaction`].
///
/// Dropping an uncommitted remote transaction closes its stream and rolls it back on the server.
pub struct Transaction {
    backend: TransactionBackend,
    request_deadline: Option<Duration>,
}

enum TransactionBackend {
    #[cfg(any(feature = "redb", feature = "in-memory"))]
    Embedded(crate::database::EmbeddedTransaction),
    #[cfg(feature = "remote")]
    Remote(client::Transaction),
}

impl Transaction {
    /// Inspects conflicts in this embedded transaction's snapshot, including local writes.
    /// Aborted transactions fail; remote conflict operations are unsupported.
    pub async fn row_conflicts(&self, table: &str, key: &Row) -> Result<Vec<String>, QueryError> {
        #[cfg(not(any(feature = "redb", feature = "in-memory")))]
        let _ = (table, key);
        Self::run(self.request_deadline, async {
            match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                TransactionBackend::Embedded(transaction) => {
                    transaction.row_conflicts(table, key).await
                }
                #[cfg(feature = "remote")]
                TransactionBackend::Remote(_) => Err(QueryError::new(
                    QueryErrorKind::Unsupported,
                    "remote conflict inspection is unsupported",
                )),
            }
        })
        .await
    }

    /// Returns concurrent values from this embedded transaction's snapshot.
    /// Aborted transactions fail; remote conflict operations are unsupported.
    pub async fn row_conflict_values(
        &self,
        table: &str,
        key: &Row,
    ) -> Result<Vec<(String, Vec<Value>)>, QueryError> {
        #[cfg(not(any(feature = "redb", feature = "in-memory")))]
        let _ = (table, key);
        Self::run(self.request_deadline, async {
            match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                TransactionBackend::Embedded(transaction) => {
                    transaction.row_conflict_values(table, key).await
                }
                #[cfg(feature = "remote")]
                TransactionBackend::Remote(_) => Err(QueryError::new(
                    QueryErrorKind::Unsupported,
                    "remote conflict inspection is unsupported",
                )),
            }
        })
        .await
    }

    async fn run<F, T>(deadline: Option<Duration>, operation: F) -> Result<T, QueryError>
    where
        F: Future<Output = Result<T, QueryError>>,
    {
        match deadline {
            Some(deadline) => tokio::time::timeout(deadline, operation)
                .await
                .map_err(|_| {
                    QueryError::new(QueryErrorKind::Timeout, "request deadline exceeded")
                })?,
            None => operation.await,
        }
    }

    async fn execute_untimed(
        &mut self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        if statements.is_empty() {
            return Err(QueryError::new(
                QueryErrorKind::Validation,
                "statement batch must not be empty",
            ));
        }
        match &mut self.backend {
            #[cfg(any(feature = "redb", feature = "in-memory"))]
            TransactionBackend::Embedded(transaction) => transaction.execute(statements).await,
            #[cfg(feature = "remote")]
            TransactionBackend::Remote(transaction) => transaction.execute(statements).await,
        }
    }

    pub async fn execute(
        &mut self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        Self::run(self.request_deadline, self.execute_untimed(statements)).await
    }

    async fn execute_translated<F>(
        &mut self,
        translation: F,
    ) -> Result<Vec<QueryResult>, QueryError>
    where
        F: Future<Output = Result<Vec<Statement>, QueryError>>,
    {
        let translation = async {
            let statements = translation.await?;
            if statements.is_empty() {
                return Err(QueryError::new(
                    QueryErrorKind::Validation,
                    "statement batch must not be empty",
                ));
            }
            Ok(statements)
        };
        match &mut self.backend {
            #[cfg(any(feature = "redb", feature = "in-memory"))]
            TransactionBackend::Embedded(transaction) => {
                transaction.execute_translated(translation).await
            }
            #[cfg(feature = "remote")]
            TransactionBackend::Remote(transaction) => {
                transaction.execute(translation.await?).await
            }
        }
    }

    pub async fn translate_and_execute<T: Translator>(
        &mut self,
        query: &str,
        translator: &T,
    ) -> Result<Vec<QueryResult>, QueryError> {
        Self::run(self.request_deadline, async {
            let translation = async {
                translator.translate(query).await.map_err(|error| {
                    QueryError::new(
                        QueryErrorKind::Validation,
                        format!("Translate error: {error}"),
                    )
                })
            };
            self.execute_translated(translation).await
        })
        .await
    }

    pub async fn translate_and_execute_with_params<T: Translator>(
        &mut self,
        query: &str,
        params: Option<&QueryParams>,
        translator: &T,
    ) -> Result<Vec<QueryResult>, QueryError> {
        Self::run(self.request_deadline, async {
            let translation = async {
                translator
                    .translate_with_params(query, params)
                    .await
                    .map_err(|error| {
                        QueryError::new(
                            QueryErrorKind::Validation,
                            format!("Translate error: {error}"),
                        )
                    })
            };
            self.execute_translated(translation).await
        })
        .await
    }

    #[cfg(feature = "sql")]
    pub async fn execute_sql(
        &mut self,
        sql: &str,
        params: Option<&QueryParams>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        self.translate_and_execute_with_params(sql, params, &sql_translator::SqlTranslator)
            .await
    }

    pub async fn commit(self) -> Result<(), QueryError> {
        let deadline = self.request_deadline;
        Self::run(deadline, async move {
            match self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                TransactionBackend::Embedded(transaction) => transaction.commit().await,
                #[cfg(feature = "remote")]
                TransactionBackend::Remote(transaction) => transaction.commit().await,
            }
        })
        .await
    }

    pub async fn rollback(self) -> Result<(), QueryError> {
        let deadline = self.request_deadline;
        Self::run(deadline, async move {
            match self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                TransactionBackend::Embedded(transaction) => transaction.rollback().await,
                #[cfg(feature = "remote")]
                TransactionBackend::Remote(transaction) => transaction.rollback().await,
            }
        })
        .await
    }
}

impl Client {
    /// Returns conflicted column names from one embedded read snapshot.
    /// An empty result means no conflicts, not that the row exists or is authorized.
    /// Remote conflict operations are unsupported.
    pub async fn row_conflicts(&self, table: &str, key: &Row) -> Result<Vec<String>, QueryError> {
        #[cfg(not(any(feature = "redb", feature = "in-memory")))]
        let _ = (table, key);
        self.run(async {
            match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                Backend::Embedded(database) => database.query_row_conflicts(table, key).await,
                #[cfg(feature = "remote")]
                Backend::Remote(_) => Err(QueryError::new(
                    QueryErrorKind::Unsupported,
                    "remote conflict inspection is unsupported",
                )),
            }
        })
        .await
    }

    /// Returns all concurrent values for each conflicted column, not just its projection.
    /// Caller-owned security policy decides whether a conflict permits an operation.
    /// Remote conflict operations are unsupported.
    pub async fn row_conflict_values(
        &self,
        table: &str,
        key: &Row,
    ) -> Result<Vec<(String, Vec<Value>)>, QueryError> {
        #[cfg(not(any(feature = "redb", feature = "in-memory")))]
        let _ = (table, key);
        self.run(async {
            match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                Backend::Embedded(database) => database.query_row_conflict_values(table, key).await,
                #[cfg(feature = "remote")]
                Backend::Remote(_) => Err(QueryError::new(
                    QueryErrorKind::Unsupported,
                    "remote conflict inspection is unsupported",
                )),
            }
        })
        .await
    }

    /// Resolves only the specified conflicted columns in one atomic operation.
    /// Invalid assignments roll back the operation; other conflicts remain intact.
    /// Caller must authorize resolution and enforce lifecycle/audit policy.
    /// Remote conflict operations are unsupported.
    pub async fn resolve_row(
        &self,
        table: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> Result<(), QueryError> {
        #[cfg(not(any(feature = "redb", feature = "in-memory")))]
        let _ = (table, key, &values);
        self.run(async {
            match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                Backend::Embedded(database) => database.query_resolve_row(table, key, values).await,
                #[cfg(feature = "remote")]
                Backend::Remote(_) => Err(QueryError::new(
                    QueryErrorKind::Unsupported,
                    "remote conflict resolution is unsupported",
                )),
            }
        })
        .await
    }

    pub async fn transaction(&self) -> Result<Transaction, QueryError> {
        self.run(async {
            let backend = match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                Backend::Embedded(database) => {
                    TransactionBackend::Embedded(database.begin_transaction().await?)
                }
                #[cfg(feature = "remote")]
                Backend::Remote(client) => TransactionBackend::Remote(client.transaction().await?),
            };
            Ok(Transaction {
                backend,
                request_deadline: self.request_deadline,
            })
        })
        .await
    }
    #[cfg(any(feature = "redb", feature = "in-memory"))]
    pub(crate) fn embedded(database: crate::Database) -> Self {
        Self {
            backend: Backend::Embedded(database),
            request_deadline: None,
        }
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.request_deadline = Some(deadline);
        self
    }

    #[cfg(feature = "remote")]
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self, QueryError> {
        Ok(Self {
            backend: Backend::Remote(SqlClient::tcp(endpoint).await?),
            request_deadline: None,
        })
    }

    #[cfg(feature = "remote")]
    pub async fn connect_with_ca(
        endpoint: impl AsRef<str>,
        ca_certificate: impl AsRef<[u8]>,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            backend: Backend::Remote(SqlClient::tcp_with_ca(endpoint, ca_certificate).await?),
            request_deadline: None,
        })
    }

    #[cfg(feature = "remote")]
    pub async fn connect_with_identity(
        endpoint: impl AsRef<str>,
        ca_certificate: impl AsRef<[u8]>,
        certificate: impl AsRef<[u8]>,
        private_key: impl AsRef<[u8]>,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            backend: Backend::Remote(
                SqlClient::tcp_with_identity(endpoint, ca_certificate, certificate, private_key)
                    .await?,
            ),
            request_deadline: None,
        })
    }

    #[cfg(all(unix, feature = "remote"))]
    pub async fn connect_unix(path: impl Into<std::path::PathBuf>) -> Result<Self, QueryError> {
        Ok(Self {
            backend: Backend::Remote(SqlClient::unix(path).await?),
            request_deadline: None,
        })
    }

    async fn run<F, T>(&self, operation: F) -> Result<T, QueryError>
    where
        F: Future<Output = Result<T, QueryError>>,
    {
        match self.request_deadline {
            Some(deadline) => tokio::time::timeout(deadline, operation)
                .await
                .map_err(|_| {
                    QueryError::new(QueryErrorKind::Timeout, "request deadline exceeded")
                })?,
            None => operation.await,
        }
    }

    async fn execute_untimed(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        if statements.is_empty() {
            return Err(QueryError::new(
                QueryErrorKind::Validation,
                "statement batch must not be empty",
            ));
        }
        match &self.backend {
            #[cfg(any(feature = "redb", feature = "in-memory"))]
            Backend::Embedded(database) => database.query_execute_untimed(statements).await,
            #[cfg(feature = "remote")]
            Backend::Remote(client) => client.execute(statements).await,
        }
    }

    pub async fn execute(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        self.run(self.execute_untimed(statements)).await
    }

    pub async fn translate_and_execute<T: Translator>(
        &self,
        query: &str,
        translator: &T,
    ) -> Result<Vec<QueryResult>, QueryError> {
        self.run(async {
            match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                Backend::Embedded(database) => {
                    database
                        .query_translate_and_execute(query, translator)
                        .await
                }
                #[cfg(feature = "remote")]
                Backend::Remote(_) => {
                    let statements = translator.translate(query).await.map_err(|error| {
                        QueryError::new(
                            QueryErrorKind::Validation,
                            format!("Translate error: {error}"),
                        )
                    })?;
                    self.execute_untimed(statements).await
                }
            }
        })
        .await
    }

    pub async fn translate_and_execute_with_params<T: Translator>(
        &self,
        query: &str,
        params: Option<&QueryParams>,
        translator: &T,
    ) -> Result<Vec<QueryResult>, QueryError> {
        self.run(async {
            match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                Backend::Embedded(database) => {
                    database
                        .query_translate_and_execute_with_params(query, params, translator)
                        .await
                }
                #[cfg(feature = "remote")]
                Backend::Remote(_) => {
                    let statements = translator
                        .translate_with_params(query, params)
                        .await
                        .map_err(|error| {
                            QueryError::new(
                                QueryErrorKind::Validation,
                                format!("Translate error: {error}"),
                            )
                        })?;
                    self.execute_untimed(statements).await
                }
            }
        })
        .await
    }

    pub async fn translate_and_select<T: Translator, U: FromRow>(
        &self,
        query: &str,
        translator: &T,
    ) -> Result<Vec<U>, QueryError> {
        self.run(async {
            match &self.backend {
                #[cfg(any(feature = "redb", feature = "in-memory"))]
                Backend::Embedded(database) => {
                    database.query_translate_and_select(query, translator).await
                }
                #[cfg(feature = "remote")]
                Backend::Remote(_) => {
                    let mut results = self
                        .translate_and_execute_untimed(query, translator)
                        .await?;
                    if results.len() != 1 {
                        return Err(QueryError::new(
                            QueryErrorKind::Rejected,
                            "Invalid query: Expected one query result",
                        ));
                    }
                    results
                        .pop()
                        .expect("result count was checked")
                        .rows_as()
                        .map_err(|error| {
                            QueryError::new(QueryErrorKind::Storage, error.to_string())
                        })
                }
            }
        })
        .await
    }

    #[cfg(feature = "remote")]
    async fn translate_and_execute_untimed<T: Translator>(
        &self,
        query: &str,
        translator: &T,
    ) -> Result<Vec<QueryResult>, QueryError> {
        let statements = translator.translate(query).await.map_err(|error| {
            QueryError::new(
                QueryErrorKind::Validation,
                format!("Translate error: {error}"),
            )
        })?;
        self.execute_untimed(statements).await
    }

    #[cfg(feature = "sql")]
    pub async fn execute_sql(
        &self,
        sql: &str,
        params: Option<&QueryParams>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        self.translate_and_execute_with_params(sql, params, &sql_translator::SqlTranslator)
            .await
    }
}

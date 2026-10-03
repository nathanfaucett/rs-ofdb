use proto::{
    ExecuteRequest, TransactionRequest, TransactionResponse, transaction_request::Command,
    transaction_response::Outcome,
};
use protocol::{decode_error_detail, query_result_from_proto, statement_to_proto};
use query::{QueryError, QueryErrorKind, QueryResult, Statement};
use tokio::sync::mpsc;

use crate::client::status_error;

/// A remote transaction. Dropping this handle closes its stream without commit.
#[derive(Debug)]
pub struct Transaction {
    sender: Option<mpsc::Sender<TransactionRequest>>,
    responses: tonic::Streaming<TransactionResponse>,
}

impl Transaction {
    pub(crate) async fn new(
        sender: mpsc::Sender<TransactionRequest>,
        responses: tonic::Streaming<TransactionResponse>,
    ) -> Result<Self, QueryError> {
        let mut transaction = Self {
            sender: Some(sender),
            responses,
        };
        match transaction.response().await? {
            Outcome::Ready(()) => Ok(transaction),
            _ => Err(protocol_error("expected transaction ready response")),
        }
    }

    pub async fn execute(
        &mut self,
        statements: Vec<Statement>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        match self
            .exchange(Command::Execute(ExecuteRequest {
                statements: statements.into_iter().map(statement_to_proto).collect(),
            }))
            .await?
        {
            Outcome::Executed(response) => response
                .results
                .into_iter()
                .map(|result| {
                    query_result_from_proto(result)
                        .map_err(|error| protocol_error(&error.to_string()))
                })
                .collect(),
            _ => Err(protocol_error("expected transaction execute response")),
        }
    }

    pub async fn commit(mut self) -> Result<(), QueryError> {
        self.finish(Command::Commit(())).await
    }

    pub async fn rollback(mut self) -> Result<(), QueryError> {
        self.finish(Command::Rollback(())).await
    }

    async fn finish(&mut self, command: Command) -> Result<(), QueryError> {
        match self.exchange(command).await? {
            Outcome::Completed(()) => Ok(()),
            _ => Err(protocol_error("expected transaction completion response")),
        }
    }

    async fn exchange(&mut self, command: Command) -> Result<Outcome, QueryError> {
        // Cancellation must close the request stream, not leave an unread response for the next call.
        let sender = self
            .sender
            .take()
            .ok_or_else(|| QueryError::new(QueryErrorKind::Rejected, "Transaction is closed"))?;
        sender
            .send(TransactionRequest {
                command: Some(command),
            })
            .await
            .map_err(|_| QueryError::new(QueryErrorKind::Transport, "transaction stream closed"))?;
        let outcome = self.response().await?;
        self.sender = Some(sender);
        match outcome {
            Outcome::ErrorDetail(detail) => {
                let (kind, message) = decode_error_detail(&detail)
                    .ok_or_else(|| protocol_error("invalid transaction error detail"))?;
                Err(QueryError::new(kind, message))
            }
            outcome => Ok(outcome),
        }
    }

    async fn response(&mut self) -> Result<Outcome, QueryError> {
        self.responses
            .message()
            .await
            .map_err(status_error)?
            .ok_or_else(|| QueryError::new(QueryErrorKind::Transport, "transaction stream closed"))?
            .outcome
            .ok_or_else(|| protocol_error("transaction outcome is missing"))
    }
}

fn protocol_error(message: &str) -> QueryError {
    QueryError::new(QueryErrorKind::Protocol, message)
}

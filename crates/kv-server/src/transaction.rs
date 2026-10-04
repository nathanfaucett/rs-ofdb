use btree::BTreeTransaction;
use kv::KvTransaction;
use kv_proto::kvdb::{
    GetResponse, ScanEntry, ScanResponse, TransactionRequest, TransactionResponse,
    transaction_request::Command, transaction_response::Outcome,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};

use crate::server::{current_time_millis, status_from_error};

pub(crate) type TransactionStream = ReceiverStream<Result<TransactionResponse, Status>>;

pub(crate) fn stream<T>(
    mut transaction: KvTransaction<T>,
    mut requests: Streaming<TransactionRequest>,
) -> TransactionStream
where
    T: BTreeTransaction<Vec<u8>, Vec<u8>> + 'static,
{
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        if sender
            .send(Ok(TransactionResponse {
                outcome: Some(Outcome::Ready(())),
            }))
            .await
            .is_err()
        {
            let _ = transaction.rollback().await;
            return;
        }
        loop {
            let request = tokio::select! {
                _ = sender.closed() => break,
                request = requests.message() => match request {
                    Ok(Some(request)) => request,
                    _ => break,
                },
            };
            let outcome = match request.command {
                Some(Command::Commit(())) | Some(Command::Rollback(())) => {
                    let result = if matches!(request.command, Some(Command::Commit(()))) {
                        transaction.commit().await
                    } else {
                        transaction.rollback().await
                    };
                    let outcome = match result {
                        Ok(()) => Outcome::Completed(()),
                        Err(error) => error_outcome(status_from_error(error)),
                    };
                    let _ = sender
                        .send(Ok(TransactionResponse {
                            outcome: Some(outcome),
                        }))
                        .await;
                    return;
                }
                Some(command) => {
                    let result = tokio::select! {
                        _ = sender.closed() => break,
                        result = execute(&mut transaction, command) => result,
                    };
                    result.unwrap_or_else(error_outcome)
                }
                None => Outcome::ErrorDetail(kv_proto::encode_error_details(
                    kv_proto::ErrorKind::Internal,
                    "transaction command is missing",
                )),
            };
            if sender
                .send(Ok(TransactionResponse {
                    outcome: Some(outcome),
                }))
                .await
                .is_err()
            {
                break;
            }
        }
        let _ = transaction.rollback().await;
    });
    ReceiverStream::new(receiver)
}

async fn execute<T>(transaction: &mut KvTransaction<T>, command: Command) -> Result<Outcome, Status>
where
    T: BTreeTransaction<Vec<u8>, Vec<u8>>,
{
    let entries = match command {
        Command::Get(request) => {
            let value = transaction
                .get(&request.key, current_time_millis()?)
                .await
                .map_err(status_from_error)?;
            return Ok(Outcome::Got(GetResponse {
                value: value.map(kv_proto::value_to_proto),
            }));
        }
        Command::Set(request) => {
            transaction
                .set(
                    &request.key,
                    kv_proto::required_value(request.value)?,
                    request.expires_at,
                )
                .await
                .map_err(status_from_error)?;
            return Ok(Outcome::Completed(()));
        }
        Command::Delete(request) => {
            transaction
                .delete(&request.key)
                .await
                .map_err(status_from_error)?;
            return Ok(Outcome::Completed(()));
        }
        Command::Scan(request) => {
            transaction
                .scan(request.start..request.end, current_time_millis()?)
                .await
        }
        Command::ScanPrefix(request) => {
            transaction
                .scan_prefix(&request.prefix, current_time_millis()?)
                .await
        }
        Command::ScanAll(_) => transaction.scan_all(current_time_millis()?).await,
        Command::Commit(()) | Command::Rollback(()) => {
            unreachable!("terminal commands are handled by the transaction stream")
        }
    }
    .map_err(status_from_error)?;
    Ok(Outcome::Scanned(ScanResponse {
        entries: entries
            .into_iter()
            .map(|(key, value)| ScanEntry {
                key,
                value: Some(kv_proto::value_to_proto(value)),
            })
            .collect(),
    }))
}

fn error_outcome(status: Status) -> Outcome {
    Outcome::ErrorDetail(status.details().to_vec())
}

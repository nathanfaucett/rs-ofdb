#![cfg(all(feature = "sync", feature = "sql", feature = "in-memory"))]

use ofdb_sql::{Database, SessionConfig, SyncRole, SyncTransport};
use tokio::sync::mpsc;

struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}
impl SyncTransport for Channel {
    type Error = &'static str;
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sender.send(frame).map_err(|_| "closed")
    }
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.recv().await.ok_or("closed")
    }
}
fn channels() -> (Channel, Channel) {
    let (a, ar) = mpsc::unbounded_channel();
    let (b, br) = mpsc::unbounded_channel();
    (
        Channel {
            sender: a,
            receiver: br,
        },
        Channel {
            sender: b,
            receiver: ar,
        },
    )
}
struct FlipFactory(std::sync::Arc<std::sync::atomic::AtomicBool>);
struct FlipStage {
    allowed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    records: std::collections::VecDeque<Vec<u8>>,
    finished: bool,
}
impl ofdb_sql::SyncStageFactory for FlipFactory {
    type Stage = FlipStage;
    type Error = &'static str;
    async fn create(&self) -> Result<FlipStage, Self::Error> {
        Ok(FlipStage {
            allowed: self.0.clone(),
            records: Default::default(),
            finished: false,
        })
    }
}
impl ofdb_sql::SyncStage for FlipStage {
    type Error = &'static str;
    async fn append(&mut self, record: Vec<u8>) -> Result<(), Self::Error> {
        if self.finished {
            return Err("finished");
        }
        self.records.push_back(record);
        Ok(())
    }
    async fn finish(&mut self) -> Result<(), Self::Error> {
        self.finished = true;
        Ok(())
    }
    async fn next_record(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        if !self.finished {
            return Err("not finished");
        }
        self.allowed
            .store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(self.records.pop_front())
    }
}
#[tokio::test]
async fn decision_change_during_authorized_catalog_does_not_interrupt_commit() {
    let source = Database::in_memory();
    let target = Database::in_memory();
    seed(&source).await;
    let allowed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let factory = FlipFactory(allowed.clone());
    let mut calls = 0;
    let mut check = || {
        calls += 1;
        core::future::ready(if allowed.load(std::sync::atomic::Ordering::SeqCst) {
            Ok(())
        } else {
            Err("decision changed".into())
        })
    };
    let config = SessionConfig::default();
    let (mut st, mut tt) = channels();
    let (_, result) = tokio::join!(
        source.synchronize(&mut st, &config, SyncRole::Initiator),
        target.synchronize_with_stage_factory_checked(
            &mut tt,
            &config,
            SyncRole::Responder,
            &factory,
            &mut check
        )
    );
    assert!(matches!(result, Err(ofdb_sql::SyncError::PreApply(_))));
    assert_eq!(calls, 2);
    let results = target
        .client()
        .execute_sql("SELECT * FROM users", None)
        .await
        .expect("catalog committed despite changed decision");
    assert!(results[0].rows.is_empty(), "later data operation denied");
}

async fn seed(db: &Database) {
    db.client()
        .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", None)
        .await
        .expect("schema");
    db.client().execute_sql("INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')", None).await.expect("row");
}
#[tokio::test]
async fn memory_checks_each_catalog_and_data_operation_in_both_roles() {
    for role in [SyncRole::Initiator, SyncRole::Responder] {
        for allowed in [0, 1, 2] {
            let source = Database::in_memory();
            let target = Database::in_memory();
            seed(&source).await;
            let before = target.export_sync_state().await.expect("before");
            let (mut st, mut tt) = channels();
            let config = SessionConfig {
                max_units_per_frame: 1,
                ..SessionConfig::default()
            };
            let mut calls = 0;
            let mut check = || {
                calls += 1;
                core::future::ready(if calls <= allowed {
                    Ok(())
                } else {
                    Err("caller denied or canceled".to_string())
                })
            };
            let source_role = if role == SyncRole::Initiator {
                SyncRole::Responder
            } else {
                SyncRole::Initiator
            };
            let (sr, tr) = tokio::join!(
                source.synchronize(&mut st, &config, source_role),
                target.synchronize_checked(&mut tt, &config, role, &mut check)
            );
            if allowed == 2 {
                sr.expect("source");
                tr.expect("target");
                assert_eq!(
                    source.export_sync_state().await.expect("source state"),
                    target.export_sync_state().await.expect("target state")
                );
            } else {
                assert!(matches!(tr, Err(ofdb_sql::SyncError::PreApply(_))));
                if allowed == 0 {
                    assert_eq!(target.export_sync_state().await.expect("unchanged"), before);
                } else {
                    let results = target
                        .client()
                        .execute_sql("SELECT * FROM users", None)
                        .await
                        .expect("catalog remains committed");
                    assert!(results[0].rows.is_empty());
                }
            }
            assert_eq!(calls, if allowed == 0 { 1 } else { 2 });
        }
    }
}

#[cfg(feature = "native-stage")]
#[tokio::test]
async fn native_facade_replays_catalog_and_rows_and_cleans_files() {
    let a = Database::in_memory();
    let b = Database::in_memory();
    seed(&a).await;
    let dir = tempfile::tempdir().expect("directory");
    let (mut at, mut bt) = channels();
    let config = SessionConfig {
        max_units_per_frame: 1,
        ..SessionConfig::default()
    };
    let (ar, br) = tokio::join!(
        a.synchronize_with_stage_directory(&mut at, &config, SyncRole::Initiator, dir.path()),
        b.synchronize_with_stage_directory(&mut bt, &config, SyncRole::Responder, dir.path())
    );
    ar.expect("initiator");
    br.expect("responder");
    assert_eq!(
        a.export_sync_state().await.expect("source"),
        b.export_sync_state().await.expect("destination")
    );
    assert_eq!(std::fs::read_dir(dir.path()).expect("listing").count(), 0);
}

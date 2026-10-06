use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io::{Error, ErrorKind, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};

use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets};
use ofdb_sql::{Database, SessionConfig, SyncRole};
use tokio::sync::watch;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const ALPN: &[u8] = b"ofdb/sql/sync/1";

pub async fn run(
    database: Arc<Database>,
    secret_path: std::path::PathBuf,
    peers: Vec<EndpointId>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Error> {
    let secret_key = load_secret_key(&secret_path)?;
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .map_err(Error::other)?;
    let allowed_peers = Arc::new(peers.iter().copied().collect::<HashSet<_>>());
    eprintln!("SQL sync node id: {}", endpoint.id());

    let mut sync_interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else {
                    break;
                };
                let database = Arc::clone(&database);
                let allowed_peers = Arc::clone(&allowed_peers);
                tokio::spawn(async move {
                    let connection = match incoming.accept() {
                        Ok(accepting) => match accepting.await {
                            Ok(connection) => connection,
                            Err(error) => {
                                eprintln!("SQL sync handshake failed: {error}");
                                return;
                            }
                        },
                        Err(error) => {
                            eprintln!("SQL sync connection rejected: {error}");
                            return;
                        }
                    };
                    if !allowed_peers.contains(&connection.remote_id()) {
                        connection.close(0u32.into(), b"peer is not authorized");
                        return;
                    }
                    let (send, receive) = match connection.accept_bi().await {
                        Ok(streams) => streams,
                        Err(error) => {
                            eprintln!("SQL sync stream failed: {error}");
                            return;
                        }
                    };
                    let mut transport = ofdb_sql::IrohTransport::new(send, receive);
                    if let Err(error) = database
                        .synchronize(&mut transport, &SessionConfig::default(), SyncRole::Responder)
                        .await
                    {
                        eprintln!("SQL sync session failed: {error}");
                    }
                });
            }
            _ = sync_interval.tick(), if !peers.is_empty() => {
                for peer in peers.iter().copied() {
                    let endpoint = endpoint.clone();
                    let database = Arc::clone(&database);
                    tokio::spawn(async move {
                        let connection = match endpoint.connect(peer, ALPN).await {
                            Ok(connection) => connection,
                            Err(error) => {
                                eprintln!("SQL sync connect to {peer} failed: {error}");
                                return;
                            }
                        };
                        let (send, receive) = match connection.open_bi().await {
                            Ok(streams) => streams,
                            Err(error) => {
                                eprintln!("SQL sync stream to {peer} failed: {error}");
                                return;
                            }
                        };
                        let mut transport = ofdb_sql::IrohTransport::new(send, receive);
                        if let Err(error) = database
                            .synchronize(&mut transport, &SessionConfig::default(), SyncRole::Initiator)
                            .await
                        {
                            eprintln!("SQL sync session with {peer} failed: {error}");
                        }
                    });
                }
            }
        }
    }

    endpoint.close().await;
    Ok(())
}

fn load_secret_key(path: &Path) -> Result<SecretKey, Error> {
    if path.exists() {
        return SecretKey::try_from(fs::read(path)?.as_slice())
            .map_err(|error| Error::new(ErrorKind::InvalidData, error));
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let secret_key = SecretKey::generate();
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    match options.open(path) {
        Ok(mut file) => {
            file.write_all(&secret_key.to_bytes())?;
            file.sync_all()?;
            Ok(secret_key)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            SecretKey::try_from(fs::read(path)?.as_slice())
                .map_err(|error| Error::new(ErrorKind::InvalidData, error))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use iroh::{Endpoint, EndpointAddr, RelayMode, endpoint::presets};
    use iroh_relay::tls::CaTlsConfig;
    use ofdb_sql::{Database, SessionConfig, SyncRole};

    use super::{ALPN, load_secret_key};

    #[tokio::test]
    async fn sync_session_runs_over_iroh() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock follows Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("ofdb-sql-sync-{nonce}"));
        std::fs::create_dir_all(&directory).expect("create temporary database directory");
        let left =
            Arc::new(Database::open(directory.join("left.redb")).expect("open left database"));
        let right =
            Arc::new(Database::open(directory.join("right.redb")).expect("open right database"));

        let (relay_map, relay_url, _relay) = iroh::test_utils::run_relay_server()
            .await
            .expect("start test relay");
        let initiator = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Custom(relay_map.clone()))
            .ca_tls_config(CaTlsConfig::insecure_skip_verify())
            .bind()
            .await
            .expect("bind initiator");
        let responder = Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(RelayMode::Custom(relay_map))
            .ca_tls_config(CaTlsConfig::insecure_skip_verify())
            .bind()
            .await
            .expect("bind responder");
        initiator.online().await;
        responder.online().await;
        let responder_addr = EndpointAddr::new(responder.id()).with_relay_url(relay_url);
        let (connection, incoming) = tokio::join!(initiator.connect(responder_addr, ALPN), async {
            responder
                .accept()
                .await
                .expect("incoming connection")
                .accept()
                .expect("accept incoming connection")
                .await
        });
        let connection = connection.expect("connect to responder through relay");
        let incoming = incoming.expect("complete relayed incoming connection");
        assert!(
            connection
                .paths()
                .iter()
                .any(|path| path.is_relay() && path.is_selected())
        );
        assert!(
            incoming
                .paths()
                .iter()
                .any(|path| path.is_relay() && path.is_selected())
        );
        let (send, receive) = connection.open_bi().await.expect("open sync stream");
        let mut outgoing = ofdb_sql::IrohTransport::new(send, receive);
        let config = SessionConfig::default();
        let (left_result, right_result) = tokio::join!(
            left.synchronize(&mut outgoing, &config, SyncRole::Initiator),
            async {
                let (send, receive) = incoming.accept_bi().await.expect("accept sync stream");
                let mut transport = ofdb_sql::IrohTransport::new(send, receive);
                right
                    .synchronize(&mut transport, &config, SyncRole::Responder)
                    .await
            }
        );
        left_result.expect("initiator sync succeeds");
        right_result.expect("responder sync succeeds");

        initiator.close().await;
        responder.close().await;
        std::fs::remove_dir_all(directory).expect("remove temporary database directory");
    }

    #[test]
    fn node_identity_survives_restart() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock follows Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("ofdb-sql-identity-{nonce}"));
        let path = directory.join("node.key");

        let first = load_secret_key(&path).expect("create node identity");
        let second = load_secret_key(&path).expect("reload node identity");
        assert_eq!(first.public(), second.public());

        std::fs::remove_dir_all(directory).expect("remove temporary identity directory");
    }
}

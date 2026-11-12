use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io::{Error, ErrorKind, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};

use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets};
use ofdb_sql::Database;
use sync::{IrohTransport, SynchronizeConfig};
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
                    let transport = IrohTransport::new(send, receive);
                    if let Err(error) = database
                        .synchronize(transport, SynchronizeConfig::default())
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
                        let transport = IrohTransport::new(send, receive);
                        if let Err(error) = database
                            .synchronize(transport, SynchronizeConfig::default())
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::load_secret_key;

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

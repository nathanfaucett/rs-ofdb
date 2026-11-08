#[cfg(feature = "p2p")]
mod p2p;

use std::{io, net::SocketAddr, path::PathBuf};

use clap::Parser;
#[cfg(feature = "p2p")]
use iroh::EndpointId;
#[cfg(feature = "p2p")]
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Parser)]
#[command(about = "Run an ofdb SQL database server")]
struct Args {
    #[arg(long, env = "OFDB_DATA", value_name = "FILE")]
    database: PathBuf,

    #[arg(long, env = "OFDB_ADDR", default_value = "0.0.0.0:8081")]
    address: SocketAddr,

    #[arg(long, env = "OFDB_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    #[arg(long, env = "OFDB_TLS_KEY")]
    tls_key: Option<PathBuf>,

    #[arg(long, env = "OFDB_TLS_CLIENT_CA")]
    tls_client_ca: Option<PathBuf>,

    #[cfg(feature = "p2p")]
    #[arg(long = "sync-peer", env = "OFDB_SYNC_PEER", value_name = "NODE_ID")]
    sync_peers: Vec<EndpointId>,

    #[cfg(feature = "p2p")]
    #[arg(long, env = "OFDB_SYNC_PEERS_FILE", value_name = "FILE")]
    sync_peers_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run(Args::parse()).await
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(feature = "p2p")]
    let secret_path = args.database.with_extension("iroh-key");
    let database = Arc::new(ofdb_sql::Database::open(args.database)?);
    let identity = load_identity(args.tls_cert, args.tls_key)?;
    let client_ca = load_client_ca(args.tls_client_ca, identity.is_some())?;

    #[cfg(feature = "p2p")]
    let sync_peers = merge_sync_peers(args.sync_peers, args.sync_peers_file)?;
    let (shutdown_sender, p2p_task) = start_p2p(Arc::clone(&database), secret_path, sync_peers);

    let result = database
        .serve_tcp_with_mtls(args.address, identity, client_ca, async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for Ctrl-C");
        })
        .await;
    #[cfg(feature = "p2p")]
    finish_p2p(shutdown_sender, p2p_task).await?;

    result
}

fn load_identity(
    certificate: Option<PathBuf>,
    private_key: Option<PathBuf>,
) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    match (certificate, private_key) {
        (Some(certificate), Some(private_key)) => Ok(Some((
            std::fs::read(certificate)?,
            std::fs::read(private_key)?,
        ))),
        (None, None) => Ok(None),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "OFDB_TLS_CERT and OFDB_TLS_KEY must be set together",
        )),
    }
}

fn load_client_ca(
    certificate: Option<PathBuf>,
    identity_configured: bool,
) -> io::Result<Option<Vec<u8>>> {
    match (certificate, identity_configured) {
        (Some(_), false) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "client CA requires OFDB_TLS_CERT and OFDB_TLS_KEY",
        )),
        (Some(certificate), true) => Ok(Some(std::fs::read(certificate)?)),
        (None, _) => Ok(None),
    }
}

#[cfg(feature = "p2p")]
fn start_p2p(
    database: Arc<ofdb_sql::Database>,
    secret_path: PathBuf,
    sync_peers: Vec<EndpointId>,
) -> (
    tokio::sync::watch::Sender<bool>,
    Option<tokio::task::JoinHandle<Result<(), io::Error>>>,
) {
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    let task = if sync_peers.is_empty() {
        None
    } else {
        Some(tokio::spawn(p2p::run(
            database,
            secret_path,
            sync_peers,
            shutdown_receiver,
        )))
    };
    (shutdown_sender, task)
}

#[cfg(feature = "p2p")]
async fn finish_p2p(
    shutdown_sender: tokio::sync::watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<(), io::Error>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = shutdown_sender.send(true);
    if let Some(task) = task {
        task.await??;
    }
    Ok(())
}

#[cfg(feature = "p2p")]
fn merge_sync_peers(
    cli_peers: Vec<EndpointId>,
    peers_file: Option<PathBuf>,
) -> Result<Vec<EndpointId>, Box<dyn std::error::Error + Send + Sync>> {
    let mut peers = cli_peers;
    if let Some(path) = peers_file {
        let content = std::fs::read_to_string(&path)?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            peers.push(EndpointId::from_str(line)?);
        }
    }
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, path::PathBuf};

    use super::{load_client_ca, load_identity, merge_sync_peers};

    #[test]
    fn tls_identity_requires_certificate_and_key_as_a_pair() {
        let error = load_identity(Some(PathBuf::from("cert.pem")), None)
            .expect_err("certificate without key must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(load_identity(None, Some(PathBuf::from("key.pem"))).is_err());
        assert!(
            load_identity(None, None)
                .expect("no TLS files means plaintext transport")
                .is_none()
        );
    }

    #[test]
    fn client_ca_requires_server_identity() {
        let error = load_client_ca(Some(PathBuf::from("client-ca.pem")), false)
            .expect_err("client CA without server TLS must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            load_client_ca(None, false)
                .expect("client CA is optional")
                .is_none()
        );
    }

    #[cfg(feature = "p2p")]
    #[test]
    fn merge_sync_peers_reads_file() {
        use iroh::SecretKey;
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("peers.txt");
        let key1 = SecretKey::generate().public();
        let key2 = SecretKey::generate().public();
        let mut f = std::fs::File::create(&file).unwrap();
        writeln!(f, "{}", key1).unwrap();
        writeln!(f, "# comment").unwrap();
        writeln!(f, "{}", key2).unwrap();
        writeln!(f, "").unwrap();
        let peers = merge_sync_peers(vec![], Some(file)).unwrap();
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&key1));
        assert!(peers.contains(&key2));
    }

    #[cfg(feature = "p2p")]
    #[test]
    fn merge_sync_peers_combines_cli_and_file() {
        use iroh::SecretKey;
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("peers.txt");
        let key1 = SecretKey::generate().public();
        let key2 = SecretKey::generate().public();
        let key3 = SecretKey::generate().public();
        let mut f = std::fs::File::create(&file).unwrap();
        writeln!(f, "{}", key2).unwrap();
        let peers = merge_sync_peers(vec![key1, key3], Some(file)).unwrap();
        assert_eq!(peers.len(), 3);
        assert!(peers.contains(&key1));
        assert!(peers.contains(&key2));
        assert!(peers.contains(&key3));
    }
}

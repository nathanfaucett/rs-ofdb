#[cfg(feature = "p2p")]
mod p2p;

use std::{io, net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
#[cfg(feature = "p2p")]
use iroh::EndpointId;

#[derive(Debug, Parser)]
#[command(about = "Run an ofdb KV database server")]
struct Args {
    #[arg(long, env = "OFDB_DATA", value_name = "FILE")]
    database: PathBuf,

    #[arg(long, env = "OFDB_ADDR", default_value = "0.0.0.0:8080")]
    address: SocketAddr,

    #[arg(long, env = "OFDB_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    #[arg(long, env = "OFDB_TLS_KEY")]
    tls_key: Option<PathBuf>,

    #[arg(long, env = "OFDB_TLS_CLIENT_CA", requires = "tls_cert")]
    tls_client_ca: Option<PathBuf>,

    #[cfg(feature = "p2p")]
    #[arg(long, env = "OFDB_SYNC_LISTEN")]
    sync_listen: bool,

    #[cfg(feature = "p2p")]
    #[arg(long = "sync-peer", env = "OFDB_SYNC_PEER", value_name = "NODE_ID")]
    sync_peers: Vec<EndpointId>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run(Args::parse()).await
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(feature = "p2p")]
    let secret_path = args.database.with_extension("iroh-key");
    let database = Arc::new(ofdb_kv::Database::open(args.database)?);
    let identity = load_identity(args.tls_cert, args.tls_key)?;
    let client_ca = args.tls_client_ca.map(std::fs::read).transpose()?;

    #[cfg(feature = "p2p")]
    let (shutdown_sender, p2p_task) = start_p2p(
        Arc::clone(&database),
        secret_path,
        args.sync_listen,
        args.sync_peers,
    );
    let result = database
        .serve_tcp_with_client_ca(args.address, identity, client_ca, async {
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

#[cfg(feature = "p2p")]
fn start_p2p(
    database: Arc<ofdb_kv::Database>,
    secret_path: PathBuf,
    sync_listen: bool,
    sync_peers: Vec<EndpointId>,
) -> (
    tokio::sync::watch::Sender<bool>,
    Option<tokio::task::JoinHandle<Result<(), io::Error>>>,
) {
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    let task = if !sync_listen && sync_peers.is_empty() {
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

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, path::PathBuf};

    use super::load_identity;

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
}

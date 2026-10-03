use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Command, Output},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ofdb_kv::{Client, Database};
use tokio::{net::TcpStream, sync::oneshot, time::sleep};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kv-cli"))
        .args(args)
        .output()
        .expect("run kv-cli")
}

fn temporary_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock follows Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ofdb-kv-cli-{label}-{}-{nonce}",
        std::process::id()
    ))
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind temporary listener");
    listener.local_addr().expect("read temporary address")
}

#[test]
fn help_excludes_hosting_and_sync_commands() {
    let help = run(&["--help"]);
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("help is UTF-8");
    assert!(!help.contains("serve"));
    assert!(!help.contains("sync"));
}

#[test]
fn local_cli_preserves_file_bytes_and_emits_json_scans() {
    let database = temporary_path("store.redb");
    let input = temporary_path("value.bin");
    let value = [0, 255, 10];
    std::fs::write(&input, value).expect("write binary test value");
    let database_arg = database.to_str().expect("database path is UTF-8");
    let input_arg = input.to_str().expect("input path is UTF-8");

    let set = run(&[
        "--database",
        database_arg,
        "set",
        "binary",
        "--file",
        input_arg,
    ]);
    assert!(
        set.status.success(),
        "{}",
        String::from_utf8_lossy(&set.stderr)
    );
    assert!(set.stdout.is_empty());

    let get = run(&["--database", database_arg, "get", "binary"]);
    assert!(
        get.status.success(),
        "{}",
        String::from_utf8_lossy(&get.stderr)
    );
    assert_eq!(get.stdout, value);

    let scan = run(&["--database", database_arg, "scan-all"]);
    assert!(
        scan.status.success(),
        "{}",
        String::from_utf8_lossy(&scan.stderr)
    );
    let entries: serde_json::Value = serde_json::from_slice(&scan.stdout).expect("valid scan JSON");
    assert_eq!(entries[0]["key"], "binary");
    assert_eq!(entries[0]["value"], "AP8K");

    let delete = run(&["--database", database_arg, "delete", "binary"]);
    assert!(delete.status.success());
    assert!(delete.stdout.is_empty());
    let missing = run(&["--database", database_arg, "get", "binary"]);
    assert_eq!(missing.status.code(), Some(1));
    assert!(missing.stdout.is_empty());

    std::fs::remove_file(input).expect("remove test value file");
    std::fs::remove_file(database).expect("remove test database");
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_cli_executes_queries_through_the_facade() {
    let address = free_address();
    let database = Database::in_memory();

    let (shutdown, shutdown_signal) = oneshot::channel();
    let server = tokio::spawn(async move {
        database
            .serve_tcp(address, async {
                let _ = shutdown_signal.await;
            })
            .await
            .expect("server stops cleanly");
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        while TcpStream::connect(address).await.is_err() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server starts");

    let endpoint = format!("http://{address}");
    let cli_endpoint = endpoint.clone();
    let output = tokio::task::spawn_blocking(move || {
        run(&[
            "--endpoint",
            &cli_endpoint,
            "set",
            "remote-key",
            "--value",
            "remote",
        ])
    })
    .await
    .expect("CLI process completes");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    let client = Client::connect(&endpoint)
        .await
        .expect("connect to hosted database");
    assert_eq!(
        client.get("remote-key").await.expect("read hosted value"),
        Some(b"remote".to_vec())
    );

    shutdown.send(()).expect("server awaits shutdown");
    server.await.expect("server task completes");
}

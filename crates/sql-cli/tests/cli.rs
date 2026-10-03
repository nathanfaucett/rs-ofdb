use std::{
    net::{SocketAddr, TcpListener},
    process::{Command, Output},
    time::Duration,
};

use ofdb_sql::Database;
use tokio::{net::TcpStream, sync::oneshot, time::sleep};

const SQL: &str = "CREATE TABLE items (id UUID PRIMARY KEY); INSERT INTO items VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)); SELECT id FROM items";

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind temporary listener");
    listener.local_addr().expect("read temporary address")
}

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_sql-cli"))
        .args(args)
        .output()
        .expect("run sql-cli")
}

#[test]
fn local_cli_executes_one_batch_and_reports_errors() {
    let output = run(&["--memory", "--query", SQL]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout.last(), Some(&b']'));
    let results: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid result JSON");
    assert_eq!(results.as_array().expect("result array").len(), 3);
    assert_eq!(results[2]["rows"].as_array().expect("rows").len(), 1);

    let failed = run(&["--memory", "--query", "SELECT id FROM missing"]);
    assert_eq!(failed.status.code(), Some(1));
    assert!(failed.stdout.is_empty());
    assert!(!failed.stderr.is_empty());

    let help = Command::new(env!("CARGO_BIN_EXE_sql-cli"))
        .arg("--help")
        .output()
        .expect("run sql-cli help");
    let help = String::from_utf8(help.stdout).expect("help is UTF-8");
    assert!(!help.contains("serve"));
    assert!(!help.contains("sync"));
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_cli_executes_queries_through_the_facade() {
    let address = free_address();
    let database = Database::in_memory();
    database
        .client()
        .execute_sql("CREATE TABLE items (id UUID PRIMARY KEY)", None)
        .await
        .expect("create hosted table");

    let hosted_database = database.clone();
    let (shutdown, shutdown_signal) = oneshot::channel();
    let server = tokio::spawn(async move {
        hosted_database
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
    let output = tokio::task::spawn_blocking(move || {
        run(&["--endpoint", &endpoint, "--query", "SELECT id FROM items"])
    })
    .await
    .expect("CLI process completes");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let results: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid result JSON");
    assert_eq!(results[0]["rows"], serde_json::json!([]));

    shutdown.send(()).expect("server awaits shutdown");
    server.await.expect("server task completes");
}

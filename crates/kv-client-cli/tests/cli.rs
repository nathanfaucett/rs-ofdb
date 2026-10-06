use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Command, Output},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ofdb_kv::{Client, Database};
use tokio::{net::TcpStream, sync::oneshot, time::sleep};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kv-client-cli"))
        .args(args)
        .output()
        .expect("run kv-client-cli")
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
fn local_cli_preserves_tagged_file_values_and_emits_json_scans() {
    let database = temporary_path("store.redb");
    let input = temporary_path("value.bin");
    let value = br#"{"type":"blob","value":"AP8K"}"#;
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
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&get.stdout).expect("tagged output"),
        serde_json::from_slice::<serde_json::Value>(value).expect("tagged input")
    );

    let scan = run(&["--database", database_arg, "scan-all"]);
    assert!(
        scan.status.success(),
        "{}",
        String::from_utf8_lossy(&scan.stderr)
    );
    let entries: serde_json::Value = serde_json::from_slice(&scan.stdout).expect("valid scan JSON");
    assert_eq!(entries[0]["key"], "binary");
    assert_eq!(
        entries[0]["value"],
        serde_json::json!({"type":"blob","value":"AP8K"})
    );

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
async fn remote_cli_verifies_tls_with_the_configured_ca() {
    let address = free_address();
    let database = Database::in_memory();
    let (shutdown, shutdown_signal) = oneshot::channel();
    let server = tokio::spawn(async move {
        database
            .serve_tcp_with_tls(
                address,
                Some((
                    include_bytes!("../../../test-fixtures/tls/server.crt").to_vec(),
                    include_bytes!("../../../test-fixtures/tls/server.key").to_vec(),
                )),
                async {
                    let _ = shutdown_signal.await;
                },
            )
            .await
            .expect("TLS server stops cleanly");
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        while TcpStream::connect(address).await.is_err() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("TLS server starts");

    let endpoint = format!("https://{address}");
    let ca_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-fixtures/tls/ca.crt")
        .into_os_string()
        .into_string()
        .expect("CA path is UTF-8");
    let output = tokio::task::spawn_blocking(move || {
        run(&[
            "--endpoint",
            &endpoint,
            "--tls-ca",
            &ca_path,
            "set",
            "tls-key",
            "--value",
            r#"{"type":"text","value":"verified"}"#,
        ])
    })
    .await
    .expect("TLS CLI process completes");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    shutdown.send(()).expect("server awaits shutdown");
    server.await.expect("server task completes");
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_cli_requires_and_accepts_a_client_certificate() {
    let address = free_address();
    let database = Database::in_memory();
    let (shutdown, shutdown_signal) = oneshot::channel();
    let server = tokio::spawn(async move {
        database
            .serve_tcp_with_client_ca(
                address,
                Some((
                    include_bytes!("../../../test-fixtures/kv-mtls/server.crt").to_vec(),
                    include_bytes!("../../../test-fixtures/kv-mtls/server.key").to_vec(),
                )),
                Some(include_bytes!("../../../test-fixtures/kv-mtls/ca.crt").to_vec()),
                async {
                    let _ = shutdown_signal.await;
                },
            )
            .await
            .expect("mTLS server stops cleanly");
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        while TcpStream::connect(address).await.is_err() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("mTLS server starts");

    let endpoint = format!("https://{address}");
    let fixture =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-fixtures/kv-mtls");
    let ca = fixture
        .join("ca.crt")
        .into_os_string()
        .into_string()
        .expect("CA path UTF-8");
    let certificate = fixture
        .join("client.crt")
        .into_os_string()
        .into_string()
        .expect("certificate path UTF-8");
    let key = fixture
        .join("client.key")
        .into_os_string()
        .into_string()
        .expect("key path UTF-8");
    let endpoint_without_identity = endpoint.clone();
    let ca_without_identity = ca.clone();
    let rejected = tokio::task::spawn_blocking(move || {
        run(&[
            "--endpoint",
            &endpoint_without_identity,
            "--tls-ca",
            &ca_without_identity,
            "get",
            "key",
        ])
    })
    .await
    .expect("unauthorized CLI process completes");
    assert!(
        !rejected.status.success(),
        "client without identity must fail"
    );

    let accepted = tokio::task::spawn_blocking(move || {
        run(&[
            "--endpoint",
            &endpoint,
            "--tls-ca",
            &ca,
            "--tls-cert",
            &certificate,
            "--tls-key",
            &key,
            "set",
            "mtls-key",
            "--value",
            r#"{"type":"text","value":"authorized"}"#,
        ])
    })
    .await
    .expect("authorized CLI process completes");
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );

    shutdown.send(()).expect("server awaits shutdown");
    server.await.expect("server task completes");
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
            r#"{"type":"text","value":"remote"}"#,
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
        Some(ofdb_kv::Value::Text("remote".into()))
    );

    shutdown.send(()).expect("server awaits shutdown");
    server.await.expect("server task completes");
}

#[test]
fn cli_round_trips_explicit_types_and_rejects_untagged_input_before_open() {
    let database = temporary_path("typed.redb");
    let path = database.to_str().expect("UTF-8 test path");
    for invalid in ["text", "42", r#"{"untagged":true}"#] {
        let output = run(&["--database", path, "set", "key", "--value", invalid]);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!database.exists(), "invalid input must not create storage");
    }
    for input in [
        r#"{"type":"null"}"#,
        r#"{"type":"type","value":"Json"}"#,
        r#"{"type":"uuid","value":"00000000-0000-0000-0000-000000000000"}"#,
        r#"{"type":"bool","value":true}"#,
        r#"{"type":"integer","value":"-9223372036854775808"}"#,
        r#"{"type":"float","value":"7ff8000000000042"}"#,
        r#"{"type":"float","value":"8000000000000000"}"#,
        r#"{"type":"text","value":"{not guessed as JSON}"}"#,
        r#"{"type":"blob","value":"AP8="}"#,
        r#"{"type":"json","value":{"type":"object","value":{"$number":{"type":"array","value":[{"type":"u64","value":"18446744073709551615"},{"type":"f64","value":"7ff8000000000042"}]}}}}"#,
    ] {
        let set = run(&["--database", path, "set", "key", "--value", input]);
        assert!(
            set.status.success(),
            "{}",
            String::from_utf8_lossy(&set.stderr)
        );
        assert!(set.stdout.is_empty());
        let get = run(&["--database", path, "get", "key"]);
        assert!(
            get.status.success(),
            "{}",
            String::from_utf8_lossy(&get.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&get.stdout).expect("typed output"),
            serde_json::from_str::<serde_json::Value>(input).expect("typed input")
        );
    }
    std::fs::remove_file(database).expect("remove typed test database");
}

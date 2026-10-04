use std::{
    io::{self, Read, Write},
    path::PathBuf,
    process::ExitCode,
    time::Duration,
};

use clap::{ArgGroup, Parser, Subcommand};
#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb"))]
use ofdb_kv::Client;
#[cfg(any(feature = "in-memory", feature = "redb"))]
use ofdb_kv::Database;
use ofdb_kv::parse_value;
#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb", test))]
use ofdb_kv::{Value, encode_value};

#[derive(Debug, Parser)]
#[command(name = "kv-cli", version, about = "Query an ofdb KV store")]
#[command(group(ArgGroup::new("target").required(true).multiple(false).args(["database", "memory", "endpoint", "unix_socket"]))) ]
struct Args {
    #[arg(long, group = "target")]
    database: Option<PathBuf>,
    #[arg(long, group = "target")]
    memory: bool,
    #[arg(long, group = "target")]
    endpoint: Option<String>,
    #[arg(long, group = "target")]
    unix_socket: Option<PathBuf>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    timeout: Option<u64>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        #[arg(long, group = "set_value", required_unless_present = "file")]
        value: Option<String>,
        #[arg(long, group = "set_value", required_unless_present = "value")]
        file: Option<PathBuf>,
        #[arg(long)]
        expires_at: Option<i64>,
    },
    Delete {
        key: String,
    },
    Scan {
        start: String,
        end: String,
    },
    ScanPrefix {
        prefix: String,
    },
    ScanAll,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let _bytes = match &args.command {
        Command::Set {
            value: _,
            file: Some(path),
            ..
        } => match read_input(path) {
            Ok(bytes) => Some(bytes),
            Err(error) => return fail(1, error.to_string()),
        },
        _ => None,
    };

    let _input = match &args.command {
        Command::Set { value, .. } => {
            let bytes = _bytes
                .as_deref()
                .or_else(|| value.as_deref().map(str::as_bytes))
                .expect("clap requires set input");
            let parsed = serde_json::from_slice(bytes)
                .map_err(|error| error.to_string())
                .and_then(|json| parse_value(&json));
            match parsed {
                Ok(value) => Some(value),
                Err(error) => return fail(2, error),
            }
        }
        _ => None,
    };

    let operation = async {
        if let Some(_endpoint) = &args.endpoint {
            #[cfg(feature = "remote")]
            {
                let client = Client::connect(_endpoint)
                    .await
                    .map_err(|error| error.to_string())?;
                return execute_client(&client, &args.command, _input).await;
            }
            #[cfg(not(feature = "remote"))]
            return Err("kv-cli was built without the `remote` feature".to_owned());
        }
        if let Some(_path) = &args.unix_socket {
            #[cfg(all(feature = "remote", unix))]
            {
                let client = Client::connect_unix(_path)
                    .await
                    .map_err(|error| error.to_string())?;
                return execute_client(&client, &args.command, _input).await;
            }
            #[cfg(not(all(feature = "remote", unix)))]
            return Err("Unix sockets require a Unix build with the `remote` feature".to_owned());
        }
        if args.memory {
            #[cfg(feature = "in-memory")]
            {
                let database = Database::in_memory();
                let client = database.client();
                return execute_client(&client, &args.command, _input).await;
            }
            #[cfg(not(feature = "in-memory"))]
            return Err("kv-cli was built without the `in-memory` feature".to_owned());
        }
        if let Some(_path) = &args.database {
            #[cfg(feature = "redb")]
            {
                let database = Database::open(_path).map_err(|error| error.to_string())?;
                let client = database.client();
                return execute_client(&client, &args.command, _input).await;
            }
            #[cfg(not(feature = "redb"))]
            return Err("kv-cli was built without the `redb` feature".to_owned());
        }
        Err("select exactly one target".to_owned())
    };

    let result: Result<Option<Vec<u8>>, String> = if let Some(seconds) = args.timeout {
        match tokio::time::timeout(Duration::from_secs(seconds), operation).await {
            Ok(result) => result,
            Err(_) => Err("operation timed out; the write may still have committed".to_owned()),
        }
    } else {
        operation.await
    };
    match result {
        Ok(Some(bytes)) => {
            if let Err(error) = io::stdout().lock().write_all(&bytes) {
                fail(1, error.to_string())
            } else {
                ExitCode::SUCCESS
            }
        }
        Ok(None) => ExitCode::SUCCESS,
        Err(error) => fail(1, error),
    }
}

fn read_input(path: &PathBuf) -> io::Result<Vec<u8>> {
    if path == std::path::Path::new("-") {
        let mut bytes = Vec::new();
        io::stdin().read_to_end(&mut bytes)?;
        Ok(bytes)
    } else {
        std::fs::read(path)
    }
}

#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb"))]
async fn execute_client(
    client: &Client,
    command: &Command,
    input: Option<Value>,
) -> Result<Option<Vec<u8>>, String> {
    match command {
        Command::Get { key } => client
            .get(key)
            .await
            .map_err(|error| error.to_string())?
            .map(|value| {
                serde_json::to_vec(&encode_value(value))
                    .map(Some)
                    .map_err(|error| error.to_string())
            })
            .ok_or_else(|| format!("key not found: {key}"))?,
        Command::Set {
            key, expires_at, ..
        } => {
            let value = input.expect("set input was parsed before connection");
            client
                .set(key, value, *expires_at)
                .await
                .map_err(|error| error.to_string())?;
            Ok(None)
        }
        Command::Delete { key } => {
            client
                .delete(key)
                .await
                .map_err(|error| error.to_string())?;
            Ok(None)
        }
        Command::Scan { start, end } => json_scan(
            client
                .scan(start, end)
                .await
                .map_err(|error| error.to_string())?,
        ),
        Command::ScanPrefix { prefix } => json_scan(
            client
                .scan_prefix(prefix)
                .await
                .map_err(|error| error.to_string())?,
        ),
        Command::ScanAll => json_scan(client.scan_all().await.map_err(|error| error.to_string())?),
    }
}

#[cfg(any(feature = "remote", feature = "in-memory", feature = "redb", test))]
fn json_scan(entries: Vec<(String, Value)>) -> Result<Option<Vec<u8>>, String> {
    let entries = entries
        .into_iter()
        .map(|(key, value)| serde_json::json!({ "key": key, "value": encode_value(value) }))
        .collect::<Vec<_>>();
    serde_json::to_vec(&entries)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn fail(code: u8, message: String) -> ExitCode {
    let _ = writeln!(io::stderr().lock(), "{message}");
    ExitCode::from(code)
}

#[cfg(test)]
mod tests {
    use super::{Args, json_scan};
    use clap::Parser;
    use ofdb_kv::Value;

    #[test]
    fn cli_requires_one_target_and_set_input() {
        assert!(
            Args::try_parse_from(["kv-cli", "--memory", "set", "key", "--value", "text"]).is_ok()
        );
        assert!(Args::try_parse_from(["kv-cli", "set", "key", "--value", "text"]).is_err());
        assert!(
            Args::try_parse_from([
                "kv-cli", "--memory", "set", "key", "--value", "text", "--file", "-"
            ])
            .is_err()
        );
    }

    #[test]
    fn scans_emit_ordered_tagged_json() {
        let bytes = json_scan(vec![
            ("a".to_owned(), Value::Blob(vec![0, 255])),
            ("b".to_owned(), Value::Blob(Vec::new())),
        ])
        .expect("encode scan")
        .expect("scan always has output");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(json[0]["key"], "a");
        assert_eq!(
            json[0]["value"],
            serde_json::json!({"type":"blob","value":"AP8="})
        );
        assert_eq!(
            json[1]["value"],
            serde_json::json!({"type":"blob","value":""})
        );
    }
}

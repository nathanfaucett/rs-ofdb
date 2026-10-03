# Offline First Database

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT)
![Test Status](https://github.com/nathanfaucett/rs-ofdb/actions/workflows/ci.yml/badge.svg)

## gRPC

Enable the `remote` feature to open a lazy client with `Database::open_uri`:

```rust,ignore
let db = ofdb::Database::open_uri("ofdb+grpc://127.0.0.1:50051")?;
db.execute(statements).await?;
```

TCP endpoints use `ofdb+grpc://host:port`; Unix sockets use
`ofdb+unix:///absolute/socket/path` on Unix. TLS (`ofdb+grpcs`) is not
supported yet. gRPC dependencies are declared in the `proto`, `client`, and
`server` crate manifests.

The `server` crate exposes `QueryService` for custom tonic routing and
`Server::tcp` / `Server::unix` for standalone servers. Wrap an engine with
`EngineExecutor::new(engine)` before passing it to a server. The socket file is
owned by the caller and is not removed automatically.

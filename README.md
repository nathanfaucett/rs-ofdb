# Offline First Database Workspace

This workspace provides separate SQL and KV database libraries:

- [`ofdb-sql`](crates/sql/README.md): SQL database and client.
- [`ofdb-kv`](crates/kv/README.md): key-value database and client.

The libraries have separate query contracts and sync protocols. Use their `Database` handles to construct embedded stores, host them, and configure explicit sync sessions. Create an embedded query client with `Database::client()`, or use `Client::connect` for remote queries.

The `sql-cli` and `kv-cli` binaries provide one-shot commands for their matching libraries. They do not start servers or sync sessions.

See [`CONTEXT.md`](CONTEXT.md) for terminology and [`docs/embedded-client-plan.md`](docs/embedded-client-plan.md) for implementation status and focused checks.

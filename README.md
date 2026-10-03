# Offline First Database Workspace

This workspace provides separate SQL and KV database libraries:

- [`ofdb-sql`](crates/sql/README.md): SQL database and client.
- [`ofdb-kv`](crates/kv/README.md): key-value database and client.

The libraries have separate query contracts and sync protocols. Use their embedded `Database` handles to host local data and configure explicit sync sessions. Use their remote `Client` handles for queries.

The `sql-cli` and `kv-cli` binaries provide one-shot commands for their matching libraries. They do not start servers or sync sessions.

See [`CONTEXT.md`](CONTEXT.md) for terminology and [`docs/facade-cli-plan.md`](docs/facade-cli-plan.md) for the implementation status.

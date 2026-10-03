# kv-cli

`kv-cli` is a one-shot KV client. Build with explicit features for the targets you need, for example:

```sh
cargo install ofdb-kv-cli --features remote
kv-cli --endpoint http://127.0.0.1:50051 get name
kv-cli --database ./cache.redb set blob --file payload.bin
```

Select exactly one of `--database PATH`, `--memory`, `--endpoint URL`, or `--unix-socket PATH`. The binary has no implicit target. `--memory` needs the `in-memory` feature, `--database` needs `redb`, and remote targets need `remote`.

Commands are `get KEY`, `set KEY`, `delete KEY`, `scan START END`, `scan-prefix PREFIX`, and `scan-all`. Set uses exactly one of `--value TEXT` or `--file PATH`; `--file -` reads opaque bytes from stdin. An optional `--expires-at UNIX_MS` sets an absolute expiry deadline.

`get` writes the exact value bytes to stdout without a newline. A missing key exits 1. Successful set and delete produce no stdout. Scans write a JSON array in key order; each entry has a string `key` and base64 `value`. Empty values and empty scans are successful. Errors go to stderr. Exit status is 0 for success, 1 for query failure, and 2 for invalid command usage.

`--timeout SECONDS` is optional and must be a positive integer. Its one budget covers connection and query after a set input file is read. A timeout does not prove that a write was rolled back.

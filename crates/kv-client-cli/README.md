# kv-client-cli

`kv-client-cli` is a one-shot KV client. Build with explicit features for the targets you need, for example:

```sh
cargo install ofdb-kv-client-cli --features remote
kv-client-cli --endpoint http://127.0.0.1:50051 get name
kv-client-cli --database ./cache.redb set blob --file payload.bin
```

Select exactly one of `--database PATH`, `--memory`, `--endpoint URL`, or `--unix-socket PATH`. The binary has no implicit target. `--memory` needs the `in-memory` feature, `--database` needs `redb`, and remote targets need `remote`.

Commands are `get KEY`, `set KEY`, `delete KEY`, `scan START END`, `scan-prefix PREFIX`, and `scan-all`. Set uses exactly one of `--value JSON` or `--file PATH`; the input must be the explicit tagged JSON encoding returned by `get` and scans. `--file -` reads that encoding from stdin. An optional `--expires-at UNIX_MS` sets an absolute expiry deadline.

`get` writes a lossless tagged JSON value to stdout without a newline. Scans write a JSON array in key order; each entry has a string `key` and tagged `value`. Integers use decimal strings, floats use exact hexadecimal bits, and blobs use base64. A missing key exits 1. Successful set and delete produce no stdout. Empty values and empty scans are successful. Errors go to stderr. Exit status is 0 for success, 1 for query failure, and 2 for invalid command usage.

For TLS, use `--tls-ca PEM` with an HTTPS endpoint. For mutual TLS, also set `--tls-cert PEM` and `--tls-key PEM`; both must be provided together. The KV server enables client-certificate verification with `--tls-client-ca PEM` (or `OFDB_TLS_CLIENT_CA`).

`--timeout SECONDS` is optional and must be a positive integer. Its one budget covers connection and query after a set input file is read. A timeout does not prove that a write was rolled back.

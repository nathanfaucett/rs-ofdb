# sql-cli

`sql-cli` is a one-shot SQL client. Build with explicit features for the targets you need, for example:

```sh
cargo install ofdb-sql-cli --features remote
sql-cli --endpoint http://127.0.0.1:50051 --query 'SELECT * FROM users'
sql-cli --database ./users.redb --file query.sql
```

Select exactly one of `--database PATH`, `--memory`, `--endpoint URL`, or `--unix-socket PATH`. The binary has no implicit target. `--memory` needs the `in-memory` feature, `--database` needs `redb`, and remote targets need `remote`.

Pass exactly one of `--query SQL` and `--file PATH`. `--file -` reads SQL from stdin. The complete input is translated and executed as one statement batch. `--params-file PATH` accepts either:

```json
{"positional":[{"type":"integer","value":"9007199254740993"}]}
```

or:

```json
{"named":{"name":{"type":"text","value":"Ada"}}}
```

Each SQL parameter is a tagged object. Supported tags are `null`, `type`, `uuid`, `bool`, `integer`, `float`, `text`, `blob`, and `json`. Integers use decimal strings. Floats use exactly 16 hexadecimal digits containing the IEEE-754 bits. Blobs use base64. For example, `{"type":"float","value":"8000000000000000"}` is negative zero. `type` values use the `ValueType` names (`Null`, `Type`, `Uuid`, `Bool`, `Integer`, `Float`, `Text`, `Json`, `Blob`).

A `json` value uses ordinary JSON for nulls, strings, booleans, arrays, and objects. Every number uses a lossless wrapper: `{"$number":{"type":"i64","value":"-2"}}`, `{"$number":{"type":"u64","value":"18446744073709551615"}}`, or `{"$number":{"type":"f64","value":"3ff0000000000000"}}`. Nested values use the same wrapper recursively. Untagged fractional JSON numbers are rejected.

Results are a JSON array of result sets. Each result set has `columns` (objects with `name`, `source_table`, and `source_column`) and `rows` (arrays of tagged values in column order). The value tags use the same encoding as parameters. Output has no extra formatting or trailing newline.

`--timeout SECONDS` is optional and must be a positive integer. It bounds connection plus execution after input files are read. A timeout does not prove that a batch was rolled back. Exit status is 0 for success, 1 for execution/input failure, and 2 for invalid command usage.

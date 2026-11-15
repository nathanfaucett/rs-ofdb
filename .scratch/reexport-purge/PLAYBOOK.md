# Re-export purge playbook (workspace `ofdb`)

GOAL: eliminate every re-export in the crates you own. A re-export is any
`pub use ...`, `pub(crate) use ...`, or `pub(super) use ...` statement
(multi-line, ending at the first `;`). After this change there must be ZERO
such statements in your crates. Never reintroduce them.

MAP: `ofdb/.scratch/reexport-purge/map.md` (other workspaces:
`of/.scratch/reexport-purge/map.md`, `ofnet/...`, `offs/...`). Rows look like:

| `crate::Client` | `crate::client::Client` | internal | file:line |
| `crate::SyncTransport` | `history::SyncTransport` | external | file:line |

- Column 1 is the EXPORTED path, crate-relative (`crate::...`). For cross-crate
  consumers, `crate::X` of the crate whose header says `lib \`L\`` is written `L::X`.
- `internal` canonical = same crate, keep `crate::...` form.
- `external` canonical = use that crate path directly; add it as a dependency of
  the consuming crate.
- NOTE: consumers in the `of` workspace use `of`'s own map, whose canonicals are
  already fully resolved (e.g. `ofdb_sql::client::Client`). Your job is to make
  those paths real: `pub mod client;` etc. and delete the re-exports.

## Steps (apply to EVERY crate dir assigned to you; only edit files inside those dirs)

1. Delete re-export statements. In every `.rs` file (src/, tests/, benches/,
   examples/, build.rs):
   - Remove each `pub use`/`pub(crate) use`/`pub(super) use` statement, from the
     `pub...use` line through the line containing the terminating `;`.
   - If the immediately preceding non-empty line is a `#[cfg(...)]` or
     `#[cfg_attr(...)]` attribute attached to that statement, remove it too.
   - If any name imported by the removed statement is still used elsewhere in the
     SAME file, replace the statement with a plain `use <same path>;` (keep the
     cfg attribute). After compiling, delete any such `use` that is unused.
2. Make modules public instead of re-exporting them:
   - Change every `mod NAME;` to `pub mod NAME;`, EXCEPT declarations carrying
     `#[cfg(test)]` (leave those private).
   - Preserve `#[cfg(...)]`, `#[path = "..."]` and other attributes on the mod.
   - Do not touch `pub mod`, `extern crate`, or `use`.
3. Rewrite every reference to a re-exported path in your files (src + tests +
   benches + examples), including inline paths in type/expression position:
   - Same crate: `use crate::X` / `crate::X::Y` / `use super::X` / `use self::X`
     where the resolved exported path appears in the map: replace the exported
     prefix with the canonical prefix. For `super::`/`self::` in a file of module
     `m`, `super::X` = `crate::parent(m)::X`, `self::X` = `crate::m::X`.
   - Cross-crate: e.g. `use engine::RedbKernel`: find the crate section with
     `lib \`engine\`` in the map, read the row, rewrite to its canonical path.
   - `as` aliases: the exported name is the alias; rewrite alias paths to the
     canonical (original) path.
   - Glob re-exports (`pub use foo::*;`): deleted like any other; consumers of
     `foo`'s items use `foo::Item` (module `foo` is now `pub mod foo`).
4. Cargo.toml: for every `external` canonical path now used, ensure a dependency
   exists in the right section. Workspace crates: path deps matching repo style,
   `default-features = false` + needed features. If package name differs from the
   code name in the canonical path, use `key = { package = "pkg", ... }`.
   Registry deps: `major.minor` versions only, `default-features = false`,
   minimal features, grouped/alphabetized with comment headers.
5. Import order: std/core/alloc, then external crates, then crate/super/self.
   No glob imports. Run `cargo fmt` on your files when done.

## Validation

- `cargo check -p <pkg> --all-targets` per owned crate. `crates/wasm` is excluded
  from the workspace: `cargo check --manifest-path crates/wasm/Cargo.toml --all-targets`.
- Fix every error caused by your edits, but ONLY inside your assigned dirs.
  Errors in other agents' crates are expected mid-flight — report, do not fix.
- `grep -rn "pub use \|pub(crate) use \|pub(super) use " <your dirs>` must be empty
  (excluding target/).

## Hard rules

- Edit only your assigned crate directories. Never touch `target/`, `.scratch/`,
  `node_modules/`, or other agents' crates.
- Never add `pub use` / `pub(crate) use` / `pub(super) use`; never use
  `pub(crate)` / `pub(super)` visibility.
- Do not rename items, files, or modules. Do not move code between files.

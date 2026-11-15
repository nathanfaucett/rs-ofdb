# Re-export map for workspace `ofdb`

Format: `exported_path` -> `canonical_path` (kind).
`internal` = within same crate, shown as crate::... ; consumers inside the crate use `crate::...`,
consumers outside use `<libname>::...`. `external` = use the external crate directly (add dep to Cargo.toml).

## crate `ofdb-btree` (lib `btree`, dir `ofdb/crates/btree`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::ChangeEvent` | `crate::changes::ChangeEvent` | internal | ofdb/crates/btree/src/lib.rs:12 |
| `crate::ChangeSignal` | `crate::changes::ChangeSignal` | internal | ofdb/crates/btree/src/lib.rs:12 |
| `crate::ChangeSubscription` | `crate::changes::ChangeSubscription` | internal | ofdb/crates/btree/src/lib.rs:12 |
| `crate::ChangeWork` | `crate::changes::ChangeWork` | internal | ofdb/crates/btree/src/lib.rs:12 |
| `crate::StorageInvalidation` | `crate::changes::StorageInvalidation` | internal | ofdb/crates/btree/src/lib.rs:12 |
| `crate::StorageKey` | `crate::changes::StorageKey` | internal | ofdb/crates/btree/src/lib.rs:12 |
| `crate::BTree` | `crate::btree::BTree` | internal | ofdb/crates/btree/src/lib.rs:20 |
| `crate::BTreeError` | `crate::btree::BTreeError` | internal | ofdb/crates/btree/src/lib.rs:20 |
| `crate::BTreeKey` | `crate::btree::BTreeKey` | internal | ofdb/crates/btree/src/lib.rs:20 |
| `crate::BTreeRead` | `crate::btree::BTreeRead` | internal | ofdb/crates/btree/src/lib.rs:20 |
| `crate::BTreeResult` | `crate::btree::BTreeResult` | internal | ofdb/crates/btree/src/lib.rs:20 |
| `crate::BTreeTransaction` | `crate::btree::BTreeTransaction` | internal | ofdb/crates/btree/src/lib.rs:20 |
| `crate::BTreeValue` | `crate::btree::BTreeValue` | internal | ofdb/crates/btree/src/lib.rs:20 |
| `crate::InMemoryBTree` | `crate::in_memory_btree::InMemoryBTree` | internal | ofdb/crates/btree/src/lib.rs:24 |
| `crate::InMemoryBTreeShardFactory` | `crate::in_memory_btree::InMemoryBTreeShardFactory` | internal | ofdb/crates/btree/src/lib.rs:26 |
| `crate::ShardedBTree` | `crate::sharded_btree::ShardedBTree` | internal | ofdb/crates/btree/src/lib.rs:28 |
| `crate::ShardedBTreeTransaction` | `crate::sharded_btree::ShardedBTreeTransaction` | internal | ofdb/crates/btree/src/lib.rs:28 |
| `crate::Sharder` | `crate::sharded_btree::Sharder` | internal | ofdb/crates/btree/src/lib.rs:28 |

## crate `ofdb-btree-automerge` (lib `btree_automerge`, dir `ofdb/crates/btree-automerge`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::AutomergeBTree` | `crate::automerge_tree::AutomergeBTree` | internal | ofdb/crates/btree-automerge/src/lib.rs:10 |
| `crate::AutomergeChangeStore` | `crate::change_store::AutomergeChangeStore` | internal | ofdb/crates/btree-automerge/src/lib.rs:11 |
| `crate::CompactionPolicy` | `crate::compaction::CompactionPolicy` | internal | ofdb/crates/btree-automerge/src/lib.rs:12 |
| `crate::ThresholdPolicy` | `crate::compaction::ThresholdPolicy` | internal | ofdb/crates/btree-automerge/src/lib.rs:12 |
| `crate::hash_hashes` | `crate::compaction::hash_hashes` | internal | ofdb/crates/btree-automerge/src/lib.rs:12 |
| `crate::hash_heads` | `crate::compaction::hash_heads` | internal | ofdb/crates/btree-automerge/src/lib.rs:12 |
| `crate::run_compaction` | `crate::compaction::run_compaction` | internal | ofdb/crates/btree-automerge/src/lib.rs:12 |
| `crate::DocumentChangeHash` | `crate::document_change_key::DocumentChangeHash` | internal | ofdb/crates/btree-automerge/src/lib.rs:13 |
| `crate::DocumentChangeKey` | `crate::document_change_key::DocumentChangeKey` | internal | ofdb/crates/btree-automerge/src/lib.rs:13 |
| `crate::DocumentId` | `crate::document_change_key::DocumentId` | internal | ofdb/crates/btree-automerge/src/lib.rs:13 |
| `crate::DocumentType` | `crate::document_type::DocumentType` | internal | ofdb/crates/btree-automerge/src/lib.rs:14 |
| `crate::reconstruct_document_values` | `crate::reconstruction::reconstruct_document_values` | internal | ofdb/crates/btree-automerge/src/lib.rs:15 |
| `crate::AutomergeBTreeTransaction` | `crate::transaction::AutomergeBTreeTransaction` | internal | ofdb/crates/btree-automerge/src/lib.rs:16 |
| `crate::get_document` | `crate::transaction::get_document` | internal | ofdb/crates/btree-automerge/src/lib.rs:16 |

## crate `ofdb-btree-indexeddb` (lib `ofdb_btree_indexeddb`, dir `ofdb/crates/btree-indexeddb`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::Database` | `crate::database::Database` | internal | ofdb/crates/btree-indexeddb/src/lib.rs:12 |
| `crate::Snapshot` | `crate::database::Snapshot` | internal | ofdb/crates/btree-indexeddb/src/lib.rs:12 |
| `crate::Table` | `crate::database::Table` | internal | ofdb/crates/btree-indexeddb/src/lib.rs:12 |
| `crate::ByteBTree` | `crate::tree::ByteBTree` | internal | ofdb/crates/btree-indexeddb/src/lib.rs:15 |
| `crate::Transaction` | `crate::tree::Transaction` | internal | ofdb/crates/btree-indexeddb/src/lib.rs:15 |

## crate `ofdb-btree-redb` (lib `btree_redb`, dir `ofdb/crates/btree-redb`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::RedbBTree` | `crate::btree::RedbBTree` | internal | ofdb/crates/btree-redb/src/lib.rs:16 |
| `crate::RedbByteBTree` | `crate::byte_btree::RedbByteBTree` | internal | ofdb/crates/btree-redb/src/lib.rs:17 |
| `crate::RedbByteBTreeTransaction` | `crate::byte_btree::RedbByteBTreeTransaction` | internal | ofdb/crates/btree-redb/src/lib.rs:17 |
| `crate::Bytes` | `crate::bytes::Bytes` | internal | ofdb/crates/btree-redb/src/lib.rs:18 |
| `crate::RedbDatabase` | `crate::database::RedbDatabase` | internal | ofdb/crates/btree-redb/src/lib.rs:19 |
| `crate::RedbDatabaseTransaction` | `crate::database::RedbDatabaseTransaction` | internal | ofdb/crates/btree-redb/src/lib.rs:19 |
| `crate::Key` | `crate::key::Key` | internal | ofdb/crates/btree-redb/src/lib.rs:20 |
| `crate::RedbKey` | `crate::redb::RedbKey` | internal | ofdb/crates/btree-redb/src/lib.rs:21 |
| `crate::RedbValue` | `crate::redb::RedbValue` | internal | ofdb/crates/btree-redb/src/lib.rs:21 |
| `crate::table_definition` | `crate::redb::table_definition` | internal | ofdb/crates/btree-redb/src/lib.rs:21 |
| `crate::RedbBTreeScopedTransaction` | `crate::transaction::RedbBTreeScopedTransaction` | internal | ofdb/crates/btree-redb/src/lib.rs:22 |
| `crate::RedbBTreeTransaction` | `crate::transaction::RedbBTreeTransaction` | internal | ofdb/crates/btree-redb/src/lib.rs:22 |
| `crate::Value` | `crate::value::Value` | internal | ofdb/crates/btree-redb/src/lib.rs:23 |
| `crate::NativeResource` | `crate::resource::NativeResource` | internal | ofdb/crates/btree-redb/src/lib.rs:25 |
| `crate::NativeTransaction` | `crate::resource::NativeTransaction` | internal | ofdb/crates/btree-redb/src/lib.rs:25 |
| `crate::open_unretired` | `crate::resource::open_unretired` | internal | ofdb/crates/btree-redb/src/lib.rs:25 |
| `crate::remove_retired` | `crate::resource::remove_retired` | internal | ofdb/crates/btree-redb/src/lib.rs:25 |
| `crate::NativeStageTracker` | `crate::stage_cleanup::NativeStageTracker` | internal | ofdb/crates/btree-redb/src/lib.rs:27 |
| `crate::SharedDatabase` | `crate::shared::SharedDatabase` | internal | ofdb/crates/btree-redb/src/lib.rs:29 |
| `crate::SharedNamespace` | `crate::shared::SharedNamespace` | internal | ofdb/crates/btree-redb/src/lib.rs:29 |

## crate `ofdb-kv` (lib `ofdb_kv`, dir `ofdb/crates/kv`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::ChangeWork` | `btree::changes::ChangeWork` | external | ofdb/crates/kv/src/lib.rs:35 |
| `crate::SharedDatabase` | `btree_redb::shared::SharedDatabase` | external | ofdb/crates/kv/src/lib.rs:37 |
| `crate::Client` | `crate::client::Client` | internal | ofdb/crates/kv/src/lib.rs:44 |
| `crate::Database` | `crate::database::Database` | internal | ofdb/crates/kv/src/lib.rs:50 |
| `crate::Error` | `crate::error::Error` | internal | ofdb/crates/kv/src/lib.rs:51 |
| `crate::ErrorKind` | `crate::error::ErrorKind` | internal | ofdb/crates/kv/src/lib.rs:51 |
| `crate::ChangeSubscription` | `btree::changes::ChangeSubscription` | external | ofdb/crates/kv/src/lib.rs:53 |
| `crate::SyncConfig` | `sync::protocol::Config` | external | ofdb/crates/kv/src/lib.rs:53 |
| `crate::SyncResult` | `sync::protocol::SyncResult` | external | ofdb/crates/kv/src/lib.rs:53 |
| `crate::SyncTransport` | `sync::transport::SyncTransport` | external | ofdb/crates/kv/src/lib.rs:53 |
| `crate::SyncError` | `sync::sync::SynchronizeError` | external | ofdb/crates/kv/src/lib.rs:53 |
| `crate::Transaction` | `crate::transaction::Transaction` | internal | ofdb/crates/kv/src/lib.rs:63 |
| `crate::Server` | `kv_server::Server` | external | ofdb/crates/kv/src/lib.rs:68 |
| `crate::JsonNumber` | `value::json_number::JsonNumber` | external | ofdb/crates/kv/src/lib.rs:70 |
| `crate::JsonValue` | `value::json_value::JsonValue` | external | ofdb/crates/kv/src/lib.rs:70 |
| `crate::Value` | `value::value::Value` | external | ofdb/crates/kv/src/lib.rs:70 |
| `crate::ValueType` | `value::value::ValueType` | external | ofdb/crates/kv/src/lib.rs:70 |
| `crate::encode_value` | `crate::value_codec::encode_value` | internal | ofdb/crates/kv/src/lib.rs:72 |
| `crate::parse_value` | `crate::value_codec::parse_value` | internal | ofdb/crates/kv/src/lib.rs:72 |
| `crate::automerge` | `automerge` | external | ofdb/crates/kv/src/lib.rs:75 |
| `crate::KvClient` | `crate::client::Client` | internal | ofdb/crates/kv/src/lib.rs:77 |
| `crate::Storage` | `crate::database::Storage` | internal | ofdb/crates/kv/src/lib.rs:79 |
| `crate::NativeChange` | `kv_store::NativeChange` | external | ofdb/crates/kv/src/lib.rs:81 |
| `crate::KvTransaction` | `crate::transaction::Transaction` | internal | ofdb/crates/kv/src/lib.rs:83 |
| `crate::KvSnapshot` | `crate::history::protocol::KvSnapshot` | internal | ofdb/crates/kv/src/lib.rs:88 |
| `crate::history::synchronize` | `crate::history::exchange::synchronize` | internal | ofdb/crates/kv/src/history/mod.rs:3 |
| `crate::history::synchronize_checked` | `crate::history::exchange::synchronize_checked` | internal | ofdb/crates/kv/src/history/mod.rs:3 |
| `crate::history::synchronize_selected_checked` | `crate::history::exchange::synchronize_selected_checked` | internal | ofdb/crates/kv/src/history/mod.rs:3 |
| `crate::history::Config` | `crate::history::protocol::Config` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::Error` | `crate::history::protocol::Error` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::KvChange` | `crate::history::protocol::KvChange` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::KvChanges` | `crate::history::protocol::KvChanges` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::KvInventory` | `crate::history::protocol::KvInventory` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::KvSnapshot` | `crate::history::protocol::KvSnapshot` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::Message` | `crate::history::protocol::Message` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::SyncRole` | `crate::history::protocol::SyncRole` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::SyncTransport` | `crate::history::protocol::SyncTransport` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::decode_frame` | `crate::history::protocol::decode_frame` | internal | ofdb/crates/kv/src/history/mod.rs:4 |
| `crate::history::encode_frame` | `crate::history::protocol::encode_frame` | internal | ofdb/crates/kv/src/history/mod.rs:4 |

## crate `ofdb-kv-client` (lib `ofdb_kv_client`, dir `ofdb/crates/kv-client`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::Client` | `crate::client::Client` | internal | ofdb/crates/kv-client/src/lib.rs:5 |

## crate `ofdb-kv-proto` (lib `kv_proto`, dir `ofdb/crates/kv-proto`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::FILE_DESCRIPTOR_SET` | `crate::kvdb::FILE_DESCRIPTOR_SET` | internal | ofdb/crates/kv-proto/src/lib.rs:5 |
| `crate::ErrorKind` | `crate::error::ErrorKind` | internal | ofdb/crates/kv-proto/src/lib.rs:9 |
| `crate::decode_error_details` | `crate::error::decode_error_details` | internal | ofdb/crates/kv-proto/src/lib.rs:9 |
| `crate::encode_error_details` | `crate::error::encode_error_details` | internal | ofdb/crates/kv-proto/src/lib.rs:9 |
| `crate::optional_value` | `crate::value::optional_value` | internal | ofdb/crates/kv-proto/src/lib.rs:14 |
| `crate::required_value` | `crate::value::required_value` | internal | ofdb/crates/kv-proto/src/lib.rs:14 |
| `crate::scan_entries` | `crate::value::scan_entries` | internal | ofdb/crates/kv-proto/src/lib.rs:14 |
| `crate::value_from_proto` | `crate::value::value_from_proto` | internal | ofdb/crates/kv-proto/src/lib.rs:14 |
| `crate::value_to_proto` | `crate::value::value_to_proto` | internal | ofdb/crates/kv-proto/src/lib.rs:14 |

## crate `ofdb-kv-server` (lib `ofdb_kv_server`, dir `ofdb/crates/kv-server`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::Server` | `crate::server::Server` | internal | ofdb/crates/kv-server/src/lib.rs:6 |

## crate `ofdb-kv-store` (lib `ofdb_kv_store`, dir `ofdb/crates/kv-store`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::ChangeApply` | `crate::history::ChangeApply` | internal | ofdb/crates/kv-store/src/lib.rs:4 |
| `crate::ChangeSummary` | `crate::history::ChangeSummary` | internal | ofdb/crates/kv-store/src/lib.rs:4 |
| `crate::KvHistory` | `crate::history::KvHistory` | internal | ofdb/crates/kv-store/src/lib.rs:4 |
| `crate::NativeChange` | `crate::history::NativeChange` | internal | ofdb/crates/kv-store/src/lib.rs:4 |
| `crate::decode_document_id` | `crate::document_id::decode_document_id` | internal | ofdb/crates/kv-store/src/lib.rs:9 |
| `crate::encode_document_id` | `crate::document_id::encode_document_id` | internal | ofdb/crates/kv-store/src/lib.rs:9 |
| `crate::KvInvalidation` | `crate::store::KvInvalidation` | internal | ofdb/crates/kv-store/src/lib.rs:11 |
| `crate::KvStore` | `crate::store::KvStore` | internal | ofdb/crates/kv-store/src/lib.rs:11 |
| `crate::KvTransaction` | `crate::store::KvTransaction` | internal | ofdb/crates/kv-store/src/lib.rs:11 |
| `crate::TimestampProvider` | `crate::store::TimestampProvider` | internal | ofdb/crates/kv-store/src/lib.rs:11 |
| `crate::ValueState` | `crate::value_document::ValueState` | internal | ofdb/crates/kv-store/src/lib.rs:12 |
| `crate::DocumentChangeKey` | `btree_automerge::document_change_key::DocumentChangeKey` | external | ofdb/crates/kv-store/src/lib.rs:14 |

## crate `ofdb-platform` (lib `ofdb_platform`, dir `ofdb/crates/platform`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::time::sleep` | `tokio::time::sleep` | external | ofdb/crates/platform/src/time.rs:5 |
| `crate::time::timeout` | `tokio::time::timeout` | external | ofdb/crates/platform/src/time.rs:5 |
| `crate::unix_millis` | `crate::clock::unix_millis` | internal | ofdb/crates/platform/src/lib.rs:13 |
| `crate::BoxFuture` | `crate::execution::BoxFuture` | internal | ofdb/crates/platform/src/lib.rs:14 |
| `crate::MaybeSend` | `core::marker::Send` | external | ofdb/crates/platform/src/lib.rs:14 |
| `crate::MaybeSync` | `core::marker::Sync` | external | ofdb/crates/platform/src/lib.rs:14 |
| `crate::sleep` | `tokio::time::sleep` | external | ofdb/crates/platform/src/lib.rs:16 |
| `crate::timeout` | `tokio::time::timeout` | external | ofdb/crates/platform/src/lib.rs:16 |
| `crate::execution::MaybeSend` | `core::marker::Send` | external | ofdb/crates/platform/src/execution.rs:6 |
| `crate::execution::MaybeSync` | `core::marker::Sync` | external | ofdb/crates/platform/src/execution.rs:6 |

## crate `ofdb-sql` (lib `ofdb_sql`, dir `ofdb/crates/sql`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::error::Error` | `query::error::QueryError` | external | ofdb/crates/sql/src/error.rs:1 |
| `crate::error::ErrorKind` | `query::error::QueryErrorKind` | external | ofdb/crates/sql/src/error.rs:1 |
| `crate::api::Engine` | `engine::engine::Engine` | external | ofdb/crates/sql/src/api.rs:7 |
| `crate::api::EngineError` | `engine::engine::EngineError` | external | ofdb/crates/sql/src/api.rs:7 |
| `crate::api::EngineResult` | `engine::engine::EngineResult` | external | ofdb/crates/sql/src/api.rs:7 |
| `crate::api::EngineTransaction` | `engine::engine::EngineTransaction` | external | ofdb/crates/sql/src/api.rs:7 |
| `crate::api::Kernel` | `engine::kernel::Kernel` | external | ofdb/crates/sql/src/api.rs:7 |
| `crate::api::KernelTransaction` | `engine::kernel::KernelTransaction` | external | ofdb/crates/sql/src/api.rs:7 |
| `crate::api::RowCodec` | `engine::codec::RowCodec` | external | ofdb/crates/sql/src/api.rs:7 |
| `crate::api::InMemoryKernel` | `engine::in_memory::InMemoryKernel` | external | ofdb/crates/sql/src/api.rs:11 |
| `crate::api::InMemoryKernelTransaction` | `engine::in_memory::InMemoryKernelTransaction` | external | ofdb/crates/sql/src/api.rs:11 |
| `crate::api::AutomergeRowCodec` | `engine_automerge::codec::AutomergeRowCodec` | external | ofdb/crates/sql/src/api.rs:18 |
| `crate::api::ChangeWork` | `btree::changes::ChangeWork` | external | ofdb/crates/sql/src/api.rs:20 |
| `crate::api::SqlInvalidation` | `engine_automerge::commit_invalidation::SqlInvalidation` | external | ofdb/crates/sql/src/api.rs:20 |
| `crate::api::RedbKernel` | `engine_redb::kernel::RedbKernel` | external | ofdb/crates/sql/src/api.rs:22 |
| `crate::api::RedbKernelTransaction` | `engine_redb::kernel::RedbKernelTransaction` | external | ofdb/crates/sql/src/api.rs:22 |
| `crate::api::redb` | `redb` | external | ofdb/crates/sql/src/api.rs:22 |
| `crate::api::FromRow` | `macros::FromRow` | external | ofdb/crates/sql/src/api.rs:24 |
| `crate::api::DataDefinition` | `query::DataDefinition` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::Query` | `query::Query` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryColumn` | `query::QueryColumn` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryDelete` | `query::QueryDelete` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryError` | `query::error::QueryError` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryErrorKind` | `query::error::QueryErrorKind` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryExpr` | `query::QueryExpr` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryExprValue` | `query::QueryExprValue` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryFrom` | `query::QueryFrom` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryInsert` | `query::QueryInsert` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryParams` | `query::QueryParams` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryResult` | `query::QueryResult` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QuerySelect` | `query::QuerySelect` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryUpdate` | `query::QueryUpdate` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::QueryUpdateAssignment` | `query::QueryUpdateAssignment` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::Statement` | `query::Statement` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::TranslateError` | `query::TranslateError` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::Translator` | `query::Translator` | external | ofdb/crates/sql/src/api.rs:25 |
| `crate::api::EngineExecutor` | `sql_server::EngineExecutor` | external | ofdb/crates/sql/src/api.rs:31 |
| `crate::api::Server` | `sql_server::Server` | external | ofdb/crates/sql/src/api.rs:31 |
| `crate::api::SqlTranslator` | `sql_translator::sql_translator::SqlTranslator` | external | ofdb/crates/sql/src/api.rs:33 |
| `crate::api::SessionConfig` | `crate::database::SessionConfig` | internal | ofdb/crates/sql/src/api.rs:36 |
| `crate::api::SyncCandidateView` | `crate::database::SyncCandidateView` | internal | ofdb/crates/sql/src/api.rs:36 |
| `crate::api::SyncKey` | `crate::database::SyncKey` | internal | ofdb/crates/sql/src/api.rs:36 |
| `crate::api::SyncManifest` | `crate::database::SyncManifest` | internal | ofdb/crates/sql/src/api.rs:36 |
| `crate::api::SyncRole` | `crate::database::SyncRole` | internal | ofdb/crates/sql/src/api.rs:36 |
| `crate::api::SyncSelection` | `crate::database::SyncSelection` | internal | ofdb/crates/sql/src/api.rs:36 |
| `crate::api::SyncStageFactory` | `crate::database::SyncStageFactory` | internal | ofdb/crates/sql/src/api.rs:36 |
| `crate::api::SyncStateUnit` | `crate::database::SyncStateUnit` | internal | ofdb/crates/sql/src/api.rs:36 |
| `crate::api::Uuid` | `uuid::Uuid` | external | ofdb/crates/sql/src/api.rs:41 |
| `crate::api::FromRowError` | `value::from_row::FromRowError` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::api::FromValue` | `value::from_row::FromValue` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::api::JsonNumber` | `value::json_number::JsonNumber` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::api::JsonValue` | `value::json_value::JsonValue` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::api::Row` | `value::value::Row` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::api::Value` | `value::value::Value` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::api::ValueType` | `value::value::ValueType` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::api::decode` | `value::from_row::decode` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::api::value` | `value::from_row::value` | external | ofdb/crates/sql/src/api.rs:42 |
| `crate::Migration` | `crate::migration::Migration` | internal | ofdb/crates/sql/src/lib.rs:45 |
| `crate::MigrationError` | `crate::migration::MigrationError` | internal | ofdb/crates/sql/src/lib.rs:45 |
| `crate::MigrationPhase` | `crate::migration::MigrationPhase` | internal | ofdb/crates/sql/src/lib.rs:45 |
| `crate::MigrationRecord` | `crate::migration::MigrationRecord` | internal | ofdb/crates/sql/src/lib.rs:45 |
| `crate::MigrationResult` | `crate::migration::MigrationResult` | internal | ofdb/crates/sql/src/lib.rs:45 |
| `crate::*` | `crate::api::*` | internal | ofdb/crates/sql/src/lib.rs:47 |
| `crate::Client` | `crate::client::Client` | internal | ofdb/crates/sql/src/lib.rs:56 |
| `crate::Transaction` | `crate::client::Transaction` | internal | ofdb/crates/sql/src/lib.rs:56 |
| `crate::Database` | `crate::database::Database` | internal | ofdb/crates/sql/src/lib.rs:63 |
| `crate::Error` | `query::error::QueryError` | external | ofdb/crates/sql/src/lib.rs:64 |
| `crate::ErrorKind` | `query::error::QueryErrorKind` | external | ofdb/crates/sql/src/lib.rs:64 |
| `crate::history` | `history` | external | ofdb/crates/sql/src/lib.rs:67 |
| `crate::SharedDatabase` | `btree_redb::shared::SharedDatabase` | external | ofdb/crates/sql/src/lib.rs:70 |
| `crate::PostgresStatement` | `crate::postgres_statement::PostgresStatement` | internal | ofdb/crates/sql/src/lib.rs:77 |

## crate `ofdb-sql-client` (lib `ofdb_sql_client`, dir `ofdb/crates/sql-client`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::Client` | `crate::client::Client` | internal | ofdb/crates/sql-client/src/lib.rs:6 |
| `crate::Transaction` | `crate::transaction::Transaction` | internal | ofdb/crates/sql-client/src/lib.rs:7 |

## crate `ofdb-sql-engine` (lib `engine`, dir `ofdb/crates/sql-engine`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::codec::Uuid` | `uuid::Uuid` | external | ofdb/crates/sql-engine/src/codec.rs:8 |
| `crate::BytesTable` | `crate::bytes_table::BytesTable` | internal | ofdb/crates/sql-engine/src/lib.rs:23 |
| `crate::BytesTableTransaction` | `crate::bytes_table::BytesTableTransaction` | internal | ofdb/crates/sql-engine/src/lib.rs:23 |
| `crate::Change` | `crate::change::Change` | internal | ofdb/crates/sql-engine/src/lib.rs:24 |
| `crate::ChangeKey` | `crate::change::ChangeKey` | internal | ofdb/crates/sql-engine/src/lib.rs:24 |
| `crate::RowCatalog` | `crate::codec::RowCatalog` | internal | ofdb/crates/sql-engine/src/lib.rs:25 |
| `crate::RowCodec` | `crate::codec::RowCodec` | internal | ofdb/crates/sql-engine/src/lib.rs:25 |
| `crate::DocumentChangeId` | `crate::document::DocumentChangeId` | internal | ofdb/crates/sql-engine/src/lib.rs:26 |
| `crate::DocumentDigest` | `crate::document::DocumentDigest` | internal | ofdb/crates/sql-engine/src/lib.rs:26 |
| `crate::DocumentSummary` | `crate::document::DocumentSummary` | internal | ofdb/crates/sql-engine/src/lib.rs:26 |
| `crate::InspectedHistory` | `crate::document::InspectedHistory` | internal | ofdb/crates/sql-engine/src/lib.rs:26 |
| `crate::RowReconciler` | `crate::document::RowReconciler` | internal | ofdb/crates/sql-engine/src/lib.rs:26 |
| `crate::RowStateView` | `crate::document::RowStateView` | internal | ofdb/crates/sql-engine/src/lib.rs:26 |
| `crate::TableStateView` | `crate::document::TableStateView` | internal | ofdb/crates/sql-engine/src/lib.rs:26 |
| `crate::Engine` | `crate::engine::Engine` | internal | ofdb/crates/sql-engine/src/lib.rs:30 |
| `crate::EngineError` | `crate::engine::EngineError` | internal | ofdb/crates/sql-engine/src/lib.rs:30 |
| `crate::EngineResult` | `crate::engine::EngineResult` | internal | ofdb/crates/sql-engine/src/lib.rs:30 |
| `crate::EngineTransaction` | `crate::engine::EngineTransaction` | internal | ofdb/crates/sql-engine/src/lib.rs:30 |
| `crate::TimestampProvider` | `crate::engine::TimestampProvider` | internal | ofdb/crates/sql-engine/src/lib.rs:30 |
| `crate::CatalogTable` | `crate::schema::CatalogTable` | internal | ofdb/crates/sql-engine/src/lib.rs:31 |
| `crate::ENGINE_INDEX_FIELDS_STORAGE` | `crate::schema::ENGINE_INDEX_FIELDS_STORAGE` | internal | ofdb/crates/sql-engine/src/lib.rs:31 |
| `crate::ENGINE_INDICES_STORAGE` | `crate::schema::ENGINE_INDICES_STORAGE` | internal | ofdb/crates/sql-engine/src/lib.rs:31 |
| `crate::ENGINE_TABLE_FIELDS_STORAGE` | `crate::schema::ENGINE_TABLE_FIELDS_STORAGE` | internal | ofdb/crates/sql-engine/src/lib.rs:31 |
| `crate::ENGINE_TABLES_STORAGE` | `crate::schema::ENGINE_TABLES_STORAGE` | internal | ofdb/crates/sql-engine/src/lib.rs:31 |
| `crate::catalog_table_for_storage` | `crate::schema::catalog_table_for_storage` | internal | ofdb/crates/sql-engine/src/lib.rs:31 |
| `crate::validate_row_id` | `crate::schema::validate_row_id` | internal | ofdb/crates/sql-engine/src/lib.rs:31 |
| `crate::Uuid` | `uuid::Uuid` | external | ofdb/crates/sql-engine/src/lib.rs:35 |
| `crate::InMemoryKernel` | `crate::in_memory::InMemoryKernel` | internal | ofdb/crates/sql-engine/src/lib.rs:38 |
| `crate::InMemoryKernelTransaction` | `crate::in_memory::InMemoryKernelTransaction` | internal | ofdb/crates/sql-engine/src/lib.rs:38 |
| `crate::Kernel` | `crate::kernel::Kernel` | internal | ofdb/crates/sql-engine/src/lib.rs:39 |
| `crate::KernelTransaction` | `crate::kernel::KernelTransaction` | internal | ofdb/crates/sql-engine/src/lib.rs:39 |
| `crate::RowTable` | `crate::row_table::RowTable` | internal | ofdb/crates/sql-engine/src/lib.rs:41 |
| `crate::MutationSummary` | `crate::state_transfer::MutationSummary` | internal | ofdb/crates/sql-engine/src/lib.rs:42 |
| `crate::RowMutation` | `crate::state_transfer::RowMutation` | internal | ofdb/crates/sql-engine/src/lib.rs:42 |

## crate `ofdb-sql-engine-automerge` (lib `engine_automerge`, dir `ofdb/crates/sql-engine-automerge`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::ChangeWork` | `btree::changes::ChangeWork` | external | ofdb/crates/sql-engine-automerge/src/lib.rs:5 |
| `crate::SqlInvalidation` | `crate::commit_invalidation::SqlInvalidation` | internal | ofdb/crates/sql-engine-automerge/src/lib.rs:6 |
| `crate::committed_invalidations` | `crate::commit_invalidation::committed_invalidations` | internal | ofdb/crates/sql-engine-automerge/src/lib.rs:6 |
| `crate::AutomergeRowCodec` | `crate::codec::AutomergeRowCodec` | internal | ofdb/crates/sql-engine-automerge/src/lib.rs:8 |
| `crate::RowMetadata` | `crate::codec::RowMetadata` | internal | ofdb/crates/sql-engine-automerge/src/lib.rs:8 |

## crate `ofdb-sql-engine-indexeddb` (lib `ofdb_sql_engine_indexeddb`, dir `ofdb/crates/sql-engine-indexeddb`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::IndexedDbKernel` | `crate::kernel::IndexedDbKernel` | internal | ofdb/crates/sql-engine-indexeddb/src/lib.rs:7 |
| `crate::IndexedDbTransaction` | `crate::kernel::IndexedDbTransaction` | internal | ofdb/crates/sql-engine-indexeddb/src/lib.rs:7 |

## crate `ofdb-sql-engine-redb` (lib `engine_redb`, dir `ofdb/crates/sql-engine-redb`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::RedbKernel` | `crate::kernel::RedbKernel` | internal | ofdb/crates/sql-engine-redb/src/lib.rs:3 |
| `crate::RedbKernelTransaction` | `crate::kernel::RedbKernelTransaction` | internal | ofdb/crates/sql-engine-redb/src/lib.rs:3 |
| `crate::redb` | `redb` | external | ofdb/crates/sql-engine-redb/src/lib.rs:4 |
| `crate::NativeResource` | `btree_redb::resource::NativeResource` | external | ofdb/crates/sql-engine-redb/src/lib.rs:6 |
| `crate::NativeTransaction` | `btree_redb::resource::NativeTransaction` | external | ofdb/crates/sql-engine-redb/src/lib.rs:6 |
| `crate::RedbDatabase` | `btree_redb::database::RedbDatabase` | external | ofdb/crates/sql-engine-redb/src/lib.rs:6 |
| `crate::SharedDatabase` | `btree_redb::shared::SharedDatabase` | external | ofdb/crates/sql-engine-redb/src/lib.rs:6 |
| `crate::SharedNamespace` | `btree_redb::shared::SharedNamespace` | external | ofdb/crates/sql-engine-redb/src/lib.rs:6 |
| `crate::open_unretired` | `btree_redb::resource::open_unretired` | external | ofdb/crates/sql-engine-redb/src/lib.rs:6 |
| `crate::remove_retired` | `btree_redb::resource::remove_retired` | external | ofdb/crates/sql-engine-redb/src/lib.rs:6 |

## crate `ofdb-sql-history` (lib `history`, dir `ofdb/crates/sql-history`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::state::SyncChangeId` | `engine::document::DocumentChangeId` | external | ofdb/crates/sql-history/src/state.rs:6 |
| `crate::state::StateDigest` | `engine::document::DocumentDigest` | external | ofdb/crates/sql-history/src/state.rs:6 |
| `crate::candidate::SyncRowView` | `engine::document::RowStateView` | external | ofdb/crates/sql-history/src/candidate.rs:26 |
| `crate::candidate::SyncTableView` | `engine::document::TableStateView` | external | ofdb/crates/sql-history/src/candidate.rs:26 |
| `crate::codec::InspectedHistory` | `engine::document::InspectedHistory` | external | ofdb/crates/sql-history/src/codec.rs:1 |
| `crate::codec::SyncRowCodec` | `engine::document::RowReconciler` | external | ofdb/crates/sql-history/src/codec.rs:1 |
| `crate::FileSyncStage` | `crate::file_stage::FileSyncStage` | internal | ofdb/crates/sql-history/src/lib.rs:21 |
| `crate::FileSyncStageFactory` | `crate::file_stage::FileSyncStageFactory` | internal | ofdb/crates/sql-history/src/lib.rs:21 |
| `crate::SyncSelection` | `crate::selection::SyncSelection` | internal | ofdb/crates/sql-history/src/lib.rs:26 |
| `crate::SyncCandidateView` | `crate::candidate::SyncCandidateView` | internal | ofdb/crates/sql-history/src/lib.rs:32 |
| `crate::SyncRowView` | `engine::document::RowStateView` | external | ofdb/crates/sql-history/src/lib.rs:32 |
| `crate::SyncTableView` | `engine::document::TableStateView` | external | ofdb/crates/sql-history/src/lib.rs:32 |
| `crate::SyncValidationDirection` | `crate::candidate::SyncValidationDirection` | internal | ofdb/crates/sql-history/src/lib.rs:32 |
| `crate::InspectedHistory` | `engine::document::InspectedHistory` | external | ofdb/crates/sql-history/src/lib.rs:33 |
| `crate::SyncRowCodec` | `engine::document::RowReconciler` | external | ofdb/crates/sql-history/src/lib.rs:33 |
| `crate::PROTOCOL_VERSION` | `crate::protocol::PROTOCOL_VERSION` | internal | ofdb/crates/sql-history/src/lib.rs:34 |
| `crate::SyncHello` | `crate::protocol::SyncHello` | internal | ofdb/crates/sql-history/src/lib.rs:34 |
| `crate::SyncIncrementalChange` | `crate::protocol::SyncIncrementalChange` | internal | ofdb/crates/sql-history/src/lib.rs:34 |
| `crate::SyncMessage` | `crate::protocol::SyncMessage` | internal | ofdb/crates/sql-history/src/lib.rs:34 |
| `crate::SyncRowInventory` | `crate::protocol::SyncRowInventory` | internal | ofdb/crates/sql-history/src/lib.rs:34 |
| `crate::SyncSnapshotRequest` | `crate::protocol::SyncSnapshotRequest` | internal | ofdb/crates/sql-history/src/lib.rs:34 |
| `crate::DEFAULT_MAX_SESSION_BYTES` | `crate::session::DEFAULT_MAX_SESSION_BYTES` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::MAX_APPLY_BATCH_BYTES` | `crate::session::MAX_APPLY_BATCH_BYTES` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::MAX_MESSAGE_BYTES` | `crate::session::MAX_MESSAGE_BYTES` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::SessionConfig` | `crate::session::SessionConfig` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::SyncError` | `crate::session::SyncError` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::SyncResult` | `crate::session::SyncResult` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::SyncRole` | `crate::session::SyncRole` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::apply_incremental_changes_for` | `crate::session::apply_incremental_changes_for` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::apply_sync_state_batch_for` | `crate::session::apply_sync_state_batch_for` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::apply_sync_state_for` | `crate::session::apply_sync_state_for` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::export_sync_state_for` | `crate::session::export_sync_state_for` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::sync_manifest_for` | `crate::session::sync_manifest_for` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::synchronize` | `crate::session::synchronize` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::synchronize_selected_with_stage_factory_checked` | `crate::session::synchronize_selected_with_stage_factory_checked` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::synchronize_tables_with_stage_factory_candidate_validated_checked` | `crate::session::synchronize_tables_with_stage_factory_candidate_validated_checked` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::synchronize_tables_with_stage_factory_checked` | `crate::session::synchronize_tables_with_stage_factory_checked` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::synchronize_tables_with_stage_factory_validated_checked` | `crate::session::synchronize_tables_with_stage_factory_validated_checked` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::synchronize_with_stage_factory` | `crate::session::synchronize_with_stage_factory` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::synchronize_with_stage_factory_checked` | `crate::session::synchronize_with_stage_factory_checked` | internal | ofdb/crates/sql-history/src/lib.rs:38 |
| `crate::MemorySyncStageFactory` | `crate::staging::MemorySyncStageFactory` | internal | ofdb/crates/sql-history/src/lib.rs:48 |
| `crate::SyncStage` | `crate::staging::SyncStage` | internal | ofdb/crates/sql-history/src/lib.rs:48 |
| `crate::SyncStageFactory` | `crate::staging::SyncStageFactory` | internal | ofdb/crates/sql-history/src/lib.rs:48 |
| `crate::StateDigest` | `engine::document::DocumentDigest` | external | ofdb/crates/sql-history/src/lib.rs:49 |
| `crate::SyncChangeId` | `engine::document::DocumentChangeId` | external | ofdb/crates/sql-history/src/lib.rs:49 |
| `crate::SyncKey` | `crate::state::SyncKey` | internal | ofdb/crates/sql-history/src/lib.rs:49 |
| `crate::SyncManifest` | `crate::state::SyncManifest` | internal | ofdb/crates/sql-history/src/lib.rs:49 |
| `crate::SyncStateUnit` | `crate::state::SyncStateUnit` | internal | ofdb/crates/sql-history/src/lib.rs:49 |
| `crate::SyncTransport` | `crate::transport::SyncTransport` | internal | ofdb/crates/sql-history/src/lib.rs:50 |
| `crate::NativeSyncStage` | `crate::shared_stage::NativeSyncStage` | internal | ofdb/crates/sql-history/src/lib.rs:55 |
| `crate::NativeSyncStageFactory` | `crate::shared_stage::NativeSyncStageFactory` | internal | ofdb/crates/sql-history/src/lib.rs:55 |

## crate `ofdb-sql-protocol` (lib `ofdb_sql_protocol`, dir `ofdb/crates/sql-protocol`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::QueryServiceError` | `crate::error::QueryServiceError` | internal | ofdb/crates/sql-protocol/src/lib.rs:15 |
| `crate::decode_error_detail` | `crate::error::decode_error_detail` | internal | ofdb/crates/sql-protocol/src/lib.rs:16 |
| `crate::encode_error_detail` | `crate::error::encode_error_detail` | internal | ofdb/crates/sql-protocol/src/lib.rs:16 |
| `crate::ConflictValues` | `crate::executor::ConflictValues` | internal | ofdb/crates/sql-protocol/src/lib.rs:17 |
| `crate::QueryExecutor` | `crate::executor::QueryExecutor` | internal | ofdb/crates/sql-protocol/src/lib.rs:17 |
| `crate::QueryTransaction` | `crate::executor::QueryTransaction` | internal | ofdb/crates/sql-protocol/src/lib.rs:17 |
| `crate::query_column_from_proto` | `crate::query::query_column_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_column_to_proto` | `crate::query::query_column_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_expr_from_proto` | `crate::query::query_expr_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_expr_to_proto` | `crate::query::query_expr_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_expr_value_from_proto` | `crate::query::query_expr_value_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_expr_value_to_proto` | `crate::query::query_expr_value_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_from_proto` | `crate::query::query_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_select_from_proto` | `crate::query::query_select_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_select_to_proto` | `crate::query::query_select_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_to_proto` | `crate::query::query_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_update_assignment_from_proto` | `crate::query::query_update_assignment_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_update_assignment_to_proto` | `crate::query::query_update_assignment_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::statement_from_proto` | `crate::query::statement_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::statement_to_proto` | `crate::query::statement_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:18 |
| `crate::query_result_from_proto` | `crate::result::query_result_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:25 |
| `crate::query_result_to_proto` | `crate::result::query_result_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:25 |
| `crate::result_column_from_proto` | `crate::result::result_column_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:25 |
| `crate::result_column_to_proto` | `crate::result::result_column_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:25 |
| `crate::row_from_proto` | `crate::result::row_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:25 |
| `crate::row_to_proto` | `crate::result::row_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:25 |
| `crate::column_from_proto` | `crate::schema::column_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:29 |
| `crate::column_to_proto` | `crate::schema::column_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:29 |
| `crate::index_from_proto` | `crate::schema::index_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:29 |
| `crate::index_to_proto` | `crate::schema::index_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:29 |
| `crate::table_from_proto` | `crate::schema::table_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:29 |
| `crate::table_to_proto` | `crate::schema::table_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:29 |
| `crate::value_from_proto` | `crate::value::value_from_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:33 |
| `crate::value_to_proto` | `crate::value::value_to_proto` | internal | ofdb/crates/sql-protocol/src/lib.rs:33 |

## crate `ofdb-sql-query` (lib `query`, dir `ofdb/crates/sql-query`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::QueryError` | `crate::error::QueryError` | internal | ofdb/crates/sql-query/src/lib.rs:10 |
| `crate::QueryErrorKind` | `crate::error::QueryErrorKind` | internal | ofdb/crates/sql-query/src/lib.rs:10 |
| `crate::*` | `crate::query::*` | internal | ofdb/crates/sql-query/src/lib.rs:11 |

## crate `ofdb-sql-schema` (lib `schema`, dir `ofdb/crates/sql-schema`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::*` | `crate::schema::*` | internal | ofdb/crates/sql-schema/src/lib.rs:8 |

## crate `ofdb-sql-server` (lib `ofdb_sql_server`, dir `ofdb/crates/sql-server`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::EngineExecutor` | `crate::executor::EngineExecutor` | internal | ofdb/crates/sql-server/src/lib.rs:7 |
| `crate::Server` | `crate::server::Server` | internal | ofdb/crates/sql-server/src/lib.rs:8 |
| `crate::QueryService` | `crate::service::QueryService` | internal | ofdb/crates/sql-server/src/lib.rs:9 |

## crate `ofdb-sql-test` (lib `test`, dir `ofdb/crates/sql-test`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::Expectation` | `crate::case::Expectation` | internal | ofdb/crates/sql-test/src/lib.rs:8 |
| `crate::ExpectedError` | `crate::case::ExpectedError` | internal | ofdb/crates/sql-test/src/lib.rs:8 |
| `crate::NodeId` | `crate::case::NodeId` | internal | ofdb/crates/sql-test/src/lib.rs:8 |
| `crate::Step` | `crate::case::Step` | internal | ofdb/crates/sql-test/src/lib.rs:8 |
| `crate::TestCase` | `crate::case::TestCase` | internal | ofdb/crates/sql-test/src/lib.rs:8 |
| `crate::TestCaseBuilder` | `crate::case::TestCaseBuilder` | internal | ofdb/crates/sql-test/src/lib.rs:8 |
| `crate::TestSuite` | `crate::case::TestSuite` | internal | ofdb/crates/sql-test/src/lib.rs:8 |
| `crate::RunnerError` | `crate::runner::RunnerError` | internal | ofdb/crates/sql-test/src/lib.rs:9 |
| `crate::TestRunner` | `crate::runner::TestRunner` | internal | ofdb/crates/sql-test/src/lib.rs:9 |
| `crate::VerificationPolicy` | `crate::runner::VerificationPolicy` | internal | ofdb/crates/sql-test/src/lib.rs:9 |
| `crate::run` | `crate::runtime::run` | internal | ofdb/crates/sql-test/src/lib.rs:10 |
| `crate::InMemoryTransport` | `crate::transport::InMemoryTransport` | internal | ofdb/crates/sql-test/src/lib.rs:11 |
| `crate::InMemoryTransportError` | `crate::transport::InMemoryTransportError` | internal | ofdb/crates/sql-test/src/lib.rs:11 |
| `crate::TransportDirection` | `crate::transport::TransportDirection` | internal | ofdb/crates/sql-test/src/lib.rs:11 |
| `crate::in_memory_transport_pair` | `crate::transport::in_memory_transport_pair` | internal | ofdb/crates/sql-test/src/lib.rs:11 |
| `crate::in_memory_transport_pair_failing` | `crate::transport::in_memory_transport_pair_failing` | internal | ofdb/crates/sql-test/src/lib.rs:11 |
| `crate::in_memory_transport_pair_failing_at` | `crate::transport::in_memory_transport_pair_failing_at` | internal | ofdb/crates/sql-test/src/lib.rs:11 |
| `crate::Row` | `value::value::Row` | external | ofdb/crates/sql-test/src/lib.rs:15 |
| `crate::Value` | `value::value::Value` | external | ofdb/crates/sql-test/src/lib.rs:15 |

## crate `ofdb-sql-translator` (lib `sql_translator`, dir `ofdb/crates/sql-translator`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::SqlTranslator` | `crate::sql_translator::SqlTranslator` | internal | ofdb/crates/sql-translator/src/lib.rs:8 |

## crate `ofdb-sync` (lib `sync`, dir `ofdb/crates/sync`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::wasm_sync::PushSyncTransport` | `crate::transport::SyncTransport` | internal | ofdb/crates/sync/src/wasm_sync.rs:24 |
| `crate::AckConfig` | `crate::protocol::AckConfig` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::Config` | `crate::protocol::Config` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::Error` | `crate::protocol::Error` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::HelloConfig` | `crate::protocol::HelloConfig` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::MAX_MESSAGE_BYTES` | `crate::protocol::MAX_MESSAGE_BYTES` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::Message` | `crate::protocol::Message` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::SyncResult` | `crate::protocol::SyncResult` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::SyncRole` | `crate::protocol::SyncRole` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::decode_message` | `crate::protocol::decode_message` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::encode_message` | `crate::protocol::encode_message` | internal | ofdb/crates/sync/src/lib.rs:10 |
| `crate::SyncTransport` | `crate::transport::SyncTransport` | internal | ofdb/crates/sync/src/lib.rs:14 |
| `crate::SynchronizeConfig` | `crate::sync::SynchronizeConfig` | internal | ofdb/crates/sync/src/lib.rs:20 |
| `crate::SynchronizeError` | `crate::sync::SynchronizeError` | internal | ofdb/crates/sync/src/lib.rs:20 |
| `crate::synchronize` | `crate::sync::synchronize` | internal | ofdb/crates/sync/src/lib.rs:20 |
| `crate::PushConfig` | `crate::wasm_sync::PushConfig` | internal | ofdb/crates/sync/src/lib.rs:26 |
| `crate::PushMessage` | `crate::wasm_sync::PushMessage` | internal | ofdb/crates/sync/src/lib.rs:26 |
| `crate::PushResult` | `crate::wasm_sync::PushResult` | internal | ofdb/crates/sync/src/lib.rs:26 |
| `crate::PushSyncTransport` | `crate::transport::SyncTransport` | internal | ofdb/crates/sync/src/lib.rs:26 |
| `crate::PushSynchronizeConfig` | `crate::wasm_sync::PushSynchronizeConfig` | internal | ofdb/crates/sync/src/lib.rs:26 |
| `crate::PushSynchronizeError` | `crate::wasm_sync::PushSynchronizeError` | internal | ofdb/crates/sync/src/lib.rs:26 |
| `crate::IrohTransport` | `crate::iroh_transport::IrohTransport` | internal | ofdb/crates/sync/src/lib.rs:32 |
| `crate::ChangeEvent` | `btree::changes::ChangeEvent` | external | ofdb/crates/sync/src/lib.rs:34 |
| `crate::ChangeSubscription` | `btree::changes::ChangeSubscription` | external | ofdb/crates/sync/src/lib.rs:36 |

## crate `ofdb-value` (lib `value`, dir `ofdb/crates/value`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::FromRow` | `crate::from_row::FromRow` | internal | ofdb/crates/value/src/lib.rs:13 |
| `crate::FromRowError` | `crate::from_row::FromRowError` | internal | ofdb/crates/value/src/lib.rs:13 |
| `crate::FromValue` | `crate::from_row::FromValue` | internal | ofdb/crates/value/src/lib.rs:13 |
| `crate::decode` | `crate::from_row::decode` | internal | ofdb/crates/value/src/lib.rs:13 |
| `crate::value` | `crate::from_row::value` | internal | ofdb/crates/value/src/lib.rs:13 |
| `crate::JsonNumber` | `crate::json_number::JsonNumber` | internal | ofdb/crates/value/src/lib.rs:14 |
| `crate::JsonValue` | `crate::json_value::JsonValue` | internal | ofdb/crates/value/src/lib.rs:15 |
| `crate::Row` | `crate::value::Row` | internal | ofdb/crates/value/src/lib.rs:16 |
| `crate::Value` | `crate::value::Value` | internal | ofdb/crates/value/src/lib.rs:16 |
| `crate::ValueType` | `crate::value::ValueType` | internal | ofdb/crates/value/src/lib.rs:16 |

## crate `ofdb-wasm` (lib `ofdb_wasm`, dir `ofdb/crates/wasm`)

| exported path | canonical | kind | file:line |
|---|---|---|---|
| `crate::JsChangeEvent` | `crate::sync::JsChangeEvent` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsPushSyncError` | `crate::sync::JsPushSyncError` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsPushSyncErrorKind` | `crate::sync::JsPushSyncErrorKind` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsPushSyncTransport` | `crate::sync::JsPushSyncTransport` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsPushSyncTransportError` | `crate::sync::JsPushSyncTransportError` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::PushConfig` | `crate::sync::PushConfig` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::PushSynchronizeConfig` | `crate::sync::PushSynchronizeConfig` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::PushSynchronizeResult` | `crate::sync::PushSynchronizeResult` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::KvChangeSubscription` | `crate::sync::KvChangeSubscription` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::SqlChangeSubscription` | `crate::sync::SqlChangeSubscription` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsKvSyncError` | `crate::sync::JsKvSyncError` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsKvSyncErrorKind` | `crate::sync::JsKvSyncErrorKind` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsKvSyncTransport` | `crate::sync::JsKvSyncTransport` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsSqlSyncError` | `crate::sync::JsSqlSyncError` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsSqlSyncErrorKind` | `crate::sync::JsSqlSyncErrorKind` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::JsSqlSyncTransport` | `crate::sync::JsSqlSyncTransport` | internal | ofdb/crates/wasm/src/lib.rs:17 |
| `crate::KvClient` | `crate::kv::KvClient` | internal | ofdb/crates/wasm/src/lib.rs:24 |
| `crate::KvDatabase` | `crate::kv::KvDatabase` | internal | ofdb/crates/wasm/src/lib.rs:24 |
| `crate::KvTransaction` | `crate::kv::KvTransaction` | internal | ofdb/crates/wasm/src/lib.rs:24 |
| `crate::SqlClient` | `crate::sql::SqlClient` | internal | ofdb/crates/wasm/src/lib.rs:25 |
| `crate::SqlDatabase` | `crate::sql::SqlDatabase` | internal | ofdb/crates/wasm/src/lib.rs:25 |
| `crate::SqlTransaction` | `crate::sql::SqlTransaction` | internal | ofdb/crates/wasm/src/lib.rs:25 |

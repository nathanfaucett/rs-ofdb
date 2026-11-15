use btree_automerge::DocumentChangeKey;
use engine::{InMemoryKernel, Kernel, KernelTransaction, RowCodec};
use engine_automerge::{AutomergeRowCodec, RowMetadata};
use futures::{StreamExt, TryStreamExt, executor::block_on, pin_mut};
use uuid::Uuid;
use value::Row;

#[test]
fn warmed_catalog_rejects_changed_bytes_even_under_the_same_storage_key() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let codec = AutomergeRowCodec::new();
        let mut transaction = kernel.transaction().await.expect("catalog transaction");
        let id = Uuid::now_v7();
        codec
            .ensure_table(&mut transaction, "tables")
            .await
            .expect("catalog storage");
        codec
            .put_row(&mut transaction, "tables", id, Row::from(["items"]))
            .await
            .expect("catalog row");
        let initial = codec
            .scan_row_states(&transaction, "tables")
            .try_collect::<Vec<_>>()
            .await
            .expect("warm decoded catalog");
        assert_eq!(initial, vec![(id, Row::from(["items"]), false)]);
        let keys = transaction
            .scan_bytes("tables")
            .try_collect::<Vec<_>>()
            .await
            .expect("stored catalog history");
        let (key, _) = keys.first().expect("stored snapshot");
        transaction
            .put_bytes(
                "tables",
                key.clone(),
                b"invalid Automerge snapshot".to_vec(),
            )
            .await
            .expect("replace bytes under unchanged key");
        let states = codec.scan_row_states(&transaction, "tables");
        pin_mut!(states);
        states.next().await.expect("catalog decode result").unwrap_err();
    });
}

#[test]
fn warmed_catalog_observes_metadata_changes_and_rejects_malformed_metadata() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let codec = AutomergeRowCodec::new();
        let mut transaction = kernel.transaction().await.expect("catalog transaction");
        let id = Uuid::now_v7();
        codec
            .ensure_table(&mut transaction, "tables")
            .await
            .expect("catalog storage");
        codec
            .put_row(&mut transaction, "tables", id, Row::from(["items"]))
            .await
            .expect("catalog row");
        assert_eq!(
            codec
                .scan_row_states(&transaction, "tables")
                .try_collect::<Vec<_>>()
                .await
                .expect("warm catalog"),
            vec![(id, Row::from(["items"]), false)]
        );
        let key = DocumentChangeKey::new_metadata(id.as_bytes().to_vec()).encode_ordered();
        transaction
            .put_bytes(
                "tables",
                key.clone(),
                postcard::to_allocvec(&RowMetadata {
                    version: 1,
                    deleted: true,
                    drop_events: vec![Uuid::now_v7()],
                    observed_drops: Vec::new(),
                })
                .expect("encoded tombstone"),
            )
            .await
            .expect("catalog tombstone");
        assert_eq!(
            codec
                .scan_row_states(&transaction, "tables")
                .try_collect::<Vec<_>>()
                .await
                .expect("fresh catalog metadata"),
            vec![(id, Row::from(["items"]), true)]
        );
        transaction
            .put_bytes("tables", key, vec![255])
            .await
            .expect("malformed metadata");
        let states = codec.scan_row_states(&transaction, "tables");
        pin_mut!(states);
        states
                .next()
                .await
                .expect("metadata decode result").unwrap_err();
    });
}

use futures::executor::block_on;
use ofdb_kv::Database;
use value::Value;

#[test]
fn redb_client_shares_storage_and_survives_database_drop() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("embedded.redb");
    let client = {
        let database = Database::open(&path).expect("open embedded database");
        let client = database.client();
        let shared = database.client();
        block_on(async {
            client
                .set("binary", Value::Blob(vec![0, 255]), None)
                .await
                .expect("write through client");
            assert_eq!(
                shared
                    .get("binary")
                    .await
                    .expect("database reads client write"),
                Some(Value::Blob(vec![0, 255]))
            );
        });
        client
    };
    block_on(async {
        assert_eq!(
            client
                .get("binary")
                .await
                .expect("client works after database drop"),
            Some(Value::Blob(vec![0, 255]))
        );
    });
    drop(client);

    block_on(async {
        let reopened = Database::open(&path).expect("reopen persisted database");
        assert_eq!(
            reopened
                .client()
                .get("binary")
                .await
                .expect("read persisted value"),
            Some(Value::Blob(vec![0, 255]))
        );
    });
}

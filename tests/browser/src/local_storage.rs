use std::{ops::Bound, rc::Rc};

use engine::{
    Engine, EngineResult, InMemoryKernel, InMemoryKernelTransaction, Kernel, KernelTransaction,
};
use engine_automerge::AutomergeRowCodec;
use futures::Stream;
use wasm_bindgen_test::wasm_bindgen_test;

struct LocalKernel(Rc<InMemoryKernel>);
struct LocalTransaction {
    inner: InMemoryKernelTransaction,
    owner: Rc<InMemoryKernel>,
}

impl Kernel for LocalKernel {
    type Transaction = LocalTransaction;
    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        // Hold a non-Send owner across a browser event-loop turn.
        let owner = self.0.clone();
        platform::sleep(core::time::Duration::from_millis(1)).await;
        Ok(LocalTransaction {
            inner: owner.transaction().await?,
            owner,
        })
    }
}

impl KernelTransaction for LocalTransaction {
    async fn ensure_table(&mut self, table: &str) -> EngineResult<()> {
        self.inner.ensure_table(table).await
    }
    async fn drop_table(&mut self, table: &str) -> EngineResult<()> {
        self.inner.drop_table(table).await
    }
    async fn get_bytes(&self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.inner.get_bytes(table, key).await
    }
    fn scan_bytes<'a>(
        &'a self,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + 'a {
        self.inner.scan_bytes(table)
    }
    fn scan_bytes_range<'a>(
        &'a self,
        table: &'a str,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + 'a {
        self.inner.scan_bytes_range(table, range)
    }
    async fn put_bytes(&mut self, table: &str, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        self.inner.put_bytes(table, key, value).await
    }
    async fn remove_bytes(&mut self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.inner.remove_bytes(table, key).await
    }
    async fn commit(self) -> EngineResult<()> {
        self.inner.commit().await?;
        drop(self.owner);
        Ok(())
    }
    async fn rollback(self) -> EngineResult<()> {
        self.inner.rollback().await
    }
}

#[wasm_bindgen_test]
async fn local_kernel_codec_reconciler_streams_and_boxed_callbacks() {
    let engine = Engine::new(
        LocalKernel(Rc::new(InMemoryKernel::new())),
        AutomergeRowCodec::default(),
    );
    let translator = ofdb_sql::SqlTranslator;
    engine
        .translate_and_execute("CREATE TABLE people (name TEXT PRIMARY KEY)", &translator)
        .await
        .expect("local kernel catalog");
    engine
        .translate_and_execute("INSERT INTO people VALUES ('Ada')", &translator)
        .await
        .expect("local codec write");
    let rows = engine
        .translate_and_execute("SELECT name FROM people", &translator)
        .await
        .expect("local streams");
    assert_eq!(rows[0].rows, vec![ofdb_sql::Row::from(["Ada"])]);
    let local = Rc::new("retained across await".to_owned());
    let result = engine
        .read_transaction(|_, _| {
            Box::pin(async move {
                platform::sleep(core::time::Duration::from_millis(1)).await;
                Ok(local)
            })
        })
        .await
        .expect("local boxed callback");
    assert_eq!(result.as_str(), "retained across await");
}

#[wasm_bindgen_test]
async fn local_btree_values_streams_and_update_callbacks() {
    use btree::{BTree, BTreeRead, BTreeTransaction, InMemoryBTree};
    use futures::{StreamExt, pin_mut};
    let tree = InMemoryBTree::<String, Rc<Vec<u8>>>::new();
    let mut transaction = tree.transaction().await.expect("local transaction");
    transaction
        .insert("key".into(), Rc::new(vec![1]))
        .await
        .expect("local value");
    let replacement = Rc::new(vec![2]);
    transaction
        .update("key".into(), move |value| {
            *value = replacement;
            Ok(())
        })
        .await
        .expect("local callback");
    {
        let entries = transaction.range(..);
        pin_mut!(entries);
        let (key, value) = entries
            .next()
            .await
            .expect("one entry")
            .expect("local stream");
        assert_eq!(key, "key");
        assert_eq!(value.as_slice(), &[2]);
    }
    transaction.commit().await.expect("local commit");
    assert_eq!(
        tree.get(&"key".into())
            .await
            .expect("committed value")
            .expect("present")
            .as_slice(),
        &[2]
    );
}

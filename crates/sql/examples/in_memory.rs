use ofdb_sql::{Database, SqlTranslator};

fn main() {
    futures::executor::block_on(async {
        let database = Database::in_memory();
        database
            .client()
            .translate_and_execute(
                "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                &SqlTranslator,
            )
            .await
            .expect("create users");
    });
}

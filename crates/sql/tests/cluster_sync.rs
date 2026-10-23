mod common;

use common::{case_concurrent_user_inserts, standard_suite};
use ofdb_sql::SessionConfig;
use ofdb_test::{
    ClusterOfflineRunner, ClusterRealtimeRunner, TransportDirection, automerge_in_memory_cluster,
    automerge_redb_cluster, run, test_case, test_suite,
};

test_suite!(
    standard_offline,
    standard_suite(),
    ClusterOfflineRunner::default()
);
test_suite!(
    standard_realtime,
    standard_suite(),
    ClusterRealtimeRunner::default()
);
test_case!(
    concurrent_inserts_offline,
    case_concurrent_user_inserts(),
    ClusterOfflineRunner::default()
);
test_case!(
    concurrent_inserts_realtime,
    case_concurrent_user_inserts(),
    ClusterRealtimeRunner::default()
);

const CREATE_USERS: &str = "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)";
const SELECT_USERS: &str = "SELECT * FROM users";
const INSERT_ADA: &str =
    "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')";
const INSERT_LIN: &str =
    "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')";

#[test]
fn bootstrap_and_noop_repair_converge() {
    run(async {
        let cluster = automerge_in_memory_cluster(2);
        let config = SessionConfig::default();
        cluster.exec(0, CREATE_USERS).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.assert_converged(SELECT_USERS).await;
        cluster.assert_state_converged().await;
    });
}

#[test]
fn offline_writes_and_ordered_three_replica_repair_converge() {
    run(async {
        let cluster = automerge_in_memory_cluster(3);
        let config = SessionConfig::default();
        cluster.exec(0, CREATE_USERS).await;
        cluster
            .sync_pairs(&[(0, 1), (1, 2)], &config)
            .await
            .unwrap();
        cluster.exec(0, INSERT_ADA).await;
        cluster.exec(1, INSERT_LIN).await;
        cluster.exec(2, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Mia')").await;
        cluster
            .sync_pairs(&[(2, 0), (1, 2), (0, 1), (2, 1)], &config)
            .await
            .unwrap();
        cluster.assert_converged(SELECT_USERS).await;
        cluster.assert_state_converged().await;
    });
}

#[test]
fn persistent_direct_replicas_sync() {
    run(async {
        let (_cleanup, cluster) = automerge_redb_cluster(2);
        cluster.exec(0, CREATE_USERS).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster.sync(0, 1, &SessionConfig::default()).await.unwrap();
        cluster.assert_converged(SELECT_USERS).await;
    });
}

#[test]
fn relay_survives_an_unavailable_source() {
    run(async {
        let cluster = automerge_in_memory_cluster(3);
        let config = SessionConfig::default();
        cluster.exec(0, CREATE_USERS).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.sync(1, 2, &config).await.unwrap();
        cluster.assert_state_converged().await;
    });
}

#[test]
fn every_directional_frame_failure_retries() {
    run(async {
        for (direction, frames) in [
            (TransportDirection::LeftToRight, 0..4),
            (TransportDirection::RightToLeft, 0..3),
        ] {
            for frame in frames {
                let cluster = automerge_in_memory_cluster(2);
                let config = SessionConfig::default();
                cluster.exec(0, CREATE_USERS).await;
                cluster.exec(0, INSERT_ADA).await;
                assert!(
                    cluster
                        .sync_interrupted_in(0, 1, &config, direction, frame)
                        .await
                        .is_err()
                );
                cluster.sync(0, 1, &config).await.unwrap();
                cluster.assert_state_converged().await;
            }
        }
    });
}

#[test]
fn replicated_table_index_storage_collisions_are_rejected_without_data_loss() {
    run(async {
        let cluster = automerge_in_memory_cluster(2);
        cluster
            .exec(0, "CREATE TABLE occupied (id UUID PRIMARY KEY, name TEXT)")
            .await;
        cluster.exec(0, "INSERT INTO occupied VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'keep')").await;
        cluster.exec(1, CREATE_USERS).await;
        cluster
            .exec(1, "CREATE UNIQUE INDEX occupied ON users (name)")
            .await;
        cluster.exec(1, INSERT_ADA).await;
        let error = cluster
            .sync(0, 1, &SessionConfig::default())
            .await
            .expect_err("replication must reject a shared table/index storage name");
        assert!(
            error.to_string().contains("reserved"),
            "namespace collision must be rejected before reading or clearing overlapping storage: {error}"
        );
        assert_eq!(
            cluster.exec(0, "SELECT name FROM occupied").await,
            vec![ofdb_sql::Row::from(["keep"])]
        );
        assert_eq!(
            cluster.exec(1, "SELECT name FROM users").await,
            vec![ofdb_sql::Row::from(["Ada"])]
        );
        assert!(
            cluster
                .try_exec(1, INSERT_LIN.replace("Lin", "Ada").as_str())
                .await
                .is_err(),
            "rejected sync preserves the unique index"
        );
    });
}

#[test]
fn rejected_catalog_writes_preserve_composite_unique_rows_across_sync() {
    run(async {
        let cluster = automerge_in_memory_cluster(2);
        let config = SessionConfig::default();
        cluster
            .exec(
                0,
                "CREATE TABLE protected (id UUID PRIMARY KEY, tenant TEXT, code INTEGER)",
            )
            .await;
        cluster
            .exec(
                0,
                "CREATE UNIQUE INDEX protected_key ON protected (tenant, code)",
            )
            .await;
        cluster.exec(0, "INSERT INTO protected VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', 7)").await;
        for node in [0, 1] {
            if node == 1 {
                cluster
                    .sync(0, 1, &config)
                    .await
                    .expect("sync internal catalog writes");
            }
            for catalog in ["tables", "table_fields", "indices", "index_fields"] {
                let error = cluster
                    .try_exec(node, &format!("DELETE FROM {catalog}"))
                    .await
                    .expect_err("catalog deletion is forbidden");
                assert!(matches!(
                    error,
                    ofdb_sql::EngineError::InvalidQuery("Catalog tables are read-only")
                ));
                assert!(
                    !cluster
                        .exec(node, &format!("SELECT * FROM {catalog}"))
                        .await
                        .is_empty()
                );
            }
            assert_eq!(
                cluster
                    .exec(
                        node,
                        "SELECT tenant, code FROM protected WHERE tenant = 'Ada' AND code = 7"
                    )
                    .await,
                vec![ofdb_sql::Row::new(vec![
                    ofdb_sql::Value::from("Ada"),
                    ofdb_sql::Value::Integer(7)
                ])],
            );
            assert!(cluster.try_exec(node, "INSERT INTO protected VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Ada', 7)").await.is_err(), "composite uniqueness survives rejected mutations and sync");
        }
        cluster
            .sync(1, 0, &config)
            .await
            .expect("repair after rejected writes");
        cluster.assert_converged("SELECT * FROM protected").await;
        cluster.assert_state_converged().await;
    });
}

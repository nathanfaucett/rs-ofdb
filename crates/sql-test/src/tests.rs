use futures::executor::block_on;

use crate::{
    ChaosRunner, ChaosScenario, ChaosStep, ClusterOfflineRunner, ClusterRealtimeRunner,
    ExpectedError, RunnerError, SingleNodeRunner, TestCase, TestRunner, VerificationPolicy,
    automerge_in_memory_cluster, run_chaos,
};

fn case() -> TestCase {
    TestCase::builder("runner_coverage")
        .setup([
                    "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                    "CREATE INDEX users_name ON users (name)",
                    "DROP INDEX users_name",
                ])
        .step_root("INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada') RETURNING *")
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Duplicate')",
            ExpectedError::ConstraintViolation,
        )
        .expect_query("SELECT name FROM users", vec![crate::Row::from(["Ada"])])
        .expect_node_query(0, "SELECT name FROM users", vec![crate::Row::from(["Ada"])])
                .expect_query(
                    "SELECT name FROM users WHERE name = 'Ada' AND name IS NOT NULL",
                    vec![crate::Row::from(["Ada"])],
                )
                .expect_query(
                    "SELECT name FROM users WHERE name <> 'Bob' OR name IS NULL",
                    vec![crate::Row::from(["Ada"])],
                )
                .expect_query("SELECT name FROM users WHERE name < 'Bob'", vec![crate::Row::from(["Ada"])])
                .expect_query("SELECT name FROM users WHERE name <= 'Ada'", vec![crate::Row::from(["Ada"])])
                .expect_query("SELECT name FROM users WHERE name > 'Bob'", vec![])
                .expect_query("SELECT name FROM users WHERE name >= 'Ada'", vec![crate::Row::from(["Ada"])])
        .build()
}

#[test]
fn runners_execute_and_verify_cases() {
    block_on(async {
        let case = case();
        let empty = TestCase::builder("empty_case").build();
        SingleNodeRunner::new().run_case(&empty).await.unwrap();
        SingleNodeRunner::new().run_case(&case).await.unwrap();
        ClusterRealtimeRunner::with_policy(VerificationPolicy::strict())
            .run_case(&empty)
            .await
            .unwrap();
        ClusterRealtimeRunner::with_policy(VerificationPolicy::strict())
            .run_case(&case)
            .await
            .unwrap();
        ClusterOfflineRunner::with_policy(VerificationPolicy::strict())
            .run_case(&empty)
            .await
            .unwrap();
        ClusterOfflineRunner::with_policy(VerificationPolicy::strict())
            .run_case(&case)
            .await
            .unwrap();
        ChaosRunner::new()
            .with_policy(VerificationPolicy::strict())
            .with_drop_rate(0.0)
            .run_case(&empty)
            .await
            .unwrap();
        ChaosRunner::new()
            .with_policy(VerificationPolicy::strict())
            .with_drop_rate(0.0)
            .run_case(&case)
            .await
            .unwrap();
    });
}

#[test]
fn runners_report_unexpected_errors_and_missing_expected_errors() {
    block_on(async {
        let unexpected = TestCase::builder("unexpected_error")
            .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
            .step_root("SELECT missing FROM users")
            .build();
        let expected_missing = TestCase::builder("expected_error_missing")
            .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
            .step_failing(0, "SELECT * FROM users", ExpectedError::SyntaxError)
            .build();

        assert!(matches!(
            SingleNodeRunner::new().run_case(&unexpected).await,
            Err(RunnerError::UnexpectedError { .. })
        ));
        assert!(matches!(
            SingleNodeRunner::new().run_case(&expected_missing).await,
            Err(RunnerError::ExpectedErrorDidNotOccur { .. })
        ));
        assert!(matches!(
            ClusterRealtimeRunner::new().run_case(&unexpected).await,
            Err(RunnerError::UnexpectedError { .. })
        ));
        assert!(matches!(
            ClusterRealtimeRunner::new()
                .run_case(&expected_missing)
                .await,
            Err(RunnerError::ExpectedErrorDidNotOccur { .. })
        ));
        assert!(matches!(
            ClusterOfflineRunner::new().run_case(&unexpected).await,
            Err(RunnerError::UnexpectedError { .. })
        ));
        assert!(matches!(
            ClusterOfflineRunner::new()
                .run_case(&expected_missing)
                .await,
            Err(RunnerError::ExpectedErrorDidNotOccur { .. })
        ));
        assert!(matches!(
            ChaosRunner::new().run_case(&unexpected).await,
            Err(RunnerError::UnexpectedError { .. })
        ));
        assert!(matches!(
            ChaosRunner::new().run_case(&expected_missing).await,
            Err(RunnerError::ExpectedErrorDidNotOccur { .. })
        ));
    });
}

#[test]
fn chaos_scenario_executes_and_converges() {
    block_on(async {
        let scenario = ChaosScenario {
            name: "coverage",
            nodes: 2,
            seed: 1,
            steps: vec![
                ChaosStep::Exec {
                    node: 0,
                    sql: "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                },
                ChaosStep::Sync,
                ChaosStep::Partition {
                    groups: &[&[0], &[1]],
                },
                ChaosStep::Heal,
                ChaosStep::DropRate(0.0),
                ChaosStep::EventuallyConsistent {
                    query: "SELECT * FROM users",
                    max_rounds: 2,
                },
            ],
        };
        run_chaos(
            automerge_in_memory_cluster(2),
            scenario,
            &sync::SessionConfig::default(),
        )
        .await;
    });
}

#[test]
fn expected_errors_match_their_categories() {
    use engine::EngineError;

    let cases = [
        (
            ExpectedError::SyntaxError,
            EngineError::Unsupported("syntax"),
        ),
        (
            ExpectedError::ConstraintViolation,
            EngineError::custom("duplicate key"),
        ),
        (
            ExpectedError::TableNotFound,
            EngineError::custom("table not found"),
        ),
        (
            ExpectedError::ColumnNotFound,
            EngineError::custom("column not found"),
        ),
        (
            ExpectedError::TypeMismatch,
            EngineError::custom("type mismatch"),
        ),
    ];
    for (expected, error) in cases {
        assert!(expected.matches(&error));
    }
    assert!(!ExpectedError::TypeMismatch.matches(&EngineError::custom("unrelated")));
}

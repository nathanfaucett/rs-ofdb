use sync::SessionConfig;

use super::verify::verify_case;

use crate::{
    case::TestCase,
    cluster::automerge_redb_cluster,
    runner::{RunnerError, TestRunner, VerificationPolicy, verify_step_result},
};

/// Orchestrates multi-node cluster integration tests in offline partition mode:
/// nodes mutate independently while offline, then sync to eventual consistency.
#[derive(Clone, Debug)]
pub struct ClusterOfflineRunner {
    pub policy: VerificationPolicy,
    pub session_config: SessionConfig,
    pub max_sync_rounds: usize,
}

impl Default for ClusterOfflineRunner {
    fn default() -> Self {
        Self {
            policy: VerificationPolicy::new(true, true, false),
            session_config: SessionConfig::default(),
            max_sync_rounds: 10,
        }
    }
}

impl ClusterOfflineRunner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_policy(policy: VerificationPolicy) -> Self {
        Self {
            policy,
            ..Default::default()
        }
    }

    pub fn with_session_config(mut self, config: SessionConfig) -> Self {
        self.session_config = config;
        self
    }
}

impl TestRunner for ClusterOfflineRunner {
    type Error = RunnerError;

    async fn run_case(&self, case: &TestCase) -> Result<(), Self::Error> {
        let n = case.required_nodes().max(2);
        let (cleanup, cluster) = automerge_redb_cluster(n);

        // Setup on node 0
        for stmt in &case.setup {
            cluster
                .try_exec(0, stmt.as_ref())
                .await
                .map_err(RunnerError::Engine)?;
        }
        if !case.setup.is_empty() {
            cluster
                .sync_all(&self.session_config)
                .await
                .map_err(|e| RunnerError::Sync(e.to_string()))?;
        }

        // Offline steps: nodes mutate independently without synchronization
        for step in &case.steps {
            let result = cluster.try_exec(step.node.0, step.sql.as_ref()).await;
            match (&step.expected_error, result) {
                (None, Ok(rows)) => verify_step_result(step, rows)?,
                (None, Err(err)) => {
                    return Err(RunnerError::UnexpectedError {
                        node: step.node,
                        expected: None,
                        actual: err.to_string(),
                    });
                }
                (Some(expected), Ok(_)) => {
                    return Err(RunnerError::ExpectedErrorDidNotOccur {
                        node: step.node,
                        expected: *expected,
                    });
                }
                (Some(expected), Err(err)) => {
                    if !expected.matches(&err) {
                        return Err(RunnerError::UnexpectedError {
                            node: step.node,
                            expected: Some(*expected),
                            actual: err.to_string(),
                        });
                    }
                }
            }
        }

        // Reconnect and sync until converged
        cluster
            .sync_until_converged(&self.session_config, self.max_sync_rounds)
            .await
            .map_err(|e| RunnerError::Sync(e.to_string()))?;

        verify_case(&cluster, case, n, self.policy).await?;

        drop(cluster);
        drop(cleanup);
        Ok(())
    }
}

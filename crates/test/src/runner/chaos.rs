use sync::SessionConfig;

use crate::{
    case::TestCase,
    chaos::ChaosNetwork,
    cluster::automerge_redb_cluster,
    runner::{RunnerError, TestRunner, VerificationPolicy, verify::verify_case},
};

/// Executes test cases under simulated network chaos:
/// frame drop rates, intermittent partition splits, and partial transfers.
/// Heals network partitions before final convergence validation.
#[derive(Clone, Debug)]
pub struct ChaosRunner {
    pub policy: VerificationPolicy,
    pub session_config: SessionConfig,
    pub drop_rate: f64,
    pub max_rounds: usize,
    pub seed: u64,
}

impl Default for ChaosRunner {
    fn default() -> Self {
        Self {
            // state_consistency validates full sync manifests
            policy: VerificationPolicy::new(true, true, false),
            session_config: SessionConfig::default(),
            drop_rate: 0.3,
            max_rounds: 32,
            seed: 42,
        }
    }
}

impl ChaosRunner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_policy(mut self, policy: VerificationPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_drop_rate(mut self, drop_rate: f64) -> Self {
        self.drop_rate = drop_rate;
        self
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn with_max_rounds(mut self, max_rounds: usize) -> Self {
        self.max_rounds = max_rounds;
        self
    }
}

impl TestRunner for ChaosRunner {
    type Error = RunnerError;

    async fn run_case(&self, case: &TestCase) -> Result<(), Self::Error> {
        let n = case.required_nodes().max(2);
        let (cleanup, cluster) = automerge_redb_cluster(n);

        // Setup on node 0 with healthy network
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

        // Initialize chaos network
        let mut network = ChaosNetwork::new(self.seed);
        network.set_drop_rate(self.drop_rate);

        // Execute steps under intermittent chaos
        for step in &case.steps {
            let result = cluster.try_exec(step.node.0, step.sql.as_ref()).await;
            match (&step.expected_error, result) {
                (None, Ok(_)) => {
                    // Attempt intermittent communication across non-partitioned pairs
                    for left in 0..n {
                        for right in left + 1..n {
                            if !network.can_communicate(left, right) {
                                continue;
                            }
                            let _ = if network.should_interrupt() {
                                cluster
                                    .sync_interrupted(left, right, &self.session_config, 0)
                                    .await
                            } else {
                                cluster.sync(left, right, &self.session_config).await
                            };
                        }
                    }
                }
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

        // Heal network
        network.heal();
        network.set_drop_rate(0.0);

        // Synchronize cluster until eventual consistency is verified
        let mut converged = false;
        for _ in 0..self.max_rounds {
            let _ = cluster.sync_all(&self.session_config).await;
            if cluster.have_same_manifests().await {
                converged = true;
                break;
            }
        }

        if !converged {
            return Err(RunnerError::Chaos(format!(
                "cluster did not reach uniform sync manifest within {} rounds",
                self.max_rounds
            )));
        }

        verify_case(&cluster, case, n, self.policy).await?;

        drop(cluster);
        drop(cleanup);
        Ok(())
    }
}

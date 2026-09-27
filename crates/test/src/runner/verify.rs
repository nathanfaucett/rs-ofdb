use engine::Kernel;
use sync::SyncRowCodec;

use crate::{
    case::{NodeId, TestCase},
    cluster::Cluster,
    runner::{RunnerError, VerificationPolicy},
};

pub(super) async fn verify_case<K, R>(
    cluster: &Cluster<K, R>,
    case: &TestCase,
    nodes: usize,
    policy: VerificationPolicy,
) -> Result<(), RunnerError>
where
    K: Kernel,
    R: SyncRowCodec<K::Transaction>,
{
    if policy.convergence {
        for expectation in &case.expectations {
            if expectation.target_node.is_none() {
                let first_rows = cluster
                    .try_exec(0, expectation.query.as_ref())
                    .await
                    .map_err(RunnerError::Engine)?;
                for node in 1..nodes {
                    let rows = cluster
                        .try_exec(node, expectation.query.as_ref())
                        .await
                        .map_err(RunnerError::Engine)?;
                    if rows != first_rows {
                        return Err(RunnerError::ConvergenceFailure {
                            query: expectation.query.clone(),
                            diverged_node: node,
                            rows_node_0: first_rows,
                            rows_other: rows,
                        });
                    }
                }
            }
        }
    }

    if policy.state_consistency {
        cluster.assert_state_converged().await;
    }

    if policy.io_correctness {
        for expectation in &case.expectations {
            let targets: Vec<usize> = match expectation.target_node {
                Some(node) => vec![node.0],
                None => (0..nodes).collect(),
            };
            for node in targets {
                let actual = cluster
                    .try_exec(node, expectation.query.as_ref())
                    .await
                    .map_err(RunnerError::Engine)?;
                if actual != expectation.expected_rows {
                    return Err(RunnerError::ExpectationFailed {
                        node: Some(NodeId(node)),
                        query: expectation.query.clone(),
                        expected: expectation.expected_rows.clone(),
                        actual,
                    });
                }
            }
        }
    }
    Ok(())
}

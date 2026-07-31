//! The update_status step in the HiveCluster controller.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    OPERATOR_NAME,
    controller::{Applied, KubernetesResources},
    crd::{HiveClusterStatus, v1alpha1},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Computes the cluster status from the applied resources and patches it onto the
/// [`v1alpha1::HiveCluster`]. Takes [`KubernetesResources<Applied>`] so the type system proves
/// the status derives from applied resources, not merely built ones. `discovery_hash` is
/// derived from the applied discovery `ConfigMap`'s resource version in the reconcile step.
pub async fn update_status(
    client: &Client,
    hive: &v1alpha1::HiveCluster,
    applied: &KubernetesResources<Applied>,
    discovery_hash: u64,
) -> Result<()> {
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    for stateful_set in &applied.stateful_sets {
        ss_cond_builder.add(stateful_set.clone());
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hive.spec.cluster_operation);

    let status = HiveClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.to_string()),
        conditions: compute_conditions(hive, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    client
        .apply_patch_status(OPERATOR_NAME, hive, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(())
}

use snafu::{ResultExt, Snafu};
use stackable_operator::status::condition::{
    compute_conditions, operations::ClusterOperationsConditionBuilder,
    statefulset::StatefulSetConditionBuilder,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::{Applied, KubernetesResources};
use crate::{
    OPERATOR_NAME,
    crd::{HiveClusterStatus, v1alpha1},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },
}

pub async fn update_status(
    client: &stackable_operator::client::Client,
    hive: &v1alpha1::HiveCluster,
    applied: KubernetesResources<Applied>,
) -> Result<(), Error> {
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    for ss in &applied.stateful_sets {
        ss_cond_builder.add(ss.clone());
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hive.spec.cluster_operation);

    let status = HiveClusterStatus {
        discovery_hash: Some(applied.discovery_hash.as_deref().unwrap_or("").to_string()),
        conditions: compute_conditions(hive, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    client
        .apply_patch_status(OPERATOR_NAME, hive, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(())
}

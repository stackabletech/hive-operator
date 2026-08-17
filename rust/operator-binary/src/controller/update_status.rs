//! The update_status step in the HiveCluster controller.

use std::hash::Hasher;

use fnv::FnvHasher;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    k8s_openapi::api::core::v1::ConfigMap,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::{controller_utils::get_cluster_name, types::operator::ClusterName},
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    HIVE_OPERATOR_NAME,
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

    #[snafu(display("failed to determine the cluster's name"))]
    ResolveClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Computes the cluster status from the applied resources and patches it onto the
/// [`v1alpha1::HiveCluster`]. Takes [`KubernetesResources<Applied>`] so the type system proves
/// the status derives from applied resources, not merely built ones.
pub async fn update_status(
    client: &Client,
    hive: &v1alpha1::HiveCluster,
    applied: &KubernetesResources<Applied>,
) -> Result<()> {
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    for stateful_set in &applied.stateful_sets {
        ss_cond_builder.add(stateful_set.clone());
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hive.spec.cluster_operation);

    let cluster_name = get_cluster_name(hive).context(ResolveClusterNameSnafu)?;

    let status = HiveClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash(&applied.config_maps, &cluster_name).to_string()),
        conditions: compute_conditions(hive, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    client
        .apply_patch_status(HIVE_OPERATOR_NAME, hive, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(())
}

/// The hash of the applied discovery `ConfigMap`'s (named after the cluster) resource version,
/// exposed in the status so that dependent clusters can restart on discovery changes. While no
/// discovery ConfigMap has been applied (the role Listener has no ingress address yet), nothing
/// is hashed and the hasher's initial state is returned.
fn discovery_hash(config_maps: &[ConfigMap], cluster_name: &ClusterName) -> u64 {
    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut hasher = FnvHasher::with_key(0);
    if let Some(resource_version) = config_maps
        .iter()
        .find(|config_map| config_map.metadata.name.as_deref() == Some(cluster_name.as_ref()))
        .and_then(|config_map| config_map.metadata.resource_version.as_ref())
    {
        hasher.write(resource_version.as_bytes());
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::{
        k8s_openapi::api::core::v1::ConfigMap, kube::api::ObjectMeta,
        v2::types::operator::ClusterName,
    };

    use super::discovery_hash;

    fn config_map(name: &str, resource_version: &str) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                resource_version: Some(resource_version.to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        }
    }

    /// The hash must react to the discovery ConfigMap (named after the cluster) and ignore the
    /// role-group ConfigMaps.
    #[test]
    fn discovery_hash_tracks_only_the_discovery_config_map() {
        let cluster_name = ClusterName::from_str("simple-hive").expect("valid cluster name");
        let role_group_cm = config_map("simple-hive-metastore-default", "1");

        let without_discovery_cm =
            discovery_hash(std::slice::from_ref(&role_group_cm), &cluster_name);
        let with_discovery_cm = discovery_hash(
            &[role_group_cm, config_map("simple-hive", "42")],
            &cluster_name,
        );
        assert_ne!(without_discovery_cm, with_discovery_cm);

        // A changed resource version changes the hash.
        let with_changed_discovery_cm =
            discovery_hash(&[config_map("simple-hive", "43")], &cluster_name);
        assert_ne!(with_discovery_cm, with_changed_discovery_cm);

        // An absent discovery ConfigMap hashes to the hasher's initial state, matching the
        // behaviour before it was skipped (no bytes were written either).
        assert_eq!(without_discovery_cm, discovery_hash(&[], &cluster_name));
    }
}

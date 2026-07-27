//! Builders that turn a `ValidatedCluster` into Kubernetes resources.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::operator::{ProductVersion, RoleGroupName},
};

use crate::{
    controller::{
        KubernetesResources, ValidatedCluster,
        build::resource::{
            config_map::build_metastore_rolegroup_config_map,
            listener::build_role_listener,
            pdb::build_pdb,
            rbac::{build_role_binding, build_service_account},
            service::{build_rolegroup_headless_service, build_rolegroup_metrics_service},
            statefulset::build_metastore_rolegroup_statefulset,
        },
    },
    crd::HiveRole,
};

// Placeholder role-group name used for the recommended labels of the role-level discovery
// `ConfigMap` (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

// Placeholder role-group name used for the recommended labels of the role-level `Listener`
// (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_LISTENER_ROLE_GROUP: RoleGroupName = "none");

// Placeholder product version used for labels on PVC templates, which cannot be modified once
// deployed. A constant value keeps the labels stable across version upgrades.
stackable_operator::constant!(pub(crate) UNVERSIONED_PRODUCT_VERSION: ProductVersion = "none");

pub mod command;
pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod opa;
pub mod properties;
pub mod resource;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    ConfigMap {
        source: resource::config_map::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role group {role_group}"))]
    StatefulSet {
        source: resource::statefulset::Error,
        role_group: RoleGroupName,
    },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every reference to another Kubernetes resource is already
/// dereferenced and validated by this point. Cluster configuration is likewise already validated,
/// so the errors returned here are resource-assembly failures only.
///
/// The role-level discovery `ConfigMap` is *not* built here: it depends on the *applied* role
/// [`Listener`](stackable_operator::crd::listener::v1alpha1::Listener)'s ingress addresses and is
/// therefore assembled in the reconcile step after the Listener has been applied.
///
/// `cluster_info` carries the Kubernetes cluster domain (needed by the Kerberos config); it is
/// static cluster metadata, not a live client, so the build step stays client-free.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KubernetesResources, Error> {
    let mut stateful_sets = vec![];
    let mut services = vec![];
    let mut listeners = vec![];
    let mut config_maps = vec![];
    let mut pod_disruption_budgets = vec![];

    // Role-level resources. Hive has the single `metastore` role; its PDB and Listener are built
    // here, but the discovery ConfigMap (which needs the applied Listener) is built in reconcile.
    if let Some(role_config) = &cluster.role_config {
        pod_disruption_budgets.extend(build_pdb(&role_config.pdb, cluster, &HiveRole::MetaStore));
        listeners.push(build_role_listener(
            cluster,
            &HiveRole::MetaStore,
            &role_config.listener_class,
        ));
    }

    for (hive_role, role_group_configs) in &cluster.role_group_configs {
        for (role_group_name, rg) in role_group_configs {
            services.push(build_rolegroup_headless_service(cluster, role_group_name));
            services.push(build_rolegroup_metrics_service(cluster, role_group_name));
            config_maps.push(
                build_metastore_rolegroup_config_map(cluster, cluster_info, role_group_name, rg)
                    .context(ConfigMapSnafu {
                        role_group: role_group_name.clone(),
                    })?,
            );
            stateful_sets.push(
                build_metastore_rolegroup_statefulset(hive_role, cluster, role_group_name, rg)
                    .context(StatefulSetSnafu {
                        role_group: role_group_name.clone(),
                    })?,
            );
        }
    }

    Ok(KubernetesResources {
        stateful_sets,
        services,
        listeners,
        config_maps,
        pod_disruption_budgets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::{
        commons::networking::DomainName, kube::Resource, utils::cluster_info::KubernetesClusterInfo,
    };

    use super::build;
    use crate::controller::test_support::{DERBY_YAML, minimal_hive, validated_cluster};

    fn test_cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::from_str("cluster.local").expect("valid cluster domain"),
        }
    }

    fn sorted_names(resources: &[impl Resource]) -> Vec<String> {
        let mut names: Vec<String> = resources
            .iter()
            .filter_map(|resource| resource.meta().name.clone())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn build_produces_expected_resource_names() {
        let hive = minimal_hive(DERBY_YAML);
        let cluster = validated_cluster(&hive);

        let resources = build(&cluster, &test_cluster_info()).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.stateful_sets),
            ["simple-hive-metastore-default"]
        );
        // One headless and one metrics Service per role group.
        assert_eq!(
            sorted_names(&resources.services),
            [
                "simple-hive-metastore-default-headless",
                "simple-hive-metastore-default-metrics",
            ]
        );
        assert_eq!(
            sorted_names(&resources.config_maps),
            ["simple-hive-metastore-default"]
        );
        // The single metastore role has one role Listener.
        assert_eq!(
            sorted_names(&resources.listeners),
            ["simple-hive-metastore"]
        );
        // A default PDB for the metastore role.
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["simple-hive-metastore"]
        );
        // The cluster-shared RBAC pair.
        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["simple-hive-serviceaccount"]
        );
        assert_eq!(
            sorted_names(&resources.role_bindings),
            ["simple-hive-rolebinding"]
        );
    }
}

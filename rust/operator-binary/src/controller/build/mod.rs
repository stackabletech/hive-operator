//! Builders that turn a `ValidatedCluster` into Kubernetes resources.

use std::{marker::PhantomData, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::meta::ownerreference_from_resource,
        types::operator::{ProductVersion, RoleGroupName},
    },
};

use crate::{
    controller::{
        KubernetesResources, Prepared, ValidatedCluster,
        build::resource::{
            config_map::build_metastore_rolegroup_config_map,
            discovery::build_discovery_configmap,
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

    #[snafu(display("failed to build the discovery ConfigMap"))]
    DiscoveryConfigMap { source: resource::discovery::Error },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every reference to another Kubernetes resource is already
/// dereferenced and validated by this point. Cluster configuration is likewise already validated,
/// so the errors returned here are resource-assembly failures only.
///
/// `cluster_info` carries the Kubernetes cluster domain (needed by the Kerberos config); it is
/// static cluster metadata, not a live client, so the build step stays client-free.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KubernetesResources<Prepared>, Error> {
    let mut stateful_sets = vec![];
    let mut services = vec![];
    let mut listeners = vec![];
    let mut config_maps = vec![];
    let mut pod_disruption_budgets = vec![];

    // Role-level resources. Hive has the single `metastore` role; its PDB and Listener are
    // built here. The discovery ConfigMap is built below, from the dereferenced Listener.
    let role_config = &cluster.role_config;
    pod_disruption_budgets.extend(build_pdb(&role_config.pdb, cluster, &HiveRole::MetaStore));
    listeners.push(build_role_listener(
        cluster,
        &HiveRole::MetaStore,
        &role_config.listener_class,
    ));

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

    if let Some(discovery_config_map) =
        build_discovery_configmap(cluster, HiveRole::MetaStore).context(DiscoveryConfigMapSnafu)?
    {
        config_maps.push(discovery_config_map);
    }

    Ok(KubernetesResources {
        stateful_sets,
        services,
        listeners,
        config_maps,
        pod_disruption_budgets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
        status: PhantomData,
    })
}

/// Returns an [`ObjectMetaBuilder`] pre-filled with the namespace, an owner reference back to
/// the cluster, and the recommended labels for a resource named `name` in `role_group_name`.
///
/// Consolidates the metadata chain repeated by the child-resource builders. Call sites that
/// need extra labels/annotations chain them onto the returned builder.
pub(crate) fn object_meta(
    cluster: &ValidatedCluster,
    name: impl Into<String>,
    role_group_name: &RoleGroupName,
) -> ObjectMetaBuilder {
    let mut builder = ObjectMetaBuilder::new();
    builder
        .name_and_namespace(cluster)
        .name(name)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(cluster.recommended_labels(role_group_name));
    builder
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use stackable_operator::{
        commons::networking::DomainName,
        crd::listener::{self, v1alpha1::Listener},
        k8s_openapi::api::core::v1::ConfigMap,
        kube::{Resource, api::ObjectMeta},
        utils::cluster_info::KubernetesClusterInfo,
    };

    use super::{KubernetesResources, Prepared, RoleGroupName, build, object_meta};
    use crate::{
        controller::test_support::{DERBY_YAML, minimal_hive, validated_cluster},
        crd::HIVE_PORT_NAME,
    };

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

    /// A metastore role Listener whose status carries an ingress address, as the
    /// listener-operator eventually writes it.
    fn role_listener_with_address() -> Listener {
        Listener {
            metadata: ObjectMeta::default(),
            spec: listener::v1alpha1::ListenerSpec::default(),
            status: Some(listener::v1alpha1::ListenerStatus {
                service_name: None,
                ingress_addresses: Some(vec![listener::v1alpha1::ListenerIngress {
                    address: "hive.example.com".to_string(),
                    address_type: listener::v1alpha1::AddressType::Hostname,
                    ports: BTreeMap::from([(HIVE_PORT_NAME.to_string(), 9083)]),
                }]),
                node_ports: None,
            }),
        }
    }

    /// The built discovery ConfigMap (named after the cluster itself), if any.
    fn discovery_config_map(resources: &KubernetesResources<Prepared>) -> Option<&ConfigMap> {
        resources
            .config_maps
            .iter()
            .find(|config_map| config_map.metadata.name.as_deref() == Some("simple-hive"))
    }

    #[test]
    fn builds_discovery_config_map_with_listener_address() {
        let hive = minimal_hive(DERBY_YAML);
        let mut cluster = validated_cluster(&hive);
        cluster.role_listener = Some(role_listener_with_address());

        let resources = build(&cluster, &test_cluster_info()).expect("build succeeds");

        let data = discovery_config_map(&resources)
            .expect("the discovery ConfigMap is built")
            .data
            .as_ref()
            .expect("the discovery ConfigMap carries data");
        assert_eq!(
            data.get("HIVE").map(String::as_str),
            Some("thrift://hive.example.com:9083")
        );
    }

    /// The discovery ConfigMap as previously stored in the cluster, as the dereference step
    /// fetches it: carrying data from an earlier reconcile run and server-populated metadata
    /// (`resourceVersion`, `uid`) that must not be echoed back into an apply patch.
    fn stored_discovery_config_map() -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some("simple-hive".to_string()),
                namespace: Some("default".to_string()),
                resource_version: Some("12345".to_string()),
                uid: Some("87654321-4321-4321-4321-210987654321".to_string()),
                ..ObjectMeta::default()
            },
            data: Some(BTreeMap::from([(
                "HIVE".to_string(),
                "thrift://old-address.example.com:9083".to_string(),
            )])),
            ..ConfigMap::default()
        }
    }

    /// While the role Listener yields no ingress address but a discovery ConfigMap was already
    /// stored by an earlier reconcile run, that ConfigMap is re-emitted unchanged instead of
    /// being skipped -- a skip would let `delete_orphaned_resources` delete the stored one,
    /// breaking Pods that mount it.
    #[test]
    fn reemits_stored_discovery_config_map_without_listener_address() {
        let hive = minimal_hive(DERBY_YAML);
        let mut cluster = validated_cluster(&hive);
        cluster.role_listener = None;
        cluster.existing_discovery_config_map = Some(stored_discovery_config_map());

        let resources = build(&cluster, &test_cluster_info()).expect("build succeeds");

        let config_map =
            discovery_config_map(&resources).expect("the stored discovery ConfigMap is re-emitted");
        assert_eq!(
            config_map.data,
            stored_discovery_config_map().data,
            "the stored data must be carried over unchanged"
        );
        // The metadata must be rebuilt fresh: server-populated fields of the fetched object
        // must not go into a server-side apply patch.
        assert_eq!(config_map.metadata.resource_version, None);
        assert_eq!(config_map.metadata.uid, None);
        assert!(config_map.metadata.owner_references.is_some());
        assert!(config_map.metadata.labels.is_some());
    }

    /// While the Listener is absent (the apply step has not created it yet) or carries no
    /// ingress address (the listener-operator has not reconciled it yet), and no discovery
    /// ConfigMap has ever been stored (initial deploy), the ConfigMap is skipped *without*
    /// failing the build: the Listener watch triggers a new reconcile run once the address
    /// is set.
    #[test]
    fn skips_discovery_config_map_without_listener_address() {
        let hive = minimal_hive(DERBY_YAML);
        let mut cluster = validated_cluster(&hive);

        let no_listener = None;
        let no_status = Some(Listener {
            status: None,
            ..role_listener_with_address()
        });
        let no_addresses = Some(Listener {
            status: Some(listener::v1alpha1::ListenerStatus {
                service_name: None,
                ingress_addresses: Some(vec![]),
                node_ports: None,
            }),
            ..role_listener_with_address()
        });
        for role_listener in [no_listener, no_status, no_addresses] {
            cluster.role_listener = role_listener;

            let resources = build(&cluster, &test_cluster_info())
                .expect("build succeeds without a listener address");

            assert!(discovery_config_map(&resources).is_none());
        }
    }

    #[test]
    fn object_meta_sets_namespace_owner_and_recommended_labels() {
        let hive = minimal_hive(DERBY_YAML);
        let cluster = validated_cluster(&hive);
        let role_group_name = RoleGroupName::from_str("default").expect("valid role group name");

        let meta = object_meta(&cluster, "test-name", &role_group_name).build();

        assert_eq!(meta.name.as_deref(), Some("test-name"));
        assert_eq!(meta.namespace.as_deref(), Some(cluster.namespace.as_ref()));
        assert!(meta.owner_references.is_some());
        assert!(meta.labels.is_some());
    }
}

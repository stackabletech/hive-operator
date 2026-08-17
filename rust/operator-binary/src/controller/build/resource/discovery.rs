use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{api::ObjectMeta, runtime::reflector::ObjectRef},
    v2::types::{kubernetes::ConfigMapName, operator::ClusterName},
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            object_meta, recommended_labels_for_role_resources,
            resource::listener::build_listener_connection_string,
        },
    },
    crd::{HiveRole, v1alpha1},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("could not build discovery config map for {obj_ref}"))]
    DiscoveryConfigMap {
        source: stackable_operator::builder::configmap::Error,
        obj_ref: ObjectRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("failed to configure listener discovery configmap"))]
    ListenerConfiguration {
        source: crate::controller::build::resource::listener::Error,
    },
}

/// An [`ObjectRef`] back to the owning [`v1alpha1::HiveCluster`], reconstructed from the validated
/// cluster identity for use in error messages.
fn cluster_object_ref(cluster: &ValidatedCluster) -> ObjectRef<v1alpha1::HiveCluster> {
    ObjectRef::new(cluster.name.as_ref()).within(cluster.namespace.as_ref())
}

/// The name of the discovery [`ConfigMap`] -- the cluster name itself.
///
/// Takes the bare cluster name (not [`ValidatedCluster`]) so the dereference step, which runs
/// before validation, can derive the same name.
pub fn discovery_config_map_name(cluster_name: &ClusterName) -> ConfigMapName {
    ConfigMapName::from_str(cluster_name.as_ref())
        .expect("a valid cluster name is a valid ConfigMap name")
}

/// Builds the discovery [`ConfigMap`] containing information about how to connect to a certain
/// [`v1alpha1::HiveCluster`].
///
/// The ConfigMap needs the role Listener's ingress address, which only the listener-operator
/// writes. While the dereferenced Listener is absent or still address-less (around the first
/// reconcile runs), no fresh ConfigMap can be built; the previously stored one is then
/// re-emitted unchanged so that it stays tracked by the apply step instead of being deleted as
/// an orphan. `None` is only returned before a discovery ConfigMap has ever been stored
/// (initial deploy) -- the run is not failed, since the Listener watch triggers a new run once
/// the address is set.
pub fn build_discovery_configmap(
    cluster: &ValidatedCluster,
    hive_role: &HiveRole,
) -> Result<Option<ConfigMap>, Error> {
    let Some(listener_address) = cluster
        .role_listener
        .as_ref()
        .and_then(|listener| listener.status.as_ref())
        .and_then(|status| status.ingress_addresses.as_ref()?.first())
    else {
        return Ok(match &cluster.existing_discovery_config_map {
            Some(existing) => {
                tracing::debug!(
                    "the metastore role Listener has no ingress address, \
                       re-emitting the stored discovery ConfigMap"
                );
                Some(reemit_discovery_configmap(cluster, hive_role, existing))
            }
            None => {
                tracing::debug!(
                    "the metastore role Listener has no ingress address yet and no \
                       discovery ConfigMap is stored, skipping it"
                );
                None
            }
        });
    };

    let mut discovery_configmap = ConfigMapBuilder::new();

    discovery_configmap.metadata(discovery_config_map_meta(cluster, hive_role));

    discovery_configmap.add_data(
        "HIVE".to_string(),
        build_listener_connection_string(listener_address, &hive_role.to_string())
            .context(ListenerConfigurationSnafu)?,
    );

    let config_map = discovery_configmap
        .build()
        .with_context(|_| DiscoveryConfigMapSnafu {
            obj_ref: cluster_object_ref(cluster),
        })?;

    Ok(Some(config_map))
}

/// Metadata shared by the freshly built and the re-emitted discovery [`ConfigMap`].
///
/// Discovery is a role-level object; the resource name comes from
/// [`discovery_config_map_name`] and the labels are the role-level recommended labels.
fn discovery_config_map_meta(cluster: &ValidatedCluster, hive_role: &HiveRole) -> ObjectMeta {
    object_meta(
        cluster,
        discovery_config_map_name(&cluster.name),
        recommended_labels_for_role_resources(cluster, hive_role),
    )
    .build()
}

/// Re-emits the discovery [`ConfigMap`] as previously stored in the cluster, so that the apply
/// step keeps tracking it in `ClusterResources` while no fresh one can be built -- an untracked
/// ConfigMap would be deleted as an orphan, breaking Pods that mount it.
///
/// The fetched `data` is carried over unchanged; applying identical content via server-side
/// apply changes nothing on the server. The metadata is built fresh instead of echoing the
/// fetched metadata: a fetched object carries server-populated fields (`resourceVersion`,
/// `uid`, `managedFields`) that must not appear in an apply patch.
fn reemit_discovery_configmap(
    cluster: &ValidatedCluster,
    hive_role: &HiveRole,
    existing: &ConfigMap,
) -> ConfigMap {
    ConfigMap {
        metadata: discovery_config_map_meta(cluster, hive_role),
        data: existing.data.clone(),
        ..ConfigMap::default()
    }
}

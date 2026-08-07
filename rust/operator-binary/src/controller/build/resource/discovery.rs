use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder, k8s_openapi::api::core::v1::ConfigMap,
    kube::runtime::reflector::ObjectRef,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            PLACEHOLDER_DISCOVERY_ROLE_GROUP, object_meta,
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

/// Builds the discovery [`ConfigMap`] containing information about how to connect to a certain
/// [`v1alpha1::HiveCluster`], or `None` while the metastore role Listener is absent or has no
/// ingress address.
///
/// The ConfigMap needs the role Listener's ingress address, which only the listener-operator
/// writes. Around the first reconcile runs the dereferenced Listener is absent or still
/// address-less; the ConfigMap is skipped then instead of failing the whole run -- the Listener
/// watch triggers a new run once the address is set. In that window an already existing
/// discovery ConfigMap is deleted as an orphan (only reachable when the Listener is deleted and
/// re-created) and re-created by the next run.
pub fn build_discovery_configmap(
    cluster: &ValidatedCluster,
    hive_role: HiveRole,
) -> Result<Option<ConfigMap>, Error> {
    let Some(listener_address) = cluster
        .role_listener
        .as_ref()
        .and_then(|listener| listener.status.as_ref())
        .and_then(|status| status.ingress_addresses.as_ref()?.first())
    else {
        tracing::debug!(
            "the metastore role Listener has no ingress address yet, \
               skipping the discovery ConfigMap"
        );
        return Ok(None);
    };

    let mut discovery_configmap = ConfigMapBuilder::new();

    discovery_configmap.metadata(
        // Discovery is a role-level object; the cluster name is used as the resource name
        // (matching `name_and_namespace`) and "discovery" as a placeholder role-group name
        // for the recommended labels.
        object_meta(
            cluster,
            cluster.name.to_string(),
            &PLACEHOLDER_DISCOVERY_ROLE_GROUP,
        )
        .build(),
    );

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

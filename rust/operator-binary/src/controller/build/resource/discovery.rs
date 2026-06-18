use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder, crd::listener::v1alpha1::Listener,
    k8s_openapi::api::core::v1::ConfigMap, kube::runtime::reflector::ObjectRef,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            PLACEHOLDER_DISCOVERY_ROLE_GROUP, resource::listener::build_listener_connection_string,
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

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain
/// [`v1alpha1::HiveCluster`].
///
/// Data is coming from the [`Listener`] objects. Connection string is only built by [`build_listener_connection_string`].
pub fn build_discovery_configmap(
    cluster: &ValidatedCluster,
    hive_role: HiveRole,
    listener: Listener,
) -> Result<ConfigMap, Error> {
    let mut discovery_configmap = ConfigMapBuilder::new();

    discovery_configmap.metadata(
        cluster
            // Discovery is a role-level object; the cluster name is used as the resource name
            // (matching `name_and_namespace`) and "discovery" as a placeholder role-group name
            // for the recommended labels.
            .object_meta(cluster.name.to_string(), &PLACEHOLDER_DISCOVERY_ROLE_GROUP)
            .build(),
    );

    discovery_configmap.add_data(
        "HIVE".to_string(),
        build_listener_connection_string(listener, &hive_role.to_string())
            .context(ListenerConfigurationSnafu)?,
    );

    discovery_configmap
        .build()
        .with_context(|_| DiscoveryConfigMapSnafu {
            obj_ref: cluster_object_ref(cluster),
        })
}

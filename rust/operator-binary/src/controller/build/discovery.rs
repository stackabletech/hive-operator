use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    crd::listener::v1alpha1::Listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::runtime::reflector::ObjectRef,
};

use crate::{
    controller::{ValidatedCluster, build_recommended_labels},
    crd::{HiveRole, v1alpha1},
    listener::build_listener_connection_string,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference {obj_ref}"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        obj_ref: ObjectRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("could not build discovery config map for {obj_ref}"))]
    DiscoveryConfigMap {
        source: stackable_operator::builder::configmap::Error,
        obj_ref: ObjectRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },
    #[snafu(display("failed to configure listener discovery configmap"))]
    ListenerConfiguration { source: crate::listener::Error },
}

/// An [`ObjectRef`] back to the owning [`v1alpha1::HiveCluster`], reconstructed from the validated
/// cluster identity for use in error messages.
fn cluster_object_ref(cluster: &ValidatedCluster) -> ObjectRef<v1alpha1::HiveCluster> {
    ObjectRef::new(cluster.name.as_ref()).within(cluster.namespace.as_ref())
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`v1alpha1::HiveCluster`] for all expected
/// scenarios.
pub async fn build_discovery_configmaps(
    cluster: &ValidatedCluster,
    hive_role: HiveRole,
    chroot: Option<&str>,
    listener: Listener,
) -> Result<Vec<ConfigMap>, Error> {
    let discovery_configmaps = vec![build_discovery_configmap(
        cluster, hive_role, chroot, listener,
    )?];

    Ok(discovery_configmaps)
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain
/// [`v1alpha1::HiveCluster`].
///
/// Data is coming from the [`Listener`] objects. Connection string is only build by [`build_listener_connection_string`].
fn build_discovery_configmap(
    cluster: &ValidatedCluster,
    hive_role: HiveRole,
    chroot: Option<&str>,
    listener: Listener,
) -> Result<ConfigMap, Error> {
    let mut discovery_configmap = ConfigMapBuilder::new();

    discovery_configmap.metadata(
        ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .ownerreference_from_resource(cluster, None, Some(true))
            .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                obj_ref: cluster_object_ref(cluster),
            })?
            .with_recommended_labels(&build_recommended_labels(
                cluster,
                &cluster.image.app_version_label_value,
                &hive_role.to_string(),
                "discovery",
            ))
            .context(MetadataBuildSnafu)?
            .build(),
    );

    discovery_configmap.add_data(
        "HIVE".to_string(),
        build_listener_connection_string(listener, &hive_role.to_string(), chroot)
            .context(ListenerConfigurationSnafu)?,
    );

    discovery_configmap
        .build()
        .with_context(|_| DiscoveryConfigMapSnafu {
            obj_ref: cluster_object_ref(cluster),
        })
}

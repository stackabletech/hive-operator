use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    crd::listener::v1alpha1::Listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{Resource, runtime::reflector::ObjectRef},
};

use crate::{
    controller::build_recommended_labels,
    crd::{HiveRole, v1alpha1},
    listener::build_listener_connection_string,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference {hive}"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        hive: ObjectRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("could not build discovery config map for {obj_ref}"))]
    DiscoveryConfigMap {
        source: stackable_operator::builder::configmap::Error,
        obj_ref: ObjectRef<v1alpha1::HiveCluster>,
    },
    #[snafu(display("invalid owner name for discovery ConfigMap"))]
    InvalidOwnerNameForDiscoveryConfigMap,

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },
    #[snafu(display("failed to configure listener discovery configmap"))]
    ListenerConfiguration { source: crate::listener::Error },
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`v1alpha1::HiveCluster`] for all expected
/// scenarios.
pub async fn build_discovery_configmaps(
    owner: &impl Resource<DynamicType = ()>,
    hive: &v1alpha1::HiveCluster,
    hive_role: HiveRole,
    resolved_product_image: &ResolvedProductImage,
    chroot: Option<&str>,
    listener: Listener,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner
        .meta()
        .name
        .as_ref()
        .context(InvalidOwnerNameForDiscoveryConfigMapSnafu)?;

    let discovery_configmaps = vec![build_discovery_configmap(
        name,
        owner,
        hive,
        hive_role,
        resolved_product_image,
        chroot,
        listener,
    )?];

    Ok(discovery_configmaps)
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain
/// [`v1alpha1::HiveCluster`].
///
/// Data is coming from the [`Listener`] objects. Connection string is only build by [`build_listener_connection_string`].
fn build_discovery_configmap(
    name: &str,
    owner: &impl Resource<DynamicType = ()>,
    hive: &v1alpha1::HiveCluster,
    hive_role: HiveRole,
    resolved_product_image: &ResolvedProductImage,
    chroot: Option<&str>,
    listener: Listener,
) -> Result<ConfigMap, Error> {
    let mut discovery_configmap = ConfigMapBuilder::new();

    discovery_configmap.metadata(
        ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(name)
            .ownerreference_from_resource(owner, None, Some(true))
            .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                hive: ObjectRef::from_obj(hive),
            })?
            .with_recommended_labels(build_recommended_labels(
                hive,
                &resolved_product_image.app_version_label,
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
            obj_ref: ObjectRef::from_obj(hive),
        })
}

pub fn build_headless_role_group_metrics_service_name(name: String) -> String {
    format!("{name}-metrics")
}

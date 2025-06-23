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
    crd::{HIVE_PORT_NAME, HiveRole, v1alpha1},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference {hive}"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        hive: ObjectRef<v1alpha1::HiveCluster>,
    },
    #[snafu(display("chroot path {chroot} was relative (must be absolute)"))]
    RelativeChroot { chroot: String },
    #[snafu(display("could not build discovery config map for {obj_ref}"))]
    DiscoveryConfigMap {
        source: stackable_operator::builder::configmap::Error,
        obj_ref: ObjectRef<v1alpha1::HiveCluster>,
    },
    #[snafu(display("could not find port [{port_name}] for rolegroup listener {role}"))]
    NoServicePort { port_name: String, role: String },

    #[snafu(display("invalid owner name for discovery ConfigMap"))]
    InvalidOwnerNameForDiscoveryConfigMap,

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },
    #[snafu(display("{role} listener has no adress"))]
    RoleListenerHasNoAddress { role: String },
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
        build_listener_connection_string(listener, &hive_role.to_string(), chroot)?,
    );

    discovery_configmap
        .build()
        .with_context(|_| DiscoveryConfigMapSnafu {
            obj_ref: ObjectRef::from_obj(hive),
        })
}

// Builds the connection string with respect to the listener provided objects
fn build_listener_connection_string(
    listener_ref: Listener,
    role: &String,
    chroot: Option<&str>,
) -> Result<String, Error> {
    // We only need the first address corresponding to the role
    let listener_address = listener_ref
        .status
        .and_then(|s| s.ingress_addresses?.into_iter().next())
        .context(RoleListenerHasNoAddressSnafu { role })?;
    let mut conn_str = format!(
        "thrift://{address}:{port}",
        address = listener_address.address,
        port = listener_address
            .ports
            .get(HIVE_PORT_NAME)
            .copied()
            .context(NoServicePortSnafu {
                port_name: HIVE_PORT_NAME,
                role
            })?
    );
    if let Some(chroot) = chroot {
        if !chroot.starts_with('/') {
            return RelativeChrootSnafu { chroot }.fail();
        }
        conn_str.push_str(chroot);
    }
    Ok(conn_str)
}

pub fn build_headless_role_group_metrics_service_name(name: String) -> String {
    format!("{name}-metrics", name = name)
}

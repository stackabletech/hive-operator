use std::{collections::BTreeMap, num::TryFromIntError};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    crd::listener::v1alpha1::Listener,
    k8s_openapi::api::core::v1::{ConfigMap, Service},
    kube::{Resource, runtime::reflector::ObjectRef},
};

use crate::{
    controller::build_recommended_labels,
    crd::{HIVE_PORT_NAME, HiveRole, v1alpha1},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no name associated"))]
    NoName,
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
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
    #[snafu(display("could not find port [{port_name}]"))]
    NoServicePort {
        port_name: String,
        //obj_ref: ObjectRef<Service>,
    },
    #[snafu(display("service [{obj_ref}] port [{port_name}] does not have a nodePort "))]
    NoNodePort {
        port_name: String,
        obj_ref: ObjectRef<Service>,
    },
    #[snafu(display("could not find Endpoints for {svc}"))]
    FindEndpoints {
        source: stackable_operator::client::Error,
        svc: ObjectRef<Service>,
    },
    #[snafu(display("nodePort was out of range"))]
    InvalidNodePort { source: TryFromIntError },
    #[snafu(display("invalid owner name for discovery ConfigMap"))]
    InvalidOwnerNameForDiscoveryConfigMap,

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },
    #[snafu(display("{rolegroup} listener has no adress"))]
    RoleGroupListenerHasNoAddress { rolegroup: String },
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`v1alpha1::HiveCluster`] for all expected
/// scenarios.
pub async fn build_discovery_configmaps(
    owner: &impl Resource<DynamicType = ()>,
    hive: &v1alpha1::HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    chroot: Option<&str>,
    listener_refs: BTreeMap<&String, Listener>,
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
        resolved_product_image,
        chroot,
        listener_refs,
    )?];

    Ok(discovery_configmaps)
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain
/// [`v1alpha1::HiveCluster`].
///
/// Data is coming from the [`Listener`] objects. Connection string is only build by [`build_listener_connection_string`]
fn build_discovery_configmap(
    name: &str,
    owner: &impl Resource<DynamicType = ()>,
    hive: &v1alpha1::HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    chroot: Option<&str>,
    listener_refs: BTreeMap<&String, Listener>,
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
                &HiveRole::MetaStore.to_string(),
                "discovery",
            ))
            .context(MetadataBuildSnafu)?
            .build(),
    );

    for (rolegroup, listener_ref) in listener_refs {
        // Names are equal to role group listener name test
        discovery_configmap.add_data(
            rolegroup,
            build_listener_connection_string(listener_ref, rolegroup, chroot)?,
        );
    }

    discovery_configmap
        .build()
        .with_context(|_| DiscoveryConfigMapSnafu {
            obj_ref: ObjectRef::from_obj(hive),
        })
}

// Builds the connection string with respect to the listener provided objects
fn build_listener_connection_string(
    listener_ref: Listener,
    rolegroup: &String,
    chroot: Option<&str>,
) -> Result<String, Error> {
    // We'd need only the first address corresponding to the rolegroup
    let listener_address = listener_ref
        .status
        .and_then(|s| s.ingress_addresses?.into_iter().next())
        .context(RoleGroupListenerHasNoAddressSnafu { rolegroup })?;
    let mut conn_str = format!(
        "thrift://{}:{}",
        listener_address.address,
        listener_address
            .ports
            .get(HIVE_PORT_NAME)
            .copied()
            .context(NoServicePortSnafu {
                port_name: HIVE_PORT_NAME
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

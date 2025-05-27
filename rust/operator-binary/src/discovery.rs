use std::{collections::BTreeSet, num::TryFromIntError};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service},
    kube::{Resource, runtime::reflector::ObjectRef},
};

use crate::{
    controller::build_recommended_labels,
    crd::{HIVE_PORT, HIVE_PORT_NAME, HiveRole, v1alpha1},
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
    #[snafu(display("could not find service [{obj_ref}] port [{port_name}]"))]
    NoServicePort {
        port_name: String,
        obj_ref: ObjectRef<Service>,
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
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`v1alpha1::HiveCluster`] for all expected
/// scenarios.
pub async fn build_discovery_configmaps(
    client: &stackable_operator::client::Client,
    owner: &impl Resource<DynamicType = ()>,
    hive: &v1alpha1::HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    chroot: Option<&str>,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner
        .meta()
        .name
        .as_ref()
        .context(InvalidOwnerNameForDiscoveryConfigMapSnafu)?;
    let namespace = owner
        .meta()
        .namespace
        .as_deref()
        .context(NoNamespaceSnafu)?;
    let cluster_domain = &client.kubernetes_cluster_info.cluster_domain;
    let discovery_configmaps = vec![build_discovery_configmap(
        name,
        owner,
        hive,
        resolved_product_image,
        chroot,
        vec![(
            format!("{name}.{namespace}.svc.{cluster_domain}"),
            HIVE_PORT,
        )],
    )?];

    Ok(discovery_configmaps)
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain
/// [`v1alpha1::HiveCluster`].
///
/// `hosts` will usually come from the cluster role service or [`nodeport_hosts`].
fn build_discovery_configmap(
    name: &str,
    owner: &impl Resource<DynamicType = ()>,
    hive: &v1alpha1::HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    chroot: Option<&str>,
    hosts: impl IntoIterator<Item = (impl Into<String>, u16)>,
) -> Result<ConfigMap, Error> {
    let mut conn_str = hosts
        .into_iter()
        .map(|(host, port)| format!("thrift://{}:{}", host.into(), port))
        .collect::<Vec<_>>()
        .join("\n");
    if let Some(chroot) = chroot {
        if !chroot.starts_with('/') {
            return RelativeChrootSnafu { chroot }.fail();
        }
        conn_str.push_str(chroot);
    }
    ConfigMapBuilder::new()
        .metadata(
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
        )
        .add_data("HIVE", conn_str)
        .build()
        .with_context(|_| DiscoveryConfigMapSnafu {
            obj_ref: ObjectRef::from_obj(hive),
        })
}

/// Lists all nodes currently hosting Pods participating in the [`Service`]
async fn nodeport_hosts(
    client: &stackable_operator::client::Client,
    svc: &Service,
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)>, Error> {
    let svc_port = svc
        .spec
        .as_ref()
        .and_then(|svc_spec| {
            svc_spec
                .ports
                .as_ref()?
                .iter()
                .find(|port| port.name.as_deref() == Some(HIVE_PORT_NAME))
        })
        .context(NoServicePortSnafu {
            port_name,
            obj_ref: ObjectRef::from_obj(svc),
        })?;

    let node_port = svc_port.node_port.context(NoNodePortSnafu {
        port_name,
        obj_ref: ObjectRef::from_obj(svc),
    })?;
    let endpoints = client
        .get::<Endpoints>(
            svc.metadata.name.as_deref().context(NoNameSnafu)?,
            svc.metadata
                .namespace
                .as_deref()
                .context(NoNamespaceSnafu)?,
        )
        .await
        .with_context(|_| FindEndpointsSnafu {
            svc: ObjectRef::from_obj(svc),
        })?;
    let nodes = endpoints
        .subsets
        .into_iter()
        .flatten()
        .flat_map(|subset| subset.addresses)
        .flatten()
        .flat_map(|addr| addr.node_name);
    let addrs = nodes
        .map(|node| Ok((node, node_port.try_into().context(InvalidNodePortSnafu)?)))
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(addrs)
}

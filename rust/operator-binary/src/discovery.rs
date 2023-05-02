use crate::controller::build_recommended_labels;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hive_crd::{HiveCluster, HiveRole, ServiceType, HIVE_PORT, HIVE_PORT_NAME};
use stackable_operator::commons::product_image_selection::ResolvedProductImage;
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    k8s_openapi::api::core::v1::{Endpoints, Service, ServiceSpec},
    kube::{runtime::reflector::ObjectRef, Resource},
};
use std::collections::BTreeSet;
use std::num::TryFromIntError;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no name associated"))]
    NoName,
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("object is missing metadata to build owner reference {hive}"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        hive: ObjectRef<HiveCluster>,
    },
    #[snafu(display("chroot path {chroot} was relative (must be absolute)"))]
    RelativeChroot { chroot: String },
    #[snafu(display("failed to list expected pods"))]
    ExpectedPods {
        source: stackable_hive_crd::NoNamespaceError,
    },
    #[snafu(display("could not build discovery config map for {obj_ref}"))]
    DiscoveryConfigMap {
        source: stackable_operator::error::Error,
        obj_ref: ObjectRef<HiveCluster>,
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
        source: stackable_operator::error::Error,
        svc: ObjectRef<Service>,
    },
    #[snafu(display("nodePort was out of range"))]
    InvalidNodePort { source: TryFromIntError },
    #[snafu(display("invalid owner name for discovery ConfigMap"))]
    InvalidOwnerNameForDiscoveryConfigMap,
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`HiveCluster`] for all expected scenarios
pub async fn build_discovery_configmaps(
    client: &stackable_operator::client::Client,
    owner: &impl Resource<DynamicType = ()>,
    hive: &HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    svc: &Service,
    chroot: Option<&str>,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner
        .meta()
        .name
        .as_ref()
        .context(InvalidOwnerNameForDiscoveryConfigMapSnafu)?;
    let mut discovery_configmaps = vec![build_discovery_configmap(
        name,
        owner,
        hive,
        resolved_product_image,
        chroot,
        pod_hosts(hive)?,
    )?];

    // TODO: Temporary solution until listener-operator is finished
    if let Some(ServiceSpec {
        type_: Some(service_type),
        ..
    }) = svc.spec.as_ref()
    {
        if service_type == &ServiceType::NodePort.to_string() {
            discovery_configmaps.push(build_discovery_configmap(
                &format!("{}-nodeport", name),
                owner,
                hive,
                resolved_product_image,
                chroot,
                nodeport_hosts(client, svc, HIVE_PORT_NAME).await?,
            )?);
        }
    }

    Ok(discovery_configmaps)
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`HiveCluster`]
///
/// `hosts` will usually come from either [`pod_hosts`] or [`nodeport_hosts`].
fn build_discovery_configmap(
    name: &str,
    owner: &impl Resource<DynamicType = ()>,
    hive: &HiveCluster,
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
                .build(),
        )
        .add_data("HIVE", conn_str)
        .build()
        .with_context(|_| DiscoveryConfigMapSnafu {
            obj_ref: ObjectRef::from_obj(hive),
        })
}

/// Lists all Pods FQDNs expected to host the [`HiveCluster`]
fn pod_hosts(hive: &HiveCluster) -> Result<impl IntoIterator<Item = (String, u16)> + '_, Error> {
    Ok(hive
        .pods()
        .context(ExpectedPodsSnafu)?
        .map(|pod_ref| (pod_ref.fqdn(), HIVE_PORT)))
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

use std::{collections::BTreeSet, num::TryFromIntError};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hive_crd::{HiveCluster, HiveRole, APP_NAME, APP_PORT};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service},
    kube::{runtime::reflector::ObjectRef, Resource, ResourceExt},
};

use crate::controller::hive_version;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", hive))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        hive: ObjectRef<HiveCluster>,
    },
    #[snafu(display("chroot path {} was relative (must be absolute)", chroot))]
    RelativeChroot { chroot: String },
    #[snafu(display("object has no name associated"))]
    NoName,
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("failed to list expected pods"))]
    ExpectedPods {
        source: stackable_hive_crd::NoNamespaceError,
    },
    #[snafu(display("could not find service port with name {}", port_name))]
    NoServicePort { port_name: String },
    #[snafu(display("service port with name {} does not have a nodePort", port_name))]
    NoNodePort { port_name: String },
    #[snafu(display("could not find Endpoints for {}", svc))]
    FindEndpoints {
        source: stackable_operator::error::Error,
        svc: ObjectRef<Service>,
    },
    #[snafu(display("nodePort was out of range"))]
    InvalidNodePort { source: TryFromIntError },
    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::error::Error,
    },
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`HiveCluster`] for all expected scenarios
pub async fn build_discovery_configmaps(
    client: &stackable_operator::client::Client,
    owner: &impl Resource<DynamicType = ()>,
    hive: &HiveCluster,
    svc: &Service,
    chroot: Option<&str>,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner.name();
    Ok(vec![
        build_discovery_configmap(&name, owner, hive, chroot, pod_hosts(hive)?)?,
        // TODO: do we need that - i think there is only internal access required?
        build_discovery_configmap(
            &format!("{}-nodeport", name),
            owner,
            hive,
            chroot,
            nodeport_hosts(client, svc, "hive").await?,
        )?,
    ])
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`HiveCluster`]
///
/// `hosts` will usually come from either [`pod_hosts`] or [`nodeport_hosts`].
fn build_discovery_configmap(
    name: &str,
    owner: &impl Resource<DynamicType = ()>,
    hive: &HiveCluster,
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
            return RelativeChroot { chroot }.fail();
        }
        conn_str.push_str(chroot);
    }
    Ok(ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hive)
                .name(name)
                .ownerreference_from_resource(owner, None, Some(true))
                .with_context(|| ObjectMissingMetadataForOwnerRef {
                    hive: ObjectRef::from_obj(hive),
                })?
                .with_recommended_labels(
                    hive,
                    APP_NAME,
                    hive_version(hive).unwrap(),
                    &HiveRole::MetaStore.to_string(),
                    "discovery",
                )
                .build(),
        )
        .add_data("HIVE", conn_str)
        .build()
        .unwrap())
}

/// Lists all Pods FQDNs expected to host the [`HiveCluster`]
fn pod_hosts(hive: &HiveCluster) -> Result<impl IntoIterator<Item = (String, u16)> + '_, Error> {
    Ok(hive
        .pods()
        .context(ExpectedPods)?
        .into_iter()
        .map(|pod_ref| (pod_ref.fqdn(), APP_PORT)))
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
                .find(|port| port.name.as_deref() == Some("hive"))
        })
        .context(NoServicePort { port_name })?;
    let node_port = svc_port.node_port.context(NoNodePort { port_name })?;
    let endpoints = client
        .get::<Endpoints>(
            svc.metadata.name.as_deref().context(NoName)?,
            svc.metadata.namespace.as_deref(),
        )
        .await
        .with_context(|| FindEndpoints {
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
        .map(|node| Ok((node, node_port.try_into().context(InvalidNodePort)?)))
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(addrs)
}

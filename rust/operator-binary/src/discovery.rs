use crate::controller::hive_version;

use snafu::{ResultExt, Snafu};
use stackable_hive_crd::{HiveCluster, HiveRole, APP_NAME, APP_PORT};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{runtime::reflector::ObjectRef, Resource, ResourceExt},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", hive))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        hive: ObjectRef<HiveCluster>,
    },
    #[snafu(display("chroot path {} was relative (must be absolute)", chroot))]
    RelativeChroot { chroot: String },
    #[snafu(display("failed to list expected pods"))]
    ExpectedPods {
        source: stackable_hive_crd::NoNamespaceError,
    },
    #[snafu(display("operator framework reported error"))]
    OperatorFramework {
        source: stackable_operator::error::Error,
    },
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`HiveCluster`]
pub fn build_discovery_configmap(
    owner: &impl Resource<DynamicType = ()>,
    hive: &HiveCluster,
    chroot: Option<&str>,
) -> Result<ConfigMap, Error> {
    let name = owner.name();
    let hosts = pod_hosts(hive)?;
    let mut conn_str = hosts
        .into_iter()
        .map(|(host, port)| format!("thrift://{}:{}", host, port))
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
                .with_recommended_labels(
                    hive,
                    APP_NAME,
                    hive_version(hive).unwrap(),
                    &HiveRole::MetaStore.to_string(),
                    "discovery",
                )
                .build(),
        )
        .add_data("hive", conn_str)
        .build()
        .context(OperatorFrameworkSnafu)
}

/// Lists all Pods FQDNs expected to host the [`HiveCluster`]
fn pod_hosts(hive: &HiveCluster) -> Result<impl IntoIterator<Item = (String, u16)> + '_, Error> {
    Ok(hive
        .pods()
        .context(ExpectedPodsSnafu)?
        .into_iter()
        .map(|pod_ref| (pod_ref.fqdn(), APP_PORT)))
}

use crate::error::Error::{
    NoHiveMetastorePodsAvailableForConnectionInfo, ObjectWithoutName, OperatorFrameworkError,
    PodWithoutHostname,
};
use crate::error::HiveOperatorResult;
use crate::{HiveCluster, HiveRole, APP_NAME, MANAGED_BY, METASTORE_PORT};

use crate::discovery::TicketReferences::ErrHivePodWithoutName;
use serde::{Deserialize, Serialize};
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_operator::k8s_openapi::api::core::v1::Pod;
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use stackable_operator::labels::{
    APP_COMPONENT_LABEL, APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL,
};
use stackable_operator::schemars::{self, JsonSchema};
use std::collections::BTreeMap;
use strum_macros::Display;
use tracing::{debug, warn};

#[derive(Display)]
pub enum TicketReferences {
    ErrHivePodWithoutName,
}

// TODO: This should probably be moved (it is here for now to be shared with Hive and Trino).
/// Contains all the required connection information for S3.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Connection {
    pub end_point: String,
    pub access_key: String,
    pub secret_key: String,
    pub ssl_enabled: bool,
    pub path_style_access: bool,
}

/// Contains all necessary information to identify a Stackable managed Hive
/// cluster and build a connection string for it.
/// The main purpose for this struct is for other operators that need to reference a
/// Hive ensemble to use in their CRDs.
/// This has the benefit of keeping references to Hive ensembles consistent
/// throughout the entire stack.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HiveReference {
    pub namespace: String,
    pub name: String,
    pub db: Option<String>,
}

/// Contains all necessary information to establish a connection with a Hive cluster.
/// Other operators using this crate will interact with this struct.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, Hash, PartialEq)]
pub struct HiveConnectionInformation {
    pub node_name: String,
    pub port: String,
    pub db: Option<String>,
}

impl HiveConnectionInformation {
    /// Returns a full qualified connection string for hive metastore.
    /// This has the form `thrift://host:port[/db]`
    /// For example:
    ///  - thrift://server1:9000/db
    pub fn full_connection_string(&self) -> String {
        let con = self.connection_string();
        if let Some(db) = &self.db {
            return format!("{}{}", con, db);
        }
        con
    }

    /// Returns a connection string (without db) for a metastore as defined by Hive.
    /// This has the form `thrift://host:port`
    /// For example:
    ///  - thrift://server1:9000
    pub fn connection_string(&self) -> String {
        format!("thrift://{}:{}", self.node_name, self.port)
    }
}

/// Returns connection information for a Hive Cluster custom resource.
///
/// # Arguments
///
/// * `client` - A [`stackable_operator::client::Client`] used to access the Kubernetes cluster
/// * `hive_reference` - The HiveReference in the custom resource
///
pub async fn get_hive_connection_info(
    client: &Client,
    hive_reference: &HiveReference,
) -> HiveOperatorResult<Option<HiveConnectionInformation>> {
    check_hive_reference(client, &hive_reference.name, &hive_reference.namespace).await?;

    let hive_pods = client
        .list_with_label_selector(None, &get_match_labels(&hive_reference.name))
        .await?;

    // No Hive pods means empty connect string. We throw an error indicating to check the
    // Hive custom resource or the Hive operator for errors.
    if hive_pods.is_empty() {
        return Err(NoHiveMetastorePodsAvailableForConnectionInfo {
            namespace: hive_reference.namespace.clone(),
            name: hive_reference.name.clone(),
        });
    }

    get_hive_connection_string_from_pods(&hive_pods, hive_reference.db.as_deref())
}

/// Builds the actual connection string after all necessary information has been retrieved.
/// Takes a list of pods belonging to this cluster from which the hostnames are retrieved.
/// Checks the 'metastore' container port instead of the cluster spec to retrieve the correct port.
///
/// WARNING: For now this only works with one metastore.
///
/// # Arguments
///
/// * `hive_pods` - All pods belonging to the cluster
/// * `root` - The additional root (e.g. "/hbase") appended to the connection string
///
pub fn get_hive_connection_string_from_pods(
    hive_pods: &[Pod],
    root: Option<&str>,
) -> HiveOperatorResult<Option<HiveConnectionInformation>> {
    let metastore_str = &HiveRole::MetaStore.to_string();
    let cleaned_db_root = pad_and_check_chroot(root)?;

    // filter for metastore
    let filtered_pods: Vec<&Pod> = hive_pods
        .iter()
        .filter(|pod| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(APP_COMPONENT_LABEL))
                == Some(metastore_str)
        })
        .collect();

    if filtered_pods.len() > 1 {
        warn!("Retrieved more than one metastore pod. This is not supported and may lead to untested side effects. \
           Please specify only one metastore in the custom resource via 'replicas=1'.");
    }

    for pod in &filtered_pods {
        let pod_name = match &pod.metadata.name {
            None => {
                return Err(ObjectWithoutName {
                    reference: ErrHivePodWithoutName.to_string(),
                })
            }
            Some(pod_name) => pod_name.clone(),
        };

        let node_name = match pod.spec.as_ref().and_then(|spec| spec.node_name.clone()) {
            None => {
                debug!("Pod [{:?}] is does not have node_name set, might not be scheduled yet, aborting.. ",
                       pod_name);
                return Err(PodWithoutHostname { pod: pod_name });
            }
            Some(node_name) => node_name,
        };

        if let Some(port) = extract_container_port(pod, APP_NAME, METASTORE_PORT) {
            return Ok(Some(HiveConnectionInformation {
                node_name,
                port,
                db: cleaned_db_root,
            }));
        }
    }

    Ok(None)
}

/// Build a Labelselector that applies only to metastore pods belonging to the cluster instance
/// referenced by `name`.
///
/// # Arguments
///
/// * `name` - The name of the Hive cluster
///
fn get_match_labels(name: &str) -> LabelSelector {
    let mut match_labels = BTreeMap::new();
    match_labels.insert(String::from(APP_NAME_LABEL), String::from(APP_NAME));
    match_labels.insert(String::from(APP_MANAGED_BY_LABEL), String::from(MANAGED_BY));
    match_labels.insert(String::from(APP_INSTANCE_LABEL), name.to_string());
    match_labels.insert(
        String::from(APP_COMPONENT_LABEL),
        HiveRole::MetaStore.to_string(),
    );

    LabelSelector {
        match_labels: Some(match_labels),
        ..Default::default()
    }
}

/// Check in kubernetes, whether the Hive object referenced by `hive_name` and `hive_namespace`
/// exists. If it exists the object will be returned.
///
/// # Arguments
///
/// * `client` - A [`stackable_operator::client::Client`] used to access the Kubernetes cluster
/// * `hive_name` - The name of Hive cluster
/// * `hive_namespace` - The namespace of the Hive cluster
///
async fn check_hive_reference(
    client: &Client,
    hive_name: &str,
    hive_namespace: &str,
) -> HiveOperatorResult<HiveCluster> {
    debug!(
        "Checking HiveReference if [{}] exists in namespace [{}].",
        hive_name, hive_namespace
    );
    let hive_cluster: OperatorResult<HiveCluster> =
        client.get(hive_name, Some(hive_namespace)).await;

    hive_cluster.map_err(|err| {
        warn!(?err, "Referencing a Hive cluster that does not exist (or some other error while fetching it): [{}/{}], we will requeue and check again",
                hive_namespace,
                hive_name
        );
        OperatorFrameworkError {source: err}
    })
}

/// Left pads the chroot string with a / if necessary - mostly for convenience, so users do not
/// need to specify the / when entering the chroot string in their config.
/// Checks if the result is a valid Hive path.
///
/// # Arguments
///
/// * `root` - The root (e.g 'hbase' or '/hbase')
///
fn pad_and_check_chroot(root: Option<&str>) -> HiveOperatorResult<Option<String>> {
    // Left pad the root with a / if needed
    // Sadly this requires copying the reference once,
    // but I know of no way to avoid that
    let padded_root = match root {
        None => return Ok(None),
        Some(root) => {
            if root.starts_with('/') {
                root.to_string()
            } else {
                format!("/{}", root)
            }
        }
    };
    Ok(Some(padded_root))
}

/// Extract the container port `port_name` from a container with name `container_name`.
/// Returns None if not the port or container are not available.
///
/// # Arguments
///
/// * `pod` - The pod to extract the container port from
/// * `container_name` - The name of the container to search for.
/// * `port_name` - The name of the container port.
///
fn extract_container_port(pod: &Pod, container_name: &str, port_name: &str) -> Option<String> {
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if container.name != container_name {
                continue;
            }

            if let Some(port) = container.ports.as_ref().and_then(|ports| {
                ports
                    .iter()
                    .find(|port| port.name == Some(port_name.to_string()))
            }) {
                return Some(port.container_port.to_string());
            }
        }
    }

    None
}

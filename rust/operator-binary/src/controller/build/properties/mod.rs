//! Per-file builders for the Hive metastore config files.

use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

pub mod core_site;
pub mod hive_site;
pub mod logging;
pub mod security_properties;

/// Resolve user-provided key/value overrides into `(key, value)` pairs, dropping
/// entries whose value is unset (`None`).
pub(crate) fn resolved_overrides(overrides: KeyValueConfigOverrides) -> BTreeMap<String, String> {
    overrides
        .overrides
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect()
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;

    use crate::{
        controller::ValidatedClusterConfig,
        crd::{
            databases::{MetadataDatabaseConnection, derby_driver_class},
            v1alpha1,
        },
    };

    pub const DERBY_YAML: &str = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
          namespace: default
        spec:
          image:
            productVersion: "4.0.0"
          clusterConfig:
            metadataDatabase:
              derby: {}
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;

    /// Build a minimal Derby-backed [`ValidatedClusterConfig`] for builder tests.
    pub fn derby_cluster_config() -> ValidatedClusterConfig {
        let hive: v1alpha1::HiveCluster =
            stackable_operator::utils::yaml_from_str_singleton_map(DERBY_YAML)
                .expect("valid HiveCluster YAML");
        let metadata_database_connection_details = hive
            .spec
            .cluster_config
            .metadata_database
            .jdbc_connection_details("METADATA")
            .expect("derby connection details");
        let connection_driver = match &hive.spec.cluster_config.metadata_database {
            MetadataDatabaseConnection::Derby(_) => derby_driver_class("4.0.0").to_owned(),
            _ => metadata_database_connection_details.driver.clone(),
        };
        ValidatedClusterConfig {
            metadata_database_connection_details,
            connection_driver,
            s3_connection_spec: None,
            hive_opa_config: None,
            kerberos_config: BTreeMap::new(),
            needs_kerberos_core_site: false,
        }
    }
}

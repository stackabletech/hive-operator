//! Per-file builders for the Hive metastore config files.

pub mod core_site;
pub mod hive_site;
pub mod product_logging;
pub mod security_properties;

/// The names of the Hive config files assembled into the rolegroup `ConfigMap`.
#[derive(Clone, Copy, Debug, strum::Display)]
pub enum ConfigFileName {
    #[strum(serialize = "hive-site.xml")]
    HiveSite,
    #[strum(serialize = "core-site.xml")]
    CoreSite,
    #[strum(serialize = "hdfs-site.xml")]
    HdfsSite,
    #[strum(serialize = "security.properties")]
    Security,
    #[strum(serialize = "metastore-log4j2.properties")]
    Log4j2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_match_the_hive_on_disk_names() {
        assert_eq!(ConfigFileName::HiveSite.to_string(), "hive-site.xml");
        assert_eq!(ConfigFileName::CoreSite.to_string(), "core-site.xml");
        assert_eq!(ConfigFileName::HdfsSite.to_string(), "hdfs-site.xml");
        assert_eq!(ConfigFileName::Security.to_string(), "security.properties");
        assert_eq!(
            ConfigFileName::Log4j2.to_string(),
            "metastore-log4j2.properties"
        );
    }
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

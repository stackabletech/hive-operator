//! Builder for `hive-site.xml`.
//!
//! Precedence (matches the pre-product-config-removal behaviour):
//! 1. Defaults: `hive.metastore.port=9083` (required product-config property, default value).
//! 2. Automatic / operator-injected: warehouse dir (hardcoded `/stackable/warehouse`),
//!    metrics enabled (`true`), JDBC driver/url/credentials, S3, Kerberos, OPA.
//! 3. Spec: `warehouseDir` from the merged config overrides the hardcoded warehouse dir.
//! 4. User `hive-site.xml` overrides (highest precedence).

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    crd::s3,
    database_connections::drivers::jdbc::JdbcDatabaseConnectionDetails,
    k8s_openapi::api::core::v1::EnvVar,
    utils::cluster_info::KubernetesClusterInfo,
};

use crate::{
    config::opa::HiveOpaConfig,
    crd::{
        MetaStoreConfig,
        databases::{MetadataDatabaseConnection, derby_driver_class},
        v1alpha1,
    },
    kerberos::kerberos_config_properties,
};

const DEFAULT_WAREHOUSE_DIR: &str = "/stackable/warehouse";
const HIVE_METASTORE_PORT: &str = "hive.metastore.port";
const DEFAULT_HIVE_METASTORE_PORT: &str = "9083";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: stackable_operator::crd::s3::v1alpha1::ConnectionError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Build the `hive-site.xml` key/value pairs. Values are `Option` so the writer
/// can skip unset entries (no `None` is produced here, but the type matches the
/// writer's iterator interface).
#[allow(clippy::too_many_arguments)]
pub fn build(
    hive: &v1alpha1::HiveCluster,
    hive_namespace: &str,
    product_version: &str,
    merged_config: &MetaStoreConfig,
    database_connection_details: &JdbcDatabaseConnectionDetails,
    s3_connection_spec: Option<&s3::v1alpha1::ConnectionSpec>,
    hive_opa_config: Option<&HiveOpaConfig>,
    cluster_info: &KubernetesClusterInfo,
    overrides: BTreeMap<String, String>,
) -> Result<BTreeMap<String, Option<String>>> {
    let mut data: BTreeMap<String, Option<String>> = BTreeMap::new();

    // 1. Defaults (required product-config property `hive.metastore.port`).
    data.insert(
        HIVE_METASTORE_PORT.to_string(),
        Some(DEFAULT_HIVE_METASTORE_PORT.to_string()),
    );

    // 2. Automatic / operator-injected.
    data.insert(
        MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
        Some(DEFAULT_WAREHOUSE_DIR.to_string()),
    );
    data.insert(
        MetaStoreConfig::METASTORE_METRICS_ENABLED.to_string(),
        Some("true".to_string()),
    );

    // The Derby driver class needs special handling.
    let driver = match &hive.spec.cluster_config.metadata_database {
        MetadataDatabaseConnection::Derby(_) => derby_driver_class(product_version),
        _ => database_connection_details.driver.as_str(),
    };
    data.insert(
        MetaStoreConfig::CONNECTION_DRIVER_NAME.to_string(),
        Some(driver.to_owned()),
    );
    data.insert(
        MetaStoreConfig::CONNECTION_URL.to_string(),
        Some(database_connection_details.connection_url.to_string()),
    );
    if let Some(EnvVar { name, .. }) = &database_connection_details.username_env {
        data.insert(
            MetaStoreConfig::CONNECTION_USER_NAME.to_string(),
            Some(format!("${{env:{name}}}")),
        );
    }
    if let Some(EnvVar { name, .. }) = &database_connection_details.password_env {
        data.insert(
            MetaStoreConfig::CONNECTION_PASSWORD.to_string(),
            Some(format!("${{env:{name}}}")),
        );
    }

    if let Some(s3) = s3_connection_spec {
        data.insert(
            MetaStoreConfig::S3_ENDPOINT.to_string(),
            Some(s3.endpoint().context(ConfigureS3ConnectionSnafu)?.to_string()),
        );
        data.insert(
            MetaStoreConfig::S3_REGION_NAME.to_string(),
            Some(s3.region.name.clone()),
        );
        if let Some((access_key_file, secret_key_file)) = s3.credentials_mount_paths() {
            data.insert(
                MetaStoreConfig::S3_ACCESS_KEY.to_string(),
                Some(format!("${{file:UTF-8:{access_key_file}}}")),
            );
            data.insert(
                MetaStoreConfig::S3_SECRET_KEY.to_string(),
                Some(format!("${{file:UTF-8:{secret_key_file}}}")),
            );
        }
        data.insert(
            MetaStoreConfig::S3_SSL_ENABLED.to_string(),
            Some(s3.tls.uses_tls().to_string()),
        );
        data.insert(
            MetaStoreConfig::S3_PATH_STYLE_ACCESS.to_string(),
            Some((s3.access_style == s3::v1alpha1::S3AccessStyle::Path).to_string()),
        );
    }

    for (name, value) in kerberos_config_properties(hive, hive_namespace, cluster_info) {
        data.insert(name.to_string(), Some(value.to_string()));
    }

    if let Some(opa_config) = hive_opa_config {
        data.extend(
            opa_config
                .as_config(product_version)
                .into_iter()
                .map(|(k, v)| (k, Some(v))),
        );
    }

    // 3. Spec: warehouse dir from the merged CRD config (overrides the default).
    if let Some(warehouse_dir) = &merged_config.warehouse_dir {
        data.insert(
            MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
            Some(warehouse_dir.clone()),
        );
    }

    // 4. User overrides (highest precedence).
    for (k, v) in overrides {
        data.insert(k, Some(v));
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hive_cluster(yaml: &str) -> v1alpha1::HiveCluster {
        stackable_operator::utils::yaml_from_str_singleton_map(yaml).expect("valid HiveCluster YAML")
    }

    const DERBY_YAML: &str = r#"
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

    fn cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: "cluster.local".parse().expect("valid domain"),
        }
    }

    fn db_details(hive: &v1alpha1::HiveCluster) -> JdbcDatabaseConnectionDetails {
        hive.spec
            .cluster_config
            .metadata_database
            .jdbc_connection_details("METADATA")
            .expect("derby connection details")
    }

    #[test]
    fn defaults_present_for_minimal_derby_cluster() {
        let hive = hive_cluster(DERBY_YAML);
        let merged = MetaStoreConfig::default();
        let db = db_details(&hive);

        let data = build(
            &hive,
            "default",
            "4.0.0",
            &merged,
            &db,
            None,
            None,
            &cluster_info(),
            BTreeMap::new(),
        )
        .expect("build hive-site");

        assert_eq!(
            data.get("hive.metastore.port"),
            Some(&Some("9083".to_string()))
        );
        assert_eq!(
            data.get("hive.metastore.metrics.enabled"),
            Some(&Some("true".to_string()))
        );
        assert_eq!(
            data.get("hive.metastore.warehouse.dir"),
            Some(&Some("/stackable/warehouse".to_string()))
        );
        assert!(data.contains_key("javax.jdo.option.ConnectionDriverName"));
        // No env credentials for an embedded Derby database.
        assert!(!data.contains_key("javax.jdo.option.ConnectionUserName"));
    }

    #[test]
    fn warehouse_dir_spec_overrides_default() {
        let hive = hive_cluster(DERBY_YAML);
        let merged = MetaStoreConfig {
            warehouse_dir: Some("/custom/warehouse".to_string()),
            ..MetaStoreConfig::default()
        };
        let db = db_details(&hive);

        let data = build(
            &hive,
            "default",
            "4.0.0",
            &merged,
            &db,
            None,
            None,
            &cluster_info(),
            BTreeMap::new(),
        )
        .expect("build hive-site");

        assert_eq!(
            data.get("hive.metastore.warehouse.dir"),
            Some(&Some("/custom/warehouse".to_string()))
        );
    }

    #[test]
    fn user_override_wins_over_everything() {
        let hive = hive_cluster(DERBY_YAML);
        let merged = MetaStoreConfig::default();
        let db = db_details(&hive);
        let overrides = [("hive.metastore.port".to_string(), "1234".to_string())]
            .into_iter()
            .collect();

        let data = build(
            &hive,
            "default",
            "4.0.0",
            &merged,
            &db,
            None,
            None,
            &cluster_info(),
            overrides,
        )
        .expect("build hive-site");

        assert_eq!(
            data.get("hive.metastore.port"),
            Some(&Some("1234".to_string()))
        );
    }
}

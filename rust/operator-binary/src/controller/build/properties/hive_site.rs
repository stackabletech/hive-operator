//! Builder for `hive-site.xml`.
//!
//! Precedence (matches the pre-product-config-removal behaviour):
//! 1. Defaults: `hive.metastore.port=9083` (required product-config property, default value).
//! 2. Automatic / operator-injected: warehouse dir (hardcoded `/stackable/warehouse`),
//!    metrics enabled (`true`), JDBC driver/url/credentials, S3, Kerberos, OPA.
//! 3. Spec: `warehouseDir` from the merged config overrides the hardcoded warehouse dir.
//! 4. User `hive-site.xml` overrides (highest precedence).
//!
//! All inputs are read from the validated [`ValidatedClusterConfig`]; this builder never
//! touches the raw `HiveCluster` spec.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{crd::s3, k8s_openapi::api::core::v1::EnvVar};

use crate::{controller::ValidatedClusterConfig, crd::MetaStoreConfig};

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

/// Build the `hive-site.xml` key/value pairs.
pub fn build(
    cluster_config: &ValidatedClusterConfig,
    product_version: &str,
    merged_config: &MetaStoreConfig,
    overrides: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let database_connection_details = &cluster_config.metadata_database_connection_details;
    let mut data: BTreeMap<String, String> = BTreeMap::new();

    // 1. Defaults (required product-config property `hive.metastore.port`).
    data.insert(
        HIVE_METASTORE_PORT.to_string(),
        DEFAULT_HIVE_METASTORE_PORT.to_string(),
    );

    // 2. Automatic / operator-injected.
    data.insert(
        MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
        DEFAULT_WAREHOUSE_DIR.to_string(),
    );
    data.insert(
        MetaStoreConfig::METASTORE_METRICS_ENABLED.to_string(),
        "true".to_string(),
    );

    data.insert(
        MetaStoreConfig::CONNECTION_DRIVER_NAME.to_string(),
        cluster_config.connection_driver.clone(),
    );
    data.insert(
        MetaStoreConfig::CONNECTION_URL.to_string(),
        database_connection_details.connection_url.to_string(),
    );
    if let Some(EnvVar { name, .. }) = &database_connection_details.username_env {
        data.insert(
            MetaStoreConfig::CONNECTION_USER_NAME.to_string(),
            format!("${{env:{name}}}"),
        );
    }
    if let Some(EnvVar { name, .. }) = &database_connection_details.password_env {
        data.insert(
            MetaStoreConfig::CONNECTION_PASSWORD.to_string(),
            format!("${{env:{name}}}"),
        );
    }

    if let Some(s3) = cluster_config.s3_connection_spec.as_ref() {
        data.insert(
            MetaStoreConfig::S3_ENDPOINT.to_string(),
            s3.endpoint()
                .context(ConfigureS3ConnectionSnafu)?
                .to_string(),
        );
        data.insert(
            MetaStoreConfig::S3_REGION_NAME.to_string(),
            s3.region.name.clone(),
        );
        if let Some((access_key_file, secret_key_file)) = s3.credentials_mount_paths() {
            data.insert(
                MetaStoreConfig::S3_ACCESS_KEY.to_string(),
                format!("${{file:UTF-8:{access_key_file}}}"),
            );
            data.insert(
                MetaStoreConfig::S3_SECRET_KEY.to_string(),
                format!("${{file:UTF-8:{secret_key_file}}}"),
            );
        }
        data.insert(
            MetaStoreConfig::S3_SSL_ENABLED.to_string(),
            s3.tls.uses_tls().to_string(),
        );
        data.insert(
            MetaStoreConfig::S3_PATH_STYLE_ACCESS.to_string(),
            (s3.access_style == s3::v1alpha1::S3AccessStyle::Path).to_string(),
        );
    }

    // Kerberos entries (resolved during validation; empty when Kerberos is disabled).
    for (name, value) in &cluster_config.kerberos_config {
        data.insert(name.clone(), value.clone());
    }

    if let Some(opa_config) = cluster_config.hive_opa_config.as_ref() {
        data.extend(opa_config.as_config(product_version));
    }

    // 3. Spec: warehouse dir from the merged CRD config (overrides the default).
    if let Some(warehouse_dir) = &merged_config.warehouse_dir {
        data.insert(
            MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
            warehouse_dir.clone(),
        );
    }

    // 4. User overrides (highest precedence).
    for (k, v) in overrides {
        data.insert(k, v);
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::derby_cluster_config;

    #[test]
    fn defaults_present_for_minimal_derby_cluster() {
        let cluster_config = derby_cluster_config();
        let merged = MetaStoreConfig::default();

        let data =
            build(&cluster_config, "4.0.0", &merged, BTreeMap::new()).expect("build hive-site");

        assert_eq!(data.get("hive.metastore.port"), Some(&"9083".to_string()));
        assert_eq!(
            data.get("hive.metastore.metrics.enabled"),
            Some(&"true".to_string())
        );
        assert_eq!(
            data.get("hive.metastore.warehouse.dir"),
            Some(&"/stackable/warehouse".to_string())
        );
        assert!(data.contains_key("javax.jdo.option.ConnectionDriverName"));
        // No env credentials for an embedded Derby database.
        assert!(!data.contains_key("javax.jdo.option.ConnectionUserName"));
    }

    #[test]
    fn warehouse_dir_spec_overrides_default() {
        let cluster_config = derby_cluster_config();
        let merged = MetaStoreConfig {
            warehouse_dir: Some("/custom/warehouse".to_string()),
            ..MetaStoreConfig::default()
        };

        let data =
            build(&cluster_config, "4.0.0", &merged, BTreeMap::new()).expect("build hive-site");

        assert_eq!(
            data.get("hive.metastore.warehouse.dir"),
            Some(&"/custom/warehouse".to_string())
        );
    }

    #[test]
    fn user_override_wins_over_everything() {
        let cluster_config = derby_cluster_config();
        let merged = MetaStoreConfig::default();
        let overrides = [("hive.metastore.port".to_string(), "1234".to_string())]
            .into_iter()
            .collect();

        let data = build(&cluster_config, "4.0.0", &merged, overrides).expect("build hive-site");

        assert_eq!(data.get("hive.metastore.port"), Some(&"1234".to_string()));
    }
}

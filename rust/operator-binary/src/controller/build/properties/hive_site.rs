//! Builder for `hive-site.xml`.
use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{crd::s3, k8s_openapi::api::core::v1::EnvVar};

use crate::{
    controller::{
        ValidatedClusterConfig, ValidatedMetaStoreConfig, build::opa::build_opa_hive_site_config,
    },
    crd::{HIVE_PORT, ICEBERG_REST_CATALOG_PORT, security::IcebergRestCatalogAuthenticationConfig},
};

const HIVE_METASTORE_PORT: &str = "hive.metastore.port";
const METASTORE_THRIFT_BIND_HOST: &str = "hive.metastore.thrift.bind.host";
const METASTORE_CATALOG_SERVLET_PORT: &str = "metastore.catalog.servlet.port";

// Iceberg REST catalog authentication property keys.
const METASTORE_CATALOG_SERVLET_AUTH: &str = "metastore.catalog.servlet.auth";
const METASTORE_CATALOG_SERVLET_AUTH_OAUTH2_ISSUER: &str =
    "metastore.catalog.servlet.auth.oauth2.issuer";
const METASTORE_CATALOG_SERVLET_AUTH_OAUTH2_AUDIENCE: &str =
    "metastore.catalog.servlet.auth.oauth2.audience";
const METASTORE_AUTHENTICATION_JWT_JWKS_URL: &str = "metastore.authentication.jwt.jwks.url";

// Metastore property keys.
const CONNECTION_DRIVER_NAME: &str = "javax.jdo.option.ConnectionDriverName";
const CONNECTION_PASSWORD: &str = "javax.jdo.option.ConnectionPassword";
const CONNECTION_URL: &str = "javax.jdo.option.ConnectionURL";
const CONNECTION_USER_NAME: &str = "javax.jdo.option.ConnectionUserName";
const METASTORE_METRICS_ENABLED: &str = "hive.metastore.metrics.enabled";
const METASTORE_WAREHOUSE_DIR: &str = "hive.metastore.warehouse.dir";
const METASTORE_WAREHOUSE_EXTERNAL_DIR: &str = "hive.metastore.warehouse.external.dir";

// S3 property keys.
const S3_ACCESS_KEY: &str = "fs.s3a.access.key";
const S3_ENDPOINT: &str = "fs.s3a.endpoint";
const S3_PATH_STYLE_ACCESS: &str = "fs.s3a.path.style.access";
const S3_REGION_NAME: &str = "fs.s3a.endpoint.region";
const S3_SECRET_KEY: &str = "fs.s3a.secret.key";
const S3_SSL_ENABLED: &str = "fs.s3a.connection.ssl.enabled";

// Maps the `s3://` scheme onto the s3a filesystem implementation.
const S3_SCHEME_IMPL: &str = "fs.s3.impl";
const S3A_FILE_SYSTEM: &str = "org.apache.hadoop.fs.s3a.S3AFileSystem";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: stackable_operator::crd::s3::v1alpha1::ConnectionError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build(
    cluster_config: &ValidatedClusterConfig,
    product_version: &str,
    _merged_config: &ValidatedMetaStoreConfig,
    kerberos_config: BTreeMap<String, String>,
    overrides: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let database_connection_details = &cluster_config.metadata_database_connection_details;
    let mut data: BTreeMap<String, String> = BTreeMap::new();

    // Defaults.
    data.insert(METASTORE_METRICS_ENABLED.to_string(), "true".to_string());

    // HMS can not disable the Thrift server: it always listens (on 9083 if no port is set) and
    // its main loop is what the background threads (compaction, housekeeping) wait for. If the
    // Thrift API is disabled, we bind it to the loopback interface so it can not be reached from
    // outside the Pod. The embedded Iceberg REST catalog talks to HMS in-process, not via Thrift.
    // Note that the bind host is only honoured by the (default) binary transport mode.
    data.insert(HIVE_METASTORE_PORT.to_string(), HIVE_PORT.to_string());
    if !cluster_config.thrift_api_enabled {
        data.insert(
            METASTORE_THRIFT_BIND_HOST.to_string(),
            "127.0.0.1".to_string(),
        );
    }

    if cluster_config.iceberg_rest_catalog_enabled {
        // The REST catalog is only started for a non-negative port (defaults to -1).
        data.insert(
            METASTORE_CATALOG_SERVLET_PORT.to_string(),
            ICEBERG_REST_CATALOG_PORT.to_string(),
        );

        let auth_type = match &cluster_config.iceberg_rest_catalog_security {
            None => "none",
            Some(IcebergRestCatalogAuthenticationConfig::Simple {}) => "simple",
            Some(IcebergRestCatalogAuthenticationConfig::OAuth2(oauth2)) => {
                data.insert(
                    METASTORE_CATALOG_SERVLET_AUTH_OAUTH2_ISSUER.to_string(),
                    oauth2.issuer_url.to_string(),
                );
                data.insert(
                    METASTORE_CATALOG_SERVLET_AUTH_OAUTH2_AUDIENCE.to_string(),
                    oauth2.audience.to_string(),
                );
                "oauth2"
            }
            Some(IcebergRestCatalogAuthenticationConfig::Jwt(jwt)) => {
                // Hive splits the setting on commas.
                data.insert(
                    METASTORE_AUTHENTICATION_JWT_JWKS_URL.to_string(),
                    jwt.jwks_urls
                        .iter()
                        .map(|url| url.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                );
                "jwt"
            }
        };
        data.insert(
            METASTORE_CATALOG_SERVLET_AUTH.to_string(),
            auth_type.to_string(),
        );
    }

    // Metadata database connection
    data.insert(
        CONNECTION_DRIVER_NAME.to_string(),
        cluster_config.connection_driver.clone(),
    );
    data.insert(
        CONNECTION_URL.to_string(),
        database_connection_details.connection_url.to_string(),
    );
    if let Some(EnvVar { name, .. }) = &database_connection_details.username_env {
        data.insert(CONNECTION_USER_NAME.to_string(), format!("${{env:{name}}}"));
    }
    if let Some(EnvVar { name, .. }) = &database_connection_details.password_env {
        data.insert(CONNECTION_PASSWORD.to_string(), format!("${{env:{name}}}"));
    }

    // S3 connection
    if let Some(s3) = cluster_config.s3_connection_spec.as_ref() {
        data.insert(
            S3_ENDPOINT.to_string(),
            s3.endpoint()
                .context(ConfigureS3ConnectionSnafu)?
                .to_string(),
        );
        data.insert(S3_REGION_NAME.to_string(), s3.region.name.clone());
        if let Some((access_key_file, secret_key_file)) = s3.credentials_mount_paths() {
            data.insert(
                S3_ACCESS_KEY.to_string(),
                format!("${{file:UTF-8:{access_key_file}}}"),
            );
            data.insert(
                S3_SECRET_KEY.to_string(),
                format!("${{file:UTF-8:{secret_key_file}}}"),
            );
        }
        data.insert(S3_SSL_ENABLED.to_string(), s3.tls.uses_tls().to_string());
        data.insert(
            S3_PATH_STYLE_ACCESS.to_string(),
            (s3.access_style == s3::v1alpha1::S3AccessStyle::Path).to_string(),
        );

        // The bundled Hadoop only registers the `s3a://` scheme, so HMS has no filesystem for
        // `s3://` locations. Clients that address data with `s3://` URIs, such as Trino's native
        // S3 Iceberg connector, therefore fail once HMS itself touches the filesystem, e.g. when
        // it creates the schema/table directory. Map the scheme onto the same implementation the
        // `fs.s3a.*` settings above configure; `S3AFileSystem` reads those regardless of the scheme
        // it is mounted under, so `s3a://` locations keep working unchanged.
        //
        // Only the `FileSystem` API is mapped, not `fs.AbstractFileSystem.s3.impl`: HMS resolves
        // filesystems through `FileSystem.get`, and the `FileContext` adapter
        // `org.apache.hadoop.fs.s3a.S3A` hardcoded `s3a` as its only supported scheme before
        // Hadoop 3.3.6, so mapping it would fail on older bundled Hadoop versions.
        data.insert(S3_SCHEME_IMPL.to_string(), S3A_FILE_SYSTEM.to_string());
    }

    // Kerberos entries (empty when Kerberos is disabled).
    data.extend(kerberos_config);

    if let Some(opa_config) = cluster_config.hive_opa_config.as_ref() {
        data.extend(build_opa_hive_site_config(opa_config, product_version));
    }

    // 3. Warehouse dir from the clusterConfig.
    data.insert(
        METASTORE_WAREHOUSE_DIR.to_string(),
        cluster_config.warehouse_dir.clone(),
    );
    if let Some(warehouse_external_dir) = &cluster_config.warehouse_external_dir {
        data.insert(
            METASTORE_WAREHOUSE_EXTERNAL_DIR.to_string(),
            warehouse_external_dir.clone(),
        );
    }

    // 4. User overrides (highest precedence).
    data.extend(overrides);

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        controller::build::properties::test_support::{derby_cluster_config, s3_cluster_config},
        crd::MetaStoreConfig,
    };

    #[test]
    fn defaults_present_for_minimal_derby_cluster() {
        let cluster_config = derby_cluster_config();
        let merged = ValidatedMetaStoreConfig::from_merged_for_test(MetaStoreConfig::default());

        let data = build(
            &cluster_config,
            "4.0.0",
            &merged,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("build hive-site");

        assert_eq!(data.get("hive.metastore.port"), Some(&"9083".to_string()));
        assert_eq!(
            data.get("hive.metastore.metrics.enabled"),
            Some(&"true".to_string())
        );
        assert_eq!(
            data.get("hive.metastore.warehouse.dir"),
            Some(&"s3a://my-bucket/my-warehouse/".to_string())
        );
        assert!(data.contains_key("javax.jdo.option.ConnectionDriverName"));
        // No env credentials for an embedded Derby database.
        assert!(!data.contains_key("javax.jdo.option.ConnectionUserName"));
        // Thrift is reachable from outside the Pod and the REST catalog is not started.
        assert!(!data.contains_key("hive.metastore.thrift.bind.host"));
        assert!(!data.contains_key("metastore.catalog.servlet.port"));
    }

    #[test]
    fn thrift_disabled_binds_to_loopback() {
        let mut cluster_config = derby_cluster_config();
        cluster_config.thrift_api_enabled = false;
        cluster_config.iceberg_rest_catalog_enabled = true;
        let merged = ValidatedMetaStoreConfig::from_merged_for_test(MetaStoreConfig::default());

        let data = build(
            &cluster_config,
            "4.2.0",
            &merged,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("build hive-site");

        assert_eq!(data.get("hive.metastore.port"), Some(&"9083".to_string()));
        assert_eq!(
            data.get("hive.metastore.thrift.bind.host"),
            Some(&"127.0.0.1".to_string())
        );
        assert_eq!(
            data.get("metastore.catalog.servlet.port"),
            Some(&"9001".to_string())
        );
    }

    #[test]
    fn s3_connection_emits_s3a_settings() {
        let cluster_config = s3_cluster_config();
        let merged = ValidatedMetaStoreConfig::from_merged_for_test(MetaStoreConfig::default());

        let data = build(
            &cluster_config,
            "4.0.0",
            &merged,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("build hive-site");

        assert_eq!(
            data.get("fs.s3a.endpoint"),
            Some(&"http://minio:9000/".to_string())
        );
        assert_eq!(
            data.get("fs.s3a.path.style.access"),
            Some(&"true".to_string())
        );
        assert_eq!(
            data.get("fs.s3a.connection.ssl.enabled"),
            Some(&"false".to_string())
        );
    }

    #[test]
    fn s3_connection_registers_the_s3_scheme() {
        let cluster_config = s3_cluster_config();
        let merged = ValidatedMetaStoreConfig::from_merged_for_test(MetaStoreConfig::default());

        let data = build(
            &cluster_config,
            "4.0.0",
            &merged,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("build hive-site");

        assert_eq!(
            data.get("fs.s3.impl"),
            Some(&"org.apache.hadoop.fs.s3a.S3AFileSystem".to_string())
        );
    }

    #[test]
    fn no_s3_connection_leaves_the_s3_scheme_unregistered() {
        let cluster_config = derby_cluster_config();
        let merged = ValidatedMetaStoreConfig::from_merged_for_test(MetaStoreConfig::default());

        let data = build(
            &cluster_config,
            "4.0.0",
            &merged,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("build hive-site");

        assert!(!data.contains_key("fs.s3.impl"));
    }

    #[test]
    fn user_override_wins_over_everything() {
        let cluster_config = derby_cluster_config();
        let merged = ValidatedMetaStoreConfig::from_merged_for_test(MetaStoreConfig::default());
        let overrides = [("hive.metastore.port".to_string(), "1234".to_string())]
            .into_iter()
            .collect();

        let data = build(
            &cluster_config,
            "4.0.0",
            &merged,
            BTreeMap::new(),
            overrides,
        )
        .expect("build hive-site");

        assert_eq!(data.get("hive.metastore.port"), Some(&"1234".to_string()));
    }
}

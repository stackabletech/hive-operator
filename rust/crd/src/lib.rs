use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::role_utils::RoleGroupRef;
use stackable_operator::{
    kube::{runtime::reflector::ObjectRef, CustomResource},
    product_config_utils::{ConfigError, Configuration},
    role_utils::Role,
    schemars::{self, JsonSchema},
};
use std::collections::BTreeMap;

pub const CONFIG_DIR_NAME: &str = "/stackable/conf";
// config file names
pub const HIVE_SITE_XML: &str = "hive-site.xml";
pub const LOG_4J_PROPERTIES: &str = "log4j.properties";

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hive.stackable.tech",
    version = "v1alpha1",
    kind = "HiveCluster",
    plural = "hiveclusters",
    shortname = "hive",
    status = "HiveClusterStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
pub struct HiveClusterSpec {
    /// Emergency stop button, if `true` then all pods are stopped without affecting configuration (as setting `replicas` to `0` would)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metastore: Option<Role<MetaStoreConfig>>,
}

#[derive(strum::Display)]
#[strum(serialize_all = "camelCase")]
pub enum HiveRole {
    #[strum(serialize = "metastore")]
    MetaStore,
}

impl HiveRole {
    /// Returns the container start command for the metastore service.
    pub fn get_command(&self, auto_init_schema: bool, db_type: &str) -> Vec<String> {
        if auto_init_schema {
            vec![
                "bin/start-metastore".to_string(),
                "--config".to_string(),
                CONFIG_DIR_NAME.to_string(),
                "--db-type".to_string(),
                db_type.to_string(),
                "--hive-bin-dir".to_string(),
                "bin".to_string(),
            ]
        } else {
            vec![
                "/bin/hive".to_string(),
                "--config".to_string(),
                CONFIG_DIR_NAME.to_string(),
                "--service".to_string(),
                "metastore".to_string(),
            ]
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaStoreConfig {
    pub metastore_port: Option<u16>,
    pub metrics_port: Option<u16>,
    pub warehouse_dir: Option<String>,
    pub database: DatabaseConnectionSpec,
    pub s3_connection: Option<S3Connection>,
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

impl MetaStoreConfig {
    // metastore
    pub const CONNECTION_URL: &'static str = "javax.jdo.option.ConnectionURL";
    pub const CONNECTION_DRIVER_NAME: &'static str = "javax.jdo.option.ConnectionDriverName";
    pub const CONNECTION_USER_NAME: &'static str = "javax.jdo.option.ConnectionUserName";
    pub const CONNECTION_PASSWORD: &'static str = "javax.jdo.option.ConnectionPassword";
    pub const METASTORE_METRICS_ENABLED: &'static str = "hive.metastore.metrics.enabled";
    pub const METASTORE_WAREHOUSE_DIR: &'static str = "hive.metastore.warehouse.dir";
    pub const DB_TYPE_CLI: &'static str = "dbType";
    // S3
    pub const S3_ENDPOINT: &'static str = "fs.s3a.endpoint";
    pub const S3_ACCESS_KEY: &'static str = "fs.s3a.access.key";
    pub const S3_SECRET_KEY: &'static str = "fs.s3a.secret.key";
    pub const S3_SSL_ENABLED: &'static str = "fs.s3a.connection.ssl.enabled";
    pub const S3_PATH_STYLE_ACCESS: &'static str = "fs.s3a.path.style.access";
    // ports
    pub const METASTORE_PORT_PROPERTY: &'static str = "hive.metastore.port";
    pub const METASTORE_PORT: &'static str = "metastore";
    pub const METRICS_PORT_PROPERTY: &'static str = "metricsPort";
    pub const METRICS_PORT: &'static str = "metrics";
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    JsonSchema,
    PartialEq,
    Serialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
pub enum DbType {
    #[serde(rename = "derby")]
    #[strum(serialize = "derby")]
    Derby,

    #[serde(rename = "mysql")]
    #[strum(serialize = "mysql")]
    Mysql,

    #[serde(rename = "postgres")]
    #[strum(serialize = "postgres")]
    Postgres,

    #[serde(rename = "oracle")]
    #[strum(serialize = "oracle")]
    Oracle,

    #[serde(rename = "mssql")]
    #[strum(serialize = "mssql")]
    Mssql,
}

impl Default for DbType {
    fn default() -> Self {
        Self::Derby
    }
}

impl DbType {
    pub fn get_jdbc_driver_class(&self) -> &str {
        match self {
            DbType::Derby => "org.apache.derby.jdbc.EmbeddedDriver",
            DbType::Mysql => "com.mysql.jdbc.Driver",
            DbType::Postgres => "org.postgresql.Driver",
            DbType::Mssql => "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            DbType::Oracle => "oracle.jdbc.driver.OracleDriver",
        }
    }
}

#[derive(
    Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, Eq, PartialEq, Serialize,
)]
#[kube(
    group = "external.stackable.tech",
    version = "v1alpha1",
    kind = "DatabaseConnection",
    plural = "databaseconnections",
    shortname = "dbconn",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseConnectionSpec {
    pub conn_string: String,
    pub user: String,
    pub password: String,
    pub db_type: DbType,
}

impl Configuration for MetaStoreConfig {
    type Configurable = HiveCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if let Some(metrics_port) = self.metrics_port {
            result.insert(
                Self::METRICS_PORT_PROPERTY.to_string(),
                Some(metrics_port.to_string()),
            );
        }

        Ok(result)
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        result.insert(
            Self::DB_TYPE_CLI.to_string(),
            Some(self.database.db_type.to_string()),
        );
        Ok(result)
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if let Some(metastore_port) = &self.metastore_port {
            result.insert(
                Self::METASTORE_PORT_PROPERTY.to_string(),
                Some(metastore_port.to_string()),
            );
        }
        if let Some(warehouse_dir) = &self.warehouse_dir {
            result.insert(
                Self::METASTORE_WAREHOUSE_DIR.to_string(),
                Some(warehouse_dir.to_string()),
            );
        }
        result.insert(
            Self::CONNECTION_URL.to_string(),
            Some(self.database.conn_string.clone()),
        );
        result.insert(
            Self::CONNECTION_USER_NAME.to_string(),
            Some(self.database.user.clone()),
        );
        result.insert(
            Self::CONNECTION_PASSWORD.to_string(),
            Some(self.database.password.clone()),
        );
        result.insert(
            Self::CONNECTION_DRIVER_NAME.to_string(),
            Some(self.database.db_type.get_jdbc_driver_class().to_string()),
        );

        if self.metrics_port.is_some() {
            result.insert(
                Self::METASTORE_METRICS_ENABLED.to_string(),
                Some("true".to_string()),
            );
        }

        if let Some(s3_connection) = &self.s3_connection {
            result.insert(
                Self::S3_ENDPOINT.to_string(),
                Some(s3_connection.end_point.clone()),
            );

            result.insert(
                Self::S3_ACCESS_KEY.to_string(),
                Some(s3_connection.access_key.clone()),
            );

            result.insert(
                Self::S3_SECRET_KEY.to_string(),
                Some(s3_connection.secret_key.clone()),
            );

            result.insert(
                Self::S3_SSL_ENABLED.to_string(),
                Some(s3_connection.ssl_enabled.to_string()),
            );

            result.insert(
                Self::S3_PATH_STYLE_ACCESS.to_string(),
                Some(s3_connection.path_style_access.to_string()),
            );
        }

        Ok(result)
    }
}

#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HiveClusterStatus {
    /// An opaque value that changes every time a discovery detail does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_hash: Option<String>,
}

#[derive(Debug, Snafu)]
#[snafu(display("object has no namespace associated"))]
pub struct NoNamespaceError;

impl HiveCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn metastore_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// The fully-qualified domain name of the role-level load-balanced Kubernetes `Service`
    pub fn metastore_role_service_fqdn(&self) -> Option<String> {
        Some(format!(
            "{}.{}.svc.cluster.local",
            self.metastore_role_service_name()?,
            self.metadata.namespace.as_ref()?
        ))
    }

    /// Metadata about a metastore rolegroup
    pub fn metastore_rolegroup_ref(
        &self,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<HiveCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: HiveRole::MetaStore.to_string(),
            role_group: group_name.into(),
        }
    }

    /// List all pods expected to form the cluster
    ///
    /// We try to predict the pods here rather than looking at the current cluster state in order to
    /// avoid instance churn. For example, regenerating zoo.cfg based on the cluster state would lead to
    /// a lot of spurious restarts, as well as opening us up to dangerous split-brain conditions because
    /// the pods have inconsistent snapshots of which servers they should expect to be in quorum.
    pub fn pods(&self) -> Result<impl Iterator<Item = PodRef> + '_, NoNamespaceError> {
        let ns = self
            .metadata
            .namespace
            .clone()
            .context(NoNamespaceContext)?;
        Ok(self
            .spec
            .metastore
            .iter()
            .flat_map(|role| &role.role_groups)
            // Order rolegroups consistently, to avoid spurious downstream rewrites
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .flat_map(move |(rolegroup_name, rolegroup)| {
                let rolegroup_ref = self.metastore_rolegroup_ref(rolegroup_name);
                let ns = ns.clone();
                (0..rolegroup.replicas.unwrap_or(0)).map(move |i| PodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                })
            }))
    }
}

/// Reference to a single `Pod` that is a component of a [`HiveCluster`]
///
/// Used for service discovery.
pub struct PodRef {
    pub namespace: String,
    pub role_group_service_name: String,
    pub pod_name: String,
}

impl PodRef {
    pub fn fqdn(&self) -> String {
        format!(
            "{}.{}.{}.svc.cluster.local",
            self.pod_name, self.role_group_service_name, self.namespace
        )
    }
}

#[cfg(test)]
mod tests {}

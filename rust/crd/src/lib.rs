pub mod commands;
pub mod discovery;
pub mod error;

use crate::commands::{Restart, Start, Stop};

use crate::discovery::S3Connection;
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use stackable_operator::command::{CommandRef, HasCommands, HasRoleRestartOrder};
use stackable_operator::controller::HasOwned;
use stackable_operator::crd::HasApplication;
use stackable_operator::identity::PodToNodeMapping;
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use stackable_operator::k8s_openapi::schemars::_serde_json::Value;
use stackable_operator::kube::api::ApiResource;
use stackable_operator::kube::CustomResource;
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::schemars::{self, JsonSchema};
use stackable_operator::status::{
    ClusterExecutionStatus, Conditions, HasClusterExecutionStatus, HasCurrentCommand, Status,
    Versioned,
};
use stackable_operator::versioning::{ProductVersion, Versioning, VersioningState};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use strum_macros::Display;
use strum_macros::EnumIter;

pub const APP_NAME: &str = "hive";
pub const MANAGED_BY: &str = "hive-operator";

pub const CONFIG_DIR_NAME: &str = "/stackable/conf";

// config file names
pub const HIVE_SITE_XML: &str = "hive-site.xml";
pub const LOG_4J_PROPERTIES: &str = "log4j.properties";

// metastore
pub const CONNECTION_URL: &str = "javax.jdo.option.ConnectionURL";
pub const CONNECTION_DRIVER_NAME: &str = "javax.jdo.option.ConnectionDriverName";
pub const CONNECTION_USER_NAME: &str = "javax.jdo.option.ConnectionUserName";
pub const CONNECTION_PASSWORD: &str = "javax.jdo.option.ConnectionPassword";
pub const METASTORE_METRICS_ENABLED: &str = "hive.metastore.metrics.enabled";
pub const METASTORE_WAREHOUSE_DIR: &str = "hive.metastore.warehouse.dir";
pub const DB_TYPE_CLI: &str = "dbType";

// S3
pub const S3_ENDPOINT: &str = "fs.s3a.endpoint";
pub const S3_ACCESS_KEY: &str = "fs.s3a.access.key";
pub const S3_SECRET_KEY: &str = "fs.s3a.secret.key";
pub const S3_SSL_ENABLED: &str = "fs.s3a.connection.ssl.enabled";
pub const S3_PATH_STYLE_ACCESS: &str = "fs.s3a.path.style.access";

// ports
pub const METASTORE_PORT_PROPERTY: &str = "hive.metastore.port";
pub const METASTORE_PORT: &str = "metastore";
pub const METRICS_PORT_PROPERTY: &str = "metricsPort";
pub const METRICS_PORT: &str = "metrics";

pub const JAVA_HOME: &str = "JAVA_HOME";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hive.stackable.tech",
    version = "v1alpha1",
    kind = "HiveCluster",
    plural = "hiveclusters",
    shortname = "hive",
    status = "HiveClusterStatus",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
pub struct HiveClusterSpec {
    pub version: HiveVersion,
    pub metastore: Role<MetaStoreConfig>,
}

#[derive(
    Clone, Debug, Deserialize, Display, EnumIter, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
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

impl Status<HiveClusterStatus> for HiveCluster {
    fn status(&self) -> &Option<HiveClusterStatus> {
        &self.status
    }
    fn status_mut(&mut self) -> &mut Option<HiveClusterStatus> {
        &mut self.status
    }
}

impl HasRoleRestartOrder for HiveCluster {
    fn get_role_restart_order() -> Vec<String> {
        vec![HiveRole::MetaStore.to_string()]
    }
}

impl HasCommands for HiveCluster {
    fn get_command_types() -> Vec<ApiResource> {
        vec![
            Start::api_resource(),
            Stop::api_resource(),
            Restart::api_resource(),
        ]
    }
}

impl HasOwned for HiveCluster {
    fn owned_objects() -> Vec<&'static str> {
        vec![Restart::crd_name(), Start::crd_name(), Stop::crd_name()]
    }
}

impl HasApplication for HiveCluster {
    fn get_application_name() -> &'static str {
        APP_NAME
    }
}

impl HasClusterExecutionStatus for HiveCluster {
    fn cluster_execution_status(&self) -> Option<ClusterExecutionStatus> {
        self.status
            .as_ref()
            .and_then(|status| status.cluster_execution_status.clone())
    }

    fn cluster_execution_status_patch(&self, execution_status: &ClusterExecutionStatus) -> Value {
        json!({ "clusterExecutionStatus": execution_status })
    }
}

// TODO: These all should be "Property" Enums that can be either simple or complex where complex allows forcing/ignoring errors and/or warnings
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaStoreConfig {
    metastore_port: Option<u16>,
    metrics_port: Option<u16>,
    warehouse_dir: Option<String>,
    database: DatabaseConnectionSpec,
    s3_connection: Option<S3Connection>,
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

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Eq, PartialEq, Serialize)]
#[kube(
    group = "external.stackable.tech",
    version = "v1alpha1",
    kind = "DatabaseConnection",
    plural = "databaseconnections",
    shortname = "dbconn",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
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
                METRICS_PORT_PROPERTY.to_string(),
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
            DB_TYPE_CLI.to_string(),
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
                METASTORE_PORT_PROPERTY.to_string(),
                Some(metastore_port.to_string()),
            );
        }
        if let Some(warehouse_dir) = &self.warehouse_dir {
            result.insert(
                METASTORE_WAREHOUSE_DIR.to_string(),
                Some(warehouse_dir.to_string()),
            );
        }
        result.insert(
            CONNECTION_URL.to_string(),
            Some(self.database.conn_string.clone()),
        );
        result.insert(
            CONNECTION_USER_NAME.to_string(),
            Some(self.database.user.clone()),
        );
        result.insert(
            CONNECTION_PASSWORD.to_string(),
            Some(self.database.password.clone()),
        );
        result.insert(
            CONNECTION_DRIVER_NAME.to_string(),
            Some(self.database.db_type.get_jdbc_driver_class().to_string()),
        );

        if self.metrics_port.is_some() {
            result.insert(
                METASTORE_METRICS_ENABLED.to_string(),
                Some("true".to_string()),
            );
        }

        if let Some(s3_connection) = &self.s3_connection {
            result.insert(
                S3_ENDPOINT.to_string(),
                Some(s3_connection.end_point.clone()),
            );

            result.insert(
                S3_ACCESS_KEY.to_string(),
                Some(s3_connection.access_key.clone()),
            );

            result.insert(
                S3_SECRET_KEY.to_string(),
                Some(s3_connection.secret_key.clone()),
            );

            result.insert(
                S3_SSL_ENABLED.to_string(),
                Some(s3_connection.ssl_enabled.to_string()),
            );

            result.insert(
                S3_PATH_STYLE_ACCESS.to_string(),
                Some(s3_connection.path_style_access.to_string()),
            );
        }

        Ok(result)
    }
}

#[allow(non_camel_case_types)]
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
pub enum HiveVersion {
    #[serde(rename = "2.3.9")]
    #[strum(serialize = "2.3.9")]
    v2_3_9,

    // TODO: we currently only support 2.3.9.
    //   remove #[serde(skip)] once it is supported and packaged.
    #[serde(skip)]
    #[serde(rename = "3.1.1")]
    #[strum(serialize = "3.1.1")]
    v3_1_1,
}

impl Versioning for HiveVersion {
    fn versioning_state(&self, other: &Self) -> VersioningState {
        let from_version = match Version::parse(&self.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    self.to_string(),
                    e.to_string()
                ))
            }
        };

        let to_version = match Version::parse(&other.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    other.to_string(),
                    e.to_string()
                ))
            }
        };

        match to_version.cmp(&from_version) {
            Ordering::Greater => VersioningState::ValidUpgrade,
            Ordering::Less => VersioningState::ValidDowngrade,
            Ordering::Equal => VersioningState::NoOp,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HiveClusterStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<ProductVersion<HiveVersion>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<PodToNodeMapping>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_command: Option<CommandRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_execution_status: Option<ClusterExecutionStatus>,
}

impl Versioned<HiveVersion> for HiveClusterStatus {
    fn version(&self) -> &Option<ProductVersion<HiveVersion>> {
        &self.version
    }
    fn version_mut(&mut self) -> &mut Option<ProductVersion<HiveVersion>> {
        &mut self.version
    }
}

impl Conditions for HiveClusterStatus {
    fn conditions(&self) -> &[Condition] {
        self.conditions.as_slice()
    }
    fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.conditions
    }
}

impl HasCurrentCommand for HiveClusterStatus {
    fn current_command(&self) -> Option<CommandRef> {
        self.current_command.clone()
    }
    fn set_current_command(&mut self, command: CommandRef) {
        self.current_command = Some(command);
    }
    fn clear_current_command(&mut self) {
        self.current_command = None
    }
    fn tracking_location() -> &'static str {
        "/status/currentCommand"
    }
}

#[cfg(test)]
mod tests {
    use crate::HiveVersion;
    use stackable_operator::versioning::{Versioning, VersioningState};
    use std::str::FromStr;

    #[test]
    fn test_hive_version_versioning() {
        assert_eq!(
            HiveVersion::v2_3_9.versioning_state(&HiveVersion::v3_1_1),
            VersioningState::ValidUpgrade
        );
        assert_eq!(
            HiveVersion::v3_1_1.versioning_state(&HiveVersion::v2_3_9),
            VersioningState::ValidDowngrade
        );
        assert_eq!(
            HiveVersion::v2_3_9.versioning_state(&HiveVersion::v2_3_9),
            VersioningState::NoOp
        );
    }

    #[test]
    #[test]
    fn test_version_conversion() {
        HiveVersion::from_str("2.3.9").unwrap();
        HiveVersion::from_str("1.2.3").unwrap_err();
    }
}

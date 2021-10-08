pub mod commands;
pub mod error;

use crate::commands::{Restart, Start, Stop};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use k8s_openapi::schemars::_serde_json::Value;
use kube::api::ApiResource;
use kube::CustomResource;
use kube::CustomResourceExt;
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use stackable_operator::command::{CommandRef, HasCommands, HasRoleRestartOrder};
use stackable_operator::controller::HasOwned;
use stackable_operator::crd::HasApplication;
use stackable_operator::identity::PodToNodeMapping;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
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

pub const CONFIG_DIR_NAME: &str = "conf";

pub const HIVE_SITE_XML: &str = "hive-site.xml";
pub const LOG_4J_PROPERTIES: &str = "log4j.properties";

pub const CONNECTION_URL: &str = "javax.jdo.option.ConnectionURL";
pub const CONNECTION_DRIVER_NAME: &str = "javax.jdo.option.ConnectionDriverName";
pub const CONNECTION_USER_NAME: &str = "javax.jdo.option.ConnectionUserName";
pub const CONNECTION_PASSWORD: &str = "javax.jdo.option.ConnectionPassword";

pub const METASTORE_PORT_PROPERTY: &str = "hive.metastore.port";
pub const METASTORE_PORT: &str = "metastore";
pub const METRICS_PORT_PROPERTY: &str = "metricsPort";
pub const METRICS_PORT: &str = "metrics";

pub const DB_TYPE_CLI: &str = "dbType";

pub const JAVA_HOME: &str = "JAVA_HOME";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hive.stackable.tech",
    version = "v1alpha1",
    kind = "HiveCluster",
    plural = "hiveclusters",
    shortname = "hive",
    namespaced
)]
#[kube(status = "HiveClusterStatus")]
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
    /// # Arguments
    ///
    /// * `version` - Current specified hive version
    pub fn get_command(
        &self,
        version: &HiveVersion,
        auto_init_schema: bool,
        db_type: &str,
    ) -> Vec<String> {
        if auto_init_schema {
            vec![
                format!("{}/stackable/bin/start-metastore", version.package_name()),
                "--config".to_string(),
                format!("{{{{configroot}}}}/{}", CONFIG_DIR_NAME),
                "--db-type".to_string(),
                db_type.to_string(),
                "--hive-bin-dir".to_string(),
                format!("{{{{packageroot}}}}/{}/bin/", version.package_name()),
            ]
        } else {
            vec![
                format!("{}/bin/hive", version.package_name()),
                "--config".to_string(),
                format!("{{{{configroot}}}}/{}", CONFIG_DIR_NAME),
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
    database: DatabaseConnectionSpec,
    java_home: String,
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
    namespaced
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

        result.insert(JAVA_HOME.to_string(), Some(self.java_home.clone()));

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
        result.insert(DB_TYPE_CLI.to_string(), Some(self.database.db_type.to_string()));
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

    #[serde(rename = "3.1.1")]
    #[strum(serialize = "3.1.1")]
    v3_1_1,
}

impl HiveVersion {
    pub fn package_name(&self) -> String {
        format!("apache-hive-{}-bin", self.to_string())
    }
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

    #[test]
    fn test_package_name() {
        assert_eq!(
            HiveVersion::v2_3_9.package_name(),
            format!("apache-hive-{}-bin", HiveVersion::v2_3_9.to_string())
        );
    }
}

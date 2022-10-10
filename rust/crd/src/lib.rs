use indoc::formatdoc;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::{
    commons::{
        resources::{CpuLimits, MemoryLimits, NoRuntimeLimits, PvcConfig, Resources},
        s3::S3ConnectionDef,
    },
    config::merge::Merge,
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::{runtime::reflector::ObjectRef, CustomResource},
    product_config_utils::{ConfigError, Configuration},
    role_utils::{Role, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use std::collections::BTreeMap;
use strum::{Display, EnumString};

pub const APP_NAME: &str = "hive";
pub const RESOURCE_MANAGER_HIVE_CONTROLLER: &str = "hive-operator-hive-controller";

pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_RW_CONFIG_DIR: &str = "/stackable/rwconfig";
// config file names
pub const HIVE_SITE_XML: &str = "hive-site.xml";
pub const HIVE_ENV_SH: &str = "hive-env.sh";
pub const LOG_4J_PROPERTIES: &str = "log4j.properties";
// default ports
pub const HIVE_PORT_NAME: &str = "hive";
pub const HIVE_PORT: u16 = 9083;
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: u16 = 9084;
// Certificates and trust stores
pub const SYSTEM_TRUST_STORE: &str = "/etc/pki/java/cacerts";
pub const SYSTEM_TRUST_STORE_PASSWORD: &str = "changeit";
pub const STACKABLE_TRUST_STORE: &str = "/stackable/truststore.p12";
pub const STACKABLE_TRUST_STORE_PASSWORD: &str = "changeit";
pub const CERTS_DIR: &str = "/stackable/certificates/";
// metastore opts
pub const HIVE_METASTORE_HADOOP_OPTS: &str = "HIVE_METASTORE_HADOOP_OPTS";
// heap
pub const HADOOP_HEAPSIZE: &str = "HADOOP_HEAPSIZE";
pub const JVM_HEAP_FACTOR: f32 = 0.8;

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3ConnectionDef>,
    /// Specify the type of the created kubernetes service.
    /// This attribute will be removed in a future release when listener-operator is finished.
    /// Use with caution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_type: Option<ServiceType>,
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
                STACKABLE_RW_CONFIG_DIR.to_string(),
                "--db-type".to_string(),
                db_type.to_string(),
                "--hive-bin-dir".to_string(),
                "bin".to_string(),
            ]
        } else {
            vec![
                "/bin/hive".to_string(),
                "--config".to_string(),
                STACKABLE_RW_CONFIG_DIR.to_string(),
                "--service".to_string(),
                "metastore".to_string(),
            ]
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Merge, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HiveStorageConfig {
    #[serde(default)]
    pub data: PvcConfig,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaStoreConfig {
    pub warehouse_dir: Option<String>,
    #[serde(default)]
    pub database: DatabaseConnectionSpec,
    pub resources: Option<Resources<HiveStorageConfig, NoRuntimeLimits>>,
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

    fn default_resources() -> Resources<HiveStorageConfig, NoRuntimeLimits> {
        Resources {
            cpu: CpuLimits {
                min: Some(Quantity("200m".to_owned())),
                max: Some(Quantity("4".to_owned())),
            },
            memory: MemoryLimits {
                limit: Some(Quantity("2Gi".to_owned())),
                runtime_limits: NoRuntimeLimits {},
            },
            storage: HiveStorageConfig {
                data: PvcConfig {
                    capacity: Some(Quantity("2Gi".to_owned())),
                    storage_class: None,
                    selectors: None,
                },
            },
        }
    }
}

// TODO: Temporary solution until listener-operator is finished
#[derive(Clone, Debug, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum ServiceType {
    NodePort,
    ClusterIP,
}

impl Default for ServiceType {
    fn default() -> Self {
        Self::NodePort
    }
}

#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize, Display, EnumString,
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

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
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

        result.insert(
            HIVE_METASTORE_HADOOP_OPTS.to_string(),
            Some(formatdoc! {"
                    -javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={METRICS_PORT}:/stackable/jmx/jmx_hive_config.yaml
                    -Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}
                    -Djavax.net.ssl.trustStorePassword={STACKABLE_TRUST_STORE_PASSWORD}
                    -Djavax.net.ssl.trustStoreType=pkcs12"}
                )
            );

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

        result.insert(
            Self::METASTORE_METRICS_ENABLED.to_string(),
            Some("true".to_string()),
        );

        Ok(result)
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
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
    /// avoid instance churn.
    pub fn pods(&self) -> Result<impl Iterator<Item = PodRef> + '_, NoNamespaceError> {
        let ns = self.metadata.namespace.clone().context(NoNamespaceSnafu)?;
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

    /// Retrieve and merge resource configs for role and role groups
    pub fn resolve_resource_config_for_role_and_rolegroup(
        &self,
        role: &HiveRole,
        rolegroup_ref: &RoleGroupRef<HiveCluster>,
    ) -> Option<Resources<HiveStorageConfig, NoRuntimeLimits>> {
        // Initialize the result with all default values as baseline
        let conf_defaults = MetaStoreConfig::default_resources();

        let role = match role {
            HiveRole::MetaStore => self.spec.metastore.as_ref()?,
        };

        // Retrieve role resource config
        let mut conf_role: Resources<HiveStorageConfig, NoRuntimeLimits> =
            role.config.config.resources.clone().unwrap_or_default();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup: Resources<HiveStorageConfig, NoRuntimeLimits> = role
            .role_groups
            .get(&rolegroup_ref.role_group)
            .and_then(|rg| rg.config.config.resources.clone())
            .unwrap_or_default();

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        tracing::debug!("Merged resource config: {:?}", conf_rolegroup);
        Some(conf_rolegroup)
    }
}

/// Reference to a single `Pod` that is a component of a [`HiveCluster`]
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

use std::{collections::BTreeMap, str::FromStr};

use indoc::formatdoc;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        cluster_operation::ClusterOperation,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            PvcConfig, PvcConfigFragment, Resources, ResourcesFragment,
        },
        s3::S3ConnectionDef,
    },
    config::{fragment, fragment::Fragment, fragment::ValidationError, merge::Merge},
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::{runtime::reflector::ObjectRef, CustomResource, ResourceExt},
    product_config_utils::{ConfigError, Configuration},
    product_logging::{
        self,
        framework::{create_vector_shutdown_file_command, remove_vector_shutdown_file_command},
        spec::Logging,
    },
    role_utils::{GenericRoleConfig, Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    time::Duration,
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use crate::affinity::get_affinity;

pub mod affinity;

pub const APP_NAME: &str = "hive";
// directories
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_CONFIG_DIR_NAME: &str = "config";
pub const STACKABLE_CONFIG_MOUNT_DIR: &str = "/stackable/mount/config";
pub const STACKABLE_CONFIG_MOUNT_DIR_NAME: &str = "config-mount";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const STACKABLE_LOG_DIR_NAME: &str = "log";
pub const STACKABLE_LOG_CONFIG_MOUNT_DIR: &str = "/stackable/mount/log-config";
pub const STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME: &str = "log-config-mount";
// config file names
pub const HIVE_SITE_XML: &str = "hive-site.xml";
pub const HIVE_ENV_SH: &str = "hive-env.sh";
pub const HIVE_LOG4J2_PROPERTIES: &str = "hive-log4j2.properties";
pub const JVM_SECURITY_PROPERTIES_FILE: &str = "security.properties";

// default ports
pub const HIVE_PORT_NAME: &str = "hive";
pub const HIVE_PORT: u16 = 9083;
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: u16 = 9084;
// certificates and trust stores
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

const DEFAULT_METASTORE_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(5);

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("no metastore role configuration provided"))]
    MissingMetaStoreRole,

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("the role {role} is not defined"))]
    CannotRetrieveHiveRole { role: String },

    #[snafu(display("the role group {role_group} is not defined"))]
    CannotRetrieveHiveRoleGroup { role_group: String },

    #[snafu(display("unknown role {role}. Should be one of {roles:?}"))]
    UnknownHiveRole {
        source: strum::ParseError,
        role: String,
        roles: Vec<String>,
    },
}

/// A Hive cluster stacklet. This resource is managed by the Stackable operator for Apache Hive.
/// Find more information on how to use it and the resources that the operator generates in the
/// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/hive/).
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
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
    /// Hive metastore settings that affect all roles and role groups.
    /// The settings in the `clusterConfig` are cluster wide settings that do not need to be configurable at role or role group level.
    pub cluster_config: HiveClusterConfig,

    // no doc - docs in ClusterOperation struct.
    #[serde(default)]
    pub cluster_operation: ClusterOperation,

    // no doc - docs in ProductImage struct.
    pub image: ProductImage,

    // no doc - docs in Role struct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metastore: Option<Role<MetaStoreConfigFragment>>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HiveClusterConfig {
    // no doc - docs in DatabaseConnectionSpec struct.
    pub database: DatabaseConnectionSpec,

    /// HDFS connection specification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hdfs: Option<HdfsConnection>,

    /// S3 connection specification. This can be either `inline` or a `reference` to an
    /// S3Connection object. Read the [S3 concept documentation](DOCS_BASE_URL_PLACEHOLDER/concepts/s3) to learn more.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3ConnectionDef>,

    /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
    /// to learn how to configure log aggregation with Vector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,

    /// This field controls which type of Service the Operator creates for this HiveCluster:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    ///
    /// * external-stable: Use a LoadBalancer service
    ///
    /// This is a temporary solution with the goal to keep yaml manifests forward compatible.
    /// In the future, this setting will control which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html)
    /// will be used to expose the service, and ListenerClass names will stay the same, allowing for a non-breaking change.
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,
}

// TODO: Temporary solution until listener-operator is finished
#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CurrentlySupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    ClusterInternal,
    #[serde(rename = "external-unstable")]
    ExternalUnstable,
    #[serde(rename = "external-stable")]
    ExternalStable,
}

impl CurrentlySupportedListenerClasses {
    pub fn k8s_service_type(&self) -> String {
        match self {
            CurrentlySupportedListenerClasses::ClusterInternal => "ClusterIP".to_string(),
            CurrentlySupportedListenerClasses::ExternalUnstable => "NodePort".to_string(),
            CurrentlySupportedListenerClasses::ExternalStable => "LoadBalancer".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsConnection {
    /// Name of the [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery)
    /// providing information about the HDFS cluster.
    /// See also the [Stackable Operator for HDFS](DOCS_BASE_URL_PLACEHOLDER/hdfs/) to learn
    /// more about setting up an HDFS cluster.
    pub config_map: String,
}

#[derive(Display, EnumString, EnumIter)]
#[strum(serialize_all = "camelCase")]
pub enum HiveRole {
    #[strum(serialize = "metastore")]
    MetaStore,
}

impl HiveRole {
    /// Returns the container start command for the metastore service.
    pub fn get_command(&self, db_type: &str) -> String {
        formatdoc! {"
            {COMMON_BASH_TRAP_FUNCTIONS}
            {remove_vector_shutdown_file_command}
            prepare_signal_handlers
            bin/start-metastore --config {STACKABLE_CONFIG_DIR} --db-type {db_type} --hive-bin-dir bin &
            wait_for_termination $!
            {create_vector_shutdown_file_command}
            ",
            remove_vector_shutdown_file_command =
                remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            create_vector_shutdown_file_command =
                create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        }
    }

    /// Metadata about a rolegroup
    pub fn rolegroup_ref(
        &self,
        hive: &HiveCluster,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<HiveCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(hive),
            role: self.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Container {
    Hive,
    Vector,
}

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct MetastoreStorageConfig {
    /// This field is deprecated. It was never used by Hive and will be removed in a future
    /// CRD version. The controller will warn if it's set to a non zero value.
    #[fragment_attrs(serde(default))]
    pub data: PvcConfig,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct MetaStoreConfig {
    /// The location of default database for the Hive warehouse.
    /// Maps to the `hive.metastore.warehouse.dir` setting.
    pub warehouse_dir: Option<String>,

    #[fragment_attrs(serde(default))]
    pub resources: Resources<MetastoreStorageConfig, NoRuntimeLimits>,

    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,

    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    #[fragment_attrs(serde(default))]
    pub graceful_shutdown_timeout: Option<Duration>,
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

    fn default_config(cluster_name: &str, role: &HiveRole) -> MetaStoreConfigFragment {
        MetaStoreConfigFragment {
            warehouse_dir: None,
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1000m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("512Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: MetastoreStorageConfigFragment {
                    data: PvcConfigFragment {
                        capacity: Some(Quantity("0Mi".to_owned())), // "0Mi" is a marker for us, so we don't warn unnecessarily
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
            graceful_shutdown_timeout: Some(DEFAULT_METASTORE_GRACEFUL_SHUTDOWN_TIMEOUT),
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

/// Database connection specification for the metadata database.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseConnectionSpec {
    /// A connection string for the database. For example:
    /// `jdbc:postgresql://hivehdfs-postgresql:5432/hivehdfs`
    pub conn_string: String,

    /// The database user.
    pub user: String,

    /// The password for the database user.
    pub password: String,

    /// The type of database to connect to. Supported are:
    /// `postgres`, `mysql`, `oracle`, `mssql` and `derby`.
    /// This value is used to configure the jdbc driver class.
    pub db_type: DbType,
}

impl Configuration for MetaStoreConfigFragment {
    type Configurable = HiveCluster;

    fn compute_env(
        &self,
        _hive: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        result.insert(
            HIVE_METASTORE_HADOOP_OPTS.to_string(),
            Some(formatdoc! {"
                    -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/jmx_hive_config.yaml
                    -Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}
                    -Djavax.net.ssl.trustStorePassword={STACKABLE_TRUST_STORE_PASSWORD}
                    -Djavax.net.ssl.trustStoreType=pkcs12
                    -Djava.security.properties={STACKABLE_CONFIG_DIR}/{JVM_SECURITY_PROPERTIES_FILE}"}
                )
            );

        Ok(result)
    }

    fn compute_cli(
        &self,
        hive: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        result.insert(
            MetaStoreConfig::DB_TYPE_CLI.to_string(),
            Some(hive.spec.cluster_config.database.db_type.to_string()),
        );
        Ok(result)
    }

    fn compute_files(
        &self,
        hive: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        match file {
            HIVE_SITE_XML => {
                if let Some(warehouse_dir) = &self.warehouse_dir {
                    result.insert(
                        MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
                        Some(warehouse_dir.to_string()),
                    );
                }
                result.insert(
                    MetaStoreConfig::CONNECTION_URL.to_string(),
                    Some(hive.spec.cluster_config.database.conn_string.clone()),
                );
                result.insert(
                    MetaStoreConfig::CONNECTION_USER_NAME.to_string(),
                    Some(hive.spec.cluster_config.database.user.clone()),
                );
                result.insert(
                    MetaStoreConfig::CONNECTION_PASSWORD.to_string(),
                    Some(hive.spec.cluster_config.database.password.clone()),
                );
                result.insert(
                    MetaStoreConfig::CONNECTION_DRIVER_NAME.to_string(),
                    Some(
                        hive.spec
                            .cluster_config
                            .database
                            .db_type
                            .get_jdbc_driver_class()
                            .to_string(),
                    ),
                );

                result.insert(
                    MetaStoreConfig::METASTORE_METRICS_ENABLED.to_string(),
                    Some("true".to_string()),
                );
            }
            HIVE_ENV_SH => {}
            _ => {}
        }

        Ok(result)
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HiveClusterStatus {
    /// An opaque value that changes every time a discovery detail does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_hash: Option<String>,
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for HiveCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("object has no namespace associated"))]
pub struct NoNamespaceError;

impl HiveCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn metastore_role_service_name(&self) -> Option<&str> {
        self.metadata.name.as_deref()
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

    pub fn role(&self, role_variant: &HiveRole) -> Result<&Role<MetaStoreConfigFragment>, Error> {
        match role_variant {
            HiveRole::MetaStore => self.spec.metastore.as_ref(),
        }
        .with_context(|| CannotRetrieveHiveRoleSnafu {
            role: role_variant.to_string(),
        })
    }

    pub fn rolegroup(
        &self,
        rolegroup_ref: &RoleGroupRef<HiveCluster>,
    ) -> Result<RoleGroup<MetaStoreConfigFragment>, Error> {
        let role_variant =
            HiveRole::from_str(&rolegroup_ref.role).with_context(|_| UnknownHiveRoleSnafu {
                role: rolegroup_ref.role.to_owned(),
                roles: HiveRole::roles(),
            })?;

        let role = self.role(&role_variant)?;
        role.role_groups
            .get(&rolegroup_ref.role_group)
            .with_context(|| CannotRetrieveHiveRoleGroupSnafu {
                role_group: rolegroup_ref.role_group.to_owned(),
            })
            .cloned()
    }

    pub fn role_config(&self, role: &HiveRole) -> Option<&GenericRoleConfig> {
        match role {
            HiveRole::MetaStore => self.spec.metastore.as_ref().map(|m| &m.role_config),
        }
    }

    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &HiveRole,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> Result<MetaStoreConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = MetaStoreConfig::default_config(&self.name_any(), role);

        // Retrieve role resource config
        let role = self.role(role)?;
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let role_group = self.rolegroup(rolegroup_ref)?;
        let mut conf_role_group = role_group.config.config;

        if let Some(RoleGroup {
            selector: Some(selector),
            ..
        }) = role.role_groups.get(&rolegroup_ref.role_group)
        {
            // Migrate old `selector` attribute, see ADR 26 affinities.
            // TODO Can be removed after support for the old `selector` field is dropped.
            #[allow(deprecated)]
            conf_role_group.affinity.add_legacy_selector(selector);
        }

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_role_group.merge(&conf_role);

        tracing::debug!("Merged config: {:?}", conf_role_group);
        fragment::validate(conf_role_group).context(FragmentValidationFailureSnafu)
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

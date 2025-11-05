use std::{collections::BTreeMap, str::FromStr};

use security::AuthenticationConfig;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        cluster_operation::ClusterOperation,
        opa::OpaConfig,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            PvcConfig, PvcConfigFragment, Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::Merge,
    },
    crd::s3,
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::{CustomResource, ResourceExt, runtime::reflector::ObjectRef},
    product_config_utils::{self, Configuration},
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition},
    utils::cluster_info::KubernetesClusterInfo,
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};
use v1alpha1::HiveMetastoreRoleConfig;

use crate::{crd::affinity::get_affinity, listener::metastore_default_listener_class};

pub mod affinity;
pub mod security;

pub const APP_NAME: &str = "hive";

// Directories
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_CONFIG_DIR_NAME: &str = "config";
pub const STACKABLE_CONFIG_MOUNT_DIR: &str = "/stackable/mount/config";
pub const STACKABLE_CONFIG_MOUNT_DIR_NAME: &str = "config-mount";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const STACKABLE_LOG_DIR_NAME: &str = "log";
pub const STACKABLE_LOG_CONFIG_MOUNT_DIR: &str = "/stackable/mount/log-config";
pub const STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME: &str = "log-config-mount";

// Config file names
pub const CORE_SITE_XML: &str = "core-site.xml";
pub const HIVE_SITE_XML: &str = "hive-site.xml";
pub const HIVE_METASTORE_LOG4J2_PROPERTIES: &str = "metastore-log4j2.properties";
pub const JVM_SECURITY_PROPERTIES_FILE: &str = "security.properties";

// Default ports
pub const HIVE_PORT_NAME: &str = "hive";
pub const HIVE_PORT: u16 = 9083;
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: u16 = 9084;

// Certificates and trust stores
pub const STACKABLE_TRUST_STORE: &str = "/stackable/truststore.p12";
pub const STACKABLE_TRUST_STORE_PASSWORD: &str = "changeit";

// DB credentials
pub const DB_USERNAME_PLACEHOLDER: &str = "xxx_db_username_xxx";
pub const DB_PASSWORD_PLACEHOLDER: &str = "xxx_db_password_xxx";
pub const DB_USERNAME_ENV: &str = "DB_USERNAME_ENV";
pub const DB_PASSWORD_ENV: &str = "DB_PASSWORD_ENV";

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

#[versioned(
    version(name = "v1alpha1"),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned"
    )
)]
pub mod versioned {
    /// A Hive cluster stacklet. This resource is managed by the Stackable operator for Apache Hive.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/hive/).
    #[versioned(crd(
        group = "hive.stackable.tech",
        plural = "hiveclusters",
        shortname = "hive",
        status = "HiveClusterStatus",
        namespaced
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HiveClusterSpec {
        /// Hive metastore settings that affect all roles and role groups.
        /// The settings in the `clusterConfig` are cluster wide settings that do not need to be configurable at role or role group level.
        pub cluster_config: v1alpha1::HiveClusterConfig,

        // no doc - docs in ClusterOperation struct.
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        // no doc - docs in ProductImage struct.
        pub image: ProductImage,

        // no doc - docs in Role struct.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub metastore:
            Option<Role<MetaStoreConfigFragment, HiveMetastoreRoleConfig, JavaCommonConfig>>,
    }

    // TODO: move generic version to op-rs?
    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HiveMetastoreRoleConfig {
        #[serde(flatten)]
        pub common: GenericRoleConfig,

        /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose the coordinator.
        #[serde(default = "metastore_default_listener_class")]
        pub listener_class: String,
    }

    #[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HiveClusterConfig {
        /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/usage-guide/security).
        pub authentication: Option<AuthenticationConfig>,

        /// Authorization options for Hive.
        /// Learn more in the [Hive authorization usage guide](DOCS_BASE_URL_PLACEHOLDER/hive/usage-guide/security#authorization).
        pub authorization: Option<security::AuthorizationConfig>,

        // no doc - docs in DatabaseConnectionSpec struct.
        pub database: DatabaseConnectionSpec,

        /// HDFS connection specification.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub hdfs: Option<HdfsConnection>,

        /// S3 connection specification. This can be either `inline` or a `reference` to an
        /// S3Connection object. Read the [S3 concept documentation](DOCS_BASE_URL_PLACEHOLDER/concepts/s3) to learn more.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub s3: Option<s3::v1alpha1::InlineConnectionOrReference>,

        /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
        /// to learn how to configure log aggregation with Vector.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<String>,
    }
}

impl Default for v1alpha1::HiveMetastoreRoleConfig {
    fn default() -> Self {
        v1alpha1::HiveMetastoreRoleConfig {
            listener_class: metastore_default_listener_class(),
            common: Default::default(),
        }
    }
}

impl HasStatusCondition for v1alpha1::HiveCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl v1alpha1::HiveCluster {
    /// Metadata about a metastore rolegroup
    pub fn metastore_rolegroup_ref(&self, group_name: impl Into<String>) -> RoleGroupRef<Self> {
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

    pub fn role(
        &self,
        role_variant: &HiveRole,
    ) -> Result<&Role<MetaStoreConfigFragment, HiveMetastoreRoleConfig, JavaCommonConfig>, Error>
    {
        match role_variant {
            HiveRole::MetaStore => self.spec.metastore.as_ref(),
        }
        .with_context(|| CannotRetrieveHiveRoleSnafu {
            role: role_variant.to_string(),
        })
    }

    /// The name of the role-listener provided for a specific role.
    /// returns a name `<cluster>-<role>`
    pub fn role_listener_name(&self, hive_role: &HiveRole) -> String {
        format!("{name}-{role}", name = self.name_any(), role = hive_role)
    }

    pub fn rolegroup(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> Result<RoleGroup<MetaStoreConfigFragment, JavaCommonConfig>, Error> {
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

    pub fn role_config(&self, role: &HiveRole) -> Option<&HiveMetastoreRoleConfig> {
        match role {
            HiveRole::MetaStore => self.spec.metastore.as_ref().map(|m| &m.role_config),
        }
    }

    pub fn has_kerberos_enabled(&self) -> bool {
        self.kerberos_secret_class().is_some()
    }

    pub fn kerberos_secret_class(&self) -> Option<String> {
        self.spec
            .cluster_config
            .authentication
            .as_ref()
            .map(|a| &a.kerberos)
            .map(|k| k.secret_class.clone())
    }

    pub fn db_type(&self) -> &DbType {
        &self.spec.cluster_config.database.db_type
    }

    pub fn get_opa_config(&self) -> Option<&OpaConfig> {
        self.spec
            .cluster_config
            .authorization
            .as_ref()
            .and_then(|a| a.opa.as_ref())
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
    /// Metadata about a rolegroup
    pub fn rolegroup_ref(
        &self,
        hive: &v1alpha1::HiveCluster,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<v1alpha1::HiveCluster> {
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

    /// A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM.
    /// We only have one role and will use "hive" everywhere (which e.g. differs from the current hdfs implementation).
    pub fn kerberos_service_name(&self) -> &'static str {
        "hive"
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
    pub const CONNECTION_DRIVER_NAME: &'static str = "javax.jdo.option.ConnectionDriverName";
    pub const CONNECTION_PASSWORD: &'static str = "javax.jdo.option.ConnectionPassword";
    // metastore
    pub const CONNECTION_URL: &'static str = "javax.jdo.option.ConnectionURL";
    pub const CONNECTION_USER_NAME: &'static str = "javax.jdo.option.ConnectionUserName";
    pub const METASTORE_METRICS_ENABLED: &'static str = "hive.metastore.metrics.enabled";
    pub const METASTORE_WAREHOUSE_DIR: &'static str = "hive.metastore.warehouse.dir";
    pub const S3_ACCESS_KEY: &'static str = "fs.s3a.access.key";
    // S3
    pub const S3_ENDPOINT: &'static str = "fs.s3a.endpoint";
    pub const S3_PATH_STYLE_ACCESS: &'static str = "fs.s3a.path.style.access";
    pub const S3_REGION_NAME: &'static str = "fs.s3a.endpoint.region";
    pub const S3_SECRET_KEY: &'static str = "fs.s3a.secret.key";
    pub const S3_SSL_ENABLED: &'static str = "fs.s3a.connection.ssl.enabled";

    fn default_config(cluster_name: &str, role: &HiveRole) -> MetaStoreConfigFragment {
        MetaStoreConfigFragment {
            warehouse_dir: None,
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1000m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("768Mi".to_owned())),
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
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseConnectionSpec {
    /// A connection string for the database. For example:
    /// `jdbc:postgresql://hivehdfs-postgresql:5432/hivehdfs`
    pub conn_string: String,

    /// The type of database to connect to. Supported are:
    /// `postgres`, `mysql`, `oracle`, `mssql` and `derby`.
    /// This value is used to configure the jdbc driver class.
    pub db_type: DbType,

    /// A reference to a Secret containing the database credentials.
    /// The Secret needs to contain the keys `username` and `password`.
    pub credentials_secret: String,
}

impl Configuration for MetaStoreConfigFragment {
    type Configurable = v1alpha1::HiveCluster;

    fn compute_env(
        &self,
        _hive: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        // Well product-config strikes again...
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _hive: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        hive: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        let mut result = BTreeMap::new();

        if file == HIVE_SITE_XML {
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
            // use a placeholder that will be replaced in the start command (also for the password)
            result.insert(
                MetaStoreConfig::CONNECTION_USER_NAME.to_string(),
                Some(DB_USERNAME_PLACEHOLDER.into()),
            );
            result.insert(
                MetaStoreConfig::CONNECTION_PASSWORD.to_string(),
                Some(DB_PASSWORD_PLACEHOLDER.into()),
            );
            result.insert(
                MetaStoreConfig::CONNECTION_DRIVER_NAME.to_string(),
                Some(hive.db_type().get_jdbc_driver_class().to_string()),
            );

            result.insert(
                MetaStoreConfig::METASTORE_METRICS_ENABLED.to_string(),
                Some("true".to_string()),
            );
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

#[derive(Debug, Snafu)]
#[snafu(display("object has no namespace associated"))]
pub struct NoNamespaceError;

/// Reference to a single `Pod` that is a component of a [`HiveCluster`]
/// Used for service discovery.
pub struct PodRef {
    pub namespace: String,
    pub role_group_service_name: String,
    pub pod_name: String,
}

impl PodRef {
    pub fn fqdn(&self, cluster_info: &KubernetesClusterInfo) -> String {
        format!(
            "{pod_name}.{service_name}.{namespace}.svc.{cluster_domain}",
            pod_name = self.pod_name,
            service_name = self.role_group_service_name,
            namespace = self.namespace,
            cluster_domain = cluster_info.cluster_domain
        )
    }
}

use std::str::FromStr;

use databases::MetadataDatabaseConnection;
/// Re-export of the shared product-logging spec data types (test-only).
#[cfg(test)]
pub use product_logging::spec::{
    ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice, CustomContainerLogConfig,
};
use security::AuthenticationConfig;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
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
    config::{fragment::Fragment, merge::Merge},
    crd::s3,
    deep_merger::ObjectOverrides,
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::CustomResource,
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, Role, RoleGroup},
    schemars::{self, JsonSchema},
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition},
    v2::{
        config_overrides::KeyValueConfigOverrides,
        role_utils::JavaCommonConfig,
        types::{
            common::Port,
            kubernetes::{
                ConfigMapName, ContainerName, ListenerClassName, SecretClassName, VolumeName,
            },
        },
    },
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString};
use v1alpha1::HiveMetastoreRoleConfig;

use crate::crd::affinity::get_affinity;

pub mod affinity;
pub mod databases;
pub mod security;

pub const FIELD_MANAGER: &str = "hive-operator";
pub const APP_NAME: &str = "hive";

// Directory mount paths
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_CONFIG_MOUNT_DIR: &str = "/stackable/mount/config";
pub const STACKABLE_LOG_CONFIG_MOUNT_DIR: &str = "/stackable/mount/log-config";

// Volume names for the directories above
stackable_operator::constant!(pub STACKABLE_CONFIG_DIR_NAME: VolumeName = "config");
stackable_operator::constant!(pub STACKABLE_CONFIG_MOUNT_DIR_NAME: VolumeName = "config-mount");
stackable_operator::constant!(pub STACKABLE_LOG_DIR_NAME: VolumeName = "log");
stackable_operator::constant!(pub STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME: VolumeName = "log-config-mount");

// Default ports
pub const HIVE_PORT_NAME: &str = "hive";
pub const HIVE_PORT: Port = Port(9083);
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: Port = Port(9084);

// Certificates and trust stores
pub const STACKABLE_TRUST_STORE: &str = "/stackable/truststore.p12";
pub const STACKABLE_TRUST_STORE_PASSWORD: &str = "changeit";

// Listener defaults
pub const DEFAULT_LISTENER_CLASS: &str = "cluster-internal";

// used by crds to define a default listener_class name
pub fn metastore_default_listener_class() -> ListenerClassName {
    ListenerClassName::from_str(DEFAULT_LISTENER_CLASS)
        .expect("the default listener class is a valid listener class name")
}

const DEFAULT_METASTORE_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(5);

pub type HiveRoleType = Role<
    MetaStoreConfigFragment,
    v1alpha1::HiveConfigOverrides,
    v1alpha1::HiveMetastoreRoleConfig,
    JavaCommonConfig,
>;

pub type HiveRoleGroupType =
    RoleGroup<MetaStoreConfigFragment, JavaCommonConfig, v1alpha1::HiveConfigOverrides>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("the role {role} is not defined"))]
    CannotRetrieveHiveRole { role: String },
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

        // no doc - docs in ObjectOverrides struct.
        #[serde(default)]
        pub object_overrides: ObjectOverrides,

        // no doc - docs in ProductImage struct.
        pub image: ProductImage,

        // no doc - docs in Role struct.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub metastore: Option<HiveRoleType>,
    }

    // TODO: move generic version to op-rs?
    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HiveMetastoreRoleConfig {
        #[serde(flatten)]
        pub common: GenericRoleConfig,

        /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose the metastore.
        #[serde(default = "metastore_default_listener_class")]
        pub listener_class: ListenerClassName,
    }

    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HiveClusterConfig {
        /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/hive/usage-guide/security).
        pub authentication: Option<AuthenticationConfig>,

        /// Authorization options for Hive.
        /// Learn more in the [Hive authorization usage guide](DOCS_BASE_URL_PLACEHOLDER/hive/usage-guide/security#authorization).
        pub authorization: Option<security::AuthorizationConfig>,

        /// Configure the database where the Hive metastore stores all it's internal metadata, such
        /// as schema and tables.
        pub metadata_database: MetadataDatabaseConnection,

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
        pub vector_aggregator_config_map_name: Option<ConfigMapName>,
    }

    #[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, Merge, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HiveConfigOverrides {
        #[serde(default, rename = "hive-site.xml")]
        pub hive_site_xml: KeyValueConfigOverrides,

        #[serde(default, rename = "security.properties")]
        pub security_properties: KeyValueConfigOverrides,
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
    pub fn role_config(&self, role: &HiveRole) -> Option<&HiveMetastoreRoleConfig> {
        match role {
            HiveRole::MetaStore => self.spec.metastore.as_ref().map(|m| &m.role_config),
        }
    }

    pub fn kerberos_secret_class(&self) -> Option<SecretClassName> {
        self.spec
            .cluster_config
            .authentication
            .as_ref()
            .map(|a| &a.kerberos)
            .map(|k| k.secret_class.clone())
    }

    pub fn get_opa_config(&self) -> Option<&OpaConfig> {
        self.spec
            .cluster_config
            .authorization
            .as_ref()
            .and_then(|a| a.opa.as_ref())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsConnection {
    /// Name of the [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery)
    /// providing information about the HDFS cluster.
    /// See also the [Stackable Operator for HDFS](DOCS_BASE_URL_PLACEHOLDER/hdfs/) to learn
    /// more about setting up an HDFS cluster.
    pub config_map: ConfigMapName,
}

#[derive(Clone, Debug, Display, EnumString, EnumIter, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[strum(serialize_all = "camelCase")]
pub enum HiveRole {
    #[strum(serialize = "metastore")]
    MetaStore,
}

impl HiveRole {
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

impl Container {
    /// The type-safe container name for this variant (matching its kebab-case serialization).
    pub fn to_container_name(&self) -> ContainerName {
        ContainerName::from_str(&self.to_string())
            .expect("a Container variant name is a valid container name")
    }
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
    pub(crate) fn default_config(cluster_name: &str, role: &HiveRole) -> MetaStoreConfigFragment {
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

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HiveClusterStatus {
    /// An opaque value that changes every time a discovery detail does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_hash: Option<String>,
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

#[cfg(test)]
mod tests {
    use stackable_operator::versioned::test_utils::RoundtripTestData;

    use super::v1alpha1;

    impl RoundtripTestData for v1alpha1::HiveClusterSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              - image:
                  productVersion: 1.2.3
                  pullPolicy: IfNotPresent
                clusterOperation:
                  stopped: false
                  reconciliationPaused: true
                clusterConfig:
                  authentication:
                    kerberos:
                      secretClass: my-kerberos
                  authorization:
                    opa:
                      configMapName: opa
                      package: hms
                  metadataDatabase:
                    postgresql:
                      host: postgresql
                      database: hive
                      credentialsSecretName: hive-credentials
                  s3:
                    reference: minio
                  hdfs:
                    configMap: hdfs
                  vectorAggregatorConfigMapName: vector-aggregator-discovery
                metastore:
                  config:
                    logging:
                      enableVectorAgent: true
                      containers:
                        hive:
                          console:
                            level: INFO
                          file:
                            level: INFO
                          loggers:
                            ROOT:
                              level: INFO
                        vector:
                          console:
                            level: INFO
                          file:
                            level: INFO
                          loggers:
                            ROOT:
                              level: INFO
                    resources:
                      cpu:
                        min: 400m
                        max: "4"
                      memory:
                        limit: 4Gi
                  podOverrides:
                    spec:
                      containers:
                        - name: vector
                          volumeMounts:
                            - name: prepared-logs
                              mountPath: /stackable/log/prepared-logs
                      volumes:
                        - name: prepared-logs
                          configMap:
                            name: prepared-logs
                  configOverrides:
                    hive-site.xml:
                      hive.metastore.warehouse.dir: /stackable/warehouse/override
                      common-var: role-value
                      role-var: role-value
                  envOverrides:
                    COMMON_VAR: role-value
                    ROLE_VAR: role-value
                  roleGroups:
                    default:
                      replicas: 1
                      config:
                        logging:
                          enableVectorAgent: true
                          containers:
                            hive:
                              custom:
                                configMap: hive-log-config
                      configOverrides:
                        hive-site.xml:
                          common-var: group-value
                          group-var: group-value
                      envOverrides:
                        COMMON_VAR: group-value
                        GROUP_VAR: group-value
        "#})
            .expect("Failed to parse HiveClusterSpec YAML")
        }
    }
}

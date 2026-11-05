use std::{ops::Deref, str::FromStr};

use databases::MetadataDatabaseConnection;
/// Re-export of the shared product-logging spec data types (test-only).
#[cfg(test)]
pub use product_logging::spec::{
    ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice, CustomContainerLogConfig,
};
use security::ThriftAuthenticationConfig;
use serde::{Deserialize, Serialize};
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
    constant,
    crd::s3,
    deep_merger::ObjectOverrides,
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::CustomResource,
    product_logging::{self, spec::Logging},
    role_utils::GenericRoleConfig,
    schemars::{self, JsonSchema},
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition},
    v2::{
        config_overrides::KeyValueConfigOverrides,
        role_utils::{JavaCommonConfig, Role, RoleGroup},
        types::{
            common::Port,
            kubernetes::{
                ConfigMapName, ContainerName, ListenerClassName, SecretClassName, VolumeName,
            },
            operator::RoleName,
        },
    },
    versioned::{TrackingFrom, versioned},
};
use strum::{Display, EnumIter};

use crate::crd::{affinity::get_affinity, security::IcebergRestCatalogAuthenticationConfig};

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
constant!(pub STACKABLE_CONFIG_DIR_NAME: VolumeName = "config");
constant!(pub STACKABLE_CONFIG_MOUNT_DIR_NAME: VolumeName = "config-mount");
constant!(pub STACKABLE_LOG_DIR_NAME: VolumeName = "log");
constant!(pub STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME: VolumeName = "log-config-mount");

// Default ports
pub const HIVE_PORT_NAME: &str = "hive";
pub const HIVE_PORT: Port = Port(9083);
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: Port = Port(9084);

// Certificates and trust stores
pub const STACKABLE_TRUST_STORE: &str = "/stackable/truststore.p12";
pub const STACKABLE_TRUST_STORE_PASSWORD: &str = "changeit";

// Listener defaults
constant!(pub DEFAULT_LISTENER_CLASS: ListenerClassName = "cluster-internal");

/// Serde default for `listenerClass`. Kept as a function because `#[serde(default = "...")]`
/// requires a function path.
pub fn metastore_default_listener_class() -> ListenerClassName {
    DEFAULT_LISTENER_CLASS.clone()
}

const DEFAULT_METASTORE_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(5);

constant!(METASTORE_ROLE_NAME: RoleName = "metastore");

pub type HiveRoleType = Role<
    MetaStoreConfigFragment,
    v1alpha2::HiveConfigOverrides,
    v1alpha2::HiveMetastoreRoleConfig,
    JavaCommonConfig,
>;

pub type HiveRoleGroupType =
    RoleGroup<MetaStoreConfigFragment, JavaCommonConfig, v1alpha2::HiveConfigOverrides>;

#[versioned(
    version(name = "v1alpha1"),
    version(name = "v1alpha2"),
    options(k8s(experimental_conversion_tracking)),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned"
    ),
    skip(from) // TODO: Can we only skip between v1alpha1 and v1alpha2 to be future-proof?
)]
pub mod versioned {
    /// A Hive cluster stacklet. This resource is managed by the Stackable operator for Apache Hive.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/hive/).
    #[versioned(crd(
        doc = "A Hive cluster stacklet. This resource is managed by the Stackable operator for Apache Hive.",
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
        #[versioned(nested)]
        pub cluster_config: HiveClusterConfig,

        // no doc - docs in ClusterOperation struct.
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        // no doc - docs in ObjectOverrides struct.
        #[serde(default)]
        pub object_overrides: ObjectOverrides,

        // no doc - docs in ProductImage struct.
        pub image: ProductImage,

        // no doc - docs in Role struct.
        pub metastore: HiveRoleType,
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
        // // FIXME: This should be a #[versioned(removed)], but that's not supported yet
        // #[versioned(deprecated(
        //     in = "v1alpha2",
        //     previous_docs = "Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/hive/usage-guide/security)."
        // ))]
        // /// This field is deprecated, please don't use it! This field will always be converted to `null`.
        // ///
        // /// Configure the Thrift API authentication on `.spec.clusterConfig.thriftApi.authentication`
        // /// instead.
        // pub deprecated_authentication: Option<ThriftAuthenticationConfig>,
        //
        //
        //
        #[versioned(removed(in = "v1alpha2"))]
        /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/hive/usage-guide/security).
        ///
        /// Moved to `.spec.clusterConfig.thriftApi.authentication` in v1alpha2.
        pub authentication: Option<ThriftAuthenticationConfig>,

        /// Authorization options for Hive.
        /// Learn more in the [Hive authorization usage guide](DOCS_BASE_URL_PLACEHOLDER/hive/usage-guide/security#authorization).
        pub authorization: Option<security::AuthorizationConfig>,

        /// Configure the database where the Hive metastore stores all it's internal metadata, such
        /// as schema and tables.
        pub metadata_database: MetadataDatabaseConnection,

        /// Start the Hive metastore Thrift API (enabled by default).
        #[versioned(added(in = "v1alpha2"))]
        #[serde(default)]
        pub thrift_api: ThriftApiConfig,

        /// Experimental support for starting an Iceberg REST catalog server (disabled by default).
        #[versioned(added(in = "v1alpha2"))]
        #[serde(default)]
        pub iceberg_rest_catalog: IcebergRestCatalogConfig,

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

    #[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ThriftApiConfig {
        /// Whether the Thrift API is exposed (defaults to `true`).
        ///
        /// Hive can not switch the Thrift server off, so if disabled it is bound to the loopback
        /// interface and not exposed via the Service and Listener.
        #[serde(default = "default_thrift_api_enabled")]
        enabled: bool,

        /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/hive/usage-guide/security).
        ///
        /// Only Kerberos is supported.
        pub authentication: Option<ThriftAuthenticationConfig>,
    }

    #[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct IcebergRestCatalogConfig {
        /// Whether the Iceberg REST catalog is started (defaults to `false`).
        /// Requires Hive 4.2.0 or newer.
        #[serde(default = "default_iceberg_rest_catalog_enabled")]
        enabled: bool,

        /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/hive/usage-guide/iceberg-rest-catalog#authentication).
        ///
        /// If not specified, the Iceberg REST catalog does not authenticate requests at all.
        pub authentication: Option<IcebergRestCatalogAuthenticationConfig>,
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

fn default_thrift_api_enabled() -> bool {
    true
}

fn default_iceberg_rest_catalog_enabled() -> bool {
    false
}

impl Default for v1alpha2::HiveMetastoreRoleConfig {
    fn default() -> Self {
        v1alpha2::HiveMetastoreRoleConfig {
            listener_class: metastore_default_listener_class(),
            common: Default::default(),
        }
    }
}

impl Default for v1alpha2::ThriftApiConfig {
    fn default() -> Self {
        Self {
            enabled: default_thrift_api_enabled(),
            authentication: None,
        }
    }
}

impl Default for v1alpha2::IcebergRestCatalogConfig {
    fn default() -> Self {
        Self {
            enabled: default_iceberg_rest_catalog_enabled(),
            authentication: None,
        }
    }
}

impl TrackingFrom<v1alpha1::HiveClusterSpec, HiveClusterStatusWithChangedValues>
    for v1alpha2::HiveClusterSpec
{
    // Note: I let stackable-versioned generate the boilerplate and adopted it to our needs.
    // Ideally stackable-versioned eventually supports all changes we need, in which case this can
    // be generated by the macro.
    fn tracking_from(
        old: v1alpha1::HiveClusterSpec,
        status: &mut HiveClusterStatusWithChangedValues,
        parent: &str,
    ) -> Self {
        use stackable_operator::versioned::{ExtractIfExt as _, TrackingStatus as _};

        // We need to model the paths exactly the same way `stackable-versioned` does, so that we
        // we can switch to macro magic later.
        let __sv_thrift_api_path =
            stackable_operator::versioned::jthong_path(parent, "cluster_config.thrift_api");
        let __sv_iceberg_rest_catalog_path = stackable_operator::versioned::jthong_path(
            parent,
            "cluster_config.iceberg_rest_catalog",
        );

        let mut cluster_config = v1alpha2::HiveClusterConfig {
            // // We old keep the deprecated property, as stackable-versioned doesn't support removing
            // // yet. We restore the value from the new location, so the deprecated value doesn't
            // // matter. We set it to null to make this clear.
            // #[expect(deprecated)]
            // deprecated_authentication: None,
            authorization: old.cluster_config.authorization,
            metadata_database: old.cluster_config.metadata_database,
            thrift_api: v1alpha2::ThriftApiConfig {
                enabled: true,
                authentication: old.cluster_config.authentication,
            },
            iceberg_rest_catalog: v1alpha2::IcebergRestCatalogConfig {
                enabled: false,
                authentication: None,
            },
            hdfs: old.cluster_config.hdfs,
            s3: old.cluster_config.s3,
            vector_aggregator_config_map_name: old.cluster_config.vector_aggregator_config_map_name,
        };

        if let Some(upgrades) = status.changes().upgrades.get_mut("v1alpha2") {
            if let Some(stackable_operator::versioned::ChangedValue { value, .. }) =
                upgrades.extract_first_if(|u| u.json_path == __sv_thrift_api_path)
            {
                cluster_config.thrift_api =  ::serde_json::from_value(value).expect("internal error: {__sv_thrift_api_path} must be serializable for spec.thrift_api");
            }
            if let Some(stackable_operator::versioned::ChangedValue { value, .. }) =
                upgrades.extract_first_if(|u| u.json_path == __sv_iceberg_rest_catalog_path)
            {
                cluster_config.iceberg_rest_catalog =  ::serde_json::from_value(value).expect("internal error: {__sv_iceberg_rest_catalog_path} must be serializable for spec.iceberg_rest_catalog");
            }
            if upgrades.is_empty() {
                status.changes().upgrades.remove("v1alpha2");
            }
        }

        Self {
            cluster_config,
            cluster_operation: old.cluster_operation,
            object_overrides: old.object_overrides,
            image: old.image,
            metastore: old.metastore,
        }
    }
}

impl TrackingFrom<v1alpha2::HiveClusterSpec, HiveClusterStatusWithChangedValues>
    for v1alpha1::HiveClusterSpec
{
    fn tracking_from(
        new: v1alpha2::HiveClusterSpec,
        status: &mut HiveClusterStatusWithChangedValues,
        parent: &str,
    ) -> Self {
        use stackable_operator::versioned::TrackingStatus as _;

        // We need to model the paths exactly the same way `stackable-versioned` does, so that we
        // we can switch to macro magic later.
        let __sv_thrift_api_path =
            stackable_operator::versioned::jthong_path(parent, "cluster_config.thrift_api");
        let __sv_iceberg_rest_catalog_path = stackable_operator::versioned::jthong_path(
            parent,
            "cluster_config.iceberg_rest_catalog",
        );

        let upgrades = status
            .changes()
            .upgrades
            .entry("v1alpha2".to_owned())
            .or_default();
        upgrades.push(stackable_operator::versioned::ChangedValue {
            json_path: __sv_thrift_api_path,
            value: ::serde_json::to_value(&new.cluster_config.thrift_api).unwrap(),
            downgraded_value: ::core::option::Option::None,
        });
        upgrades.push(stackable_operator::versioned::ChangedValue {
            json_path: __sv_iceberg_rest_catalog_path,
            value: ::serde_json::to_value(&new.cluster_config.iceberg_rest_catalog).unwrap(),
            downgraded_value: ::core::option::Option::None,
        });

        let cluster_config = v1alpha1::HiveClusterConfig {
            authentication: new.cluster_config.thrift_api.authentication,
            authorization: new.cluster_config.authorization,
            metadata_database: new.cluster_config.metadata_database,
            hdfs: new.cluster_config.hdfs,
            s3: new.cluster_config.s3,
            vector_aggregator_config_map_name: new.cluster_config.vector_aggregator_config_map_name,
        };
        Self {
            cluster_config,
            cluster_operation: new.cluster_operation,
            object_overrides: new.object_overrides,
            image: new.image,
            metastore: new.metastore,
        }
    }
}

impl HasStatusCondition for v1alpha2::HiveCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.status.conditions.clone(),
            None => vec![],
        }
    }
}

impl v1alpha2::HiveCluster {
    pub fn role_config(&self, role: &HiveRole) -> &v1alpha2::HiveMetastoreRoleConfig {
        match role {
            HiveRole::MetaStore => &self.spec.metastore.role_config,
        }
    }

    pub fn kerberos_secret_class(&self) -> Option<SecretClassName> {
        self.spec
            .cluster_config
            .thrift_api
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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum HiveRole {
    MetaStore,
}

impl HiveRole {
    /// A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM.
    /// We only have one role and will use "hive" everywhere (which e.g. differs from the current hdfs implementation).
    pub fn kerberos_service_name(&self) -> &'static str {
        "hive"
    }
}

impl Deref for HiveRole {
    type Target = RoleName;

    fn deref(&self) -> &Self::Target {
        match self {
            HiveRole::MetaStore => &METASTORE_ROLE_NAME,
        }
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

// Typed container names. They must match the strum `Display` (kebab-case) of the variants above,
// which is pinned by a unit test.
constant!(HIVE_CONTAINER_NAME: ContainerName = "hive");
constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");

impl Container {
    /// The typed container name of this variant.
    pub fn name(&self) -> &'static ContainerName {
        match self {
            Container::Hive => &HIVE_CONTAINER_NAME,
            Container::Vector => &VECTOR_CONTAINER_NAME,
        }
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
    pub(crate) fn default_config(
        cluster_name: &str,
        role: &HiveRole,
        opa_config: Option<&OpaConfig>,
    ) -> MetaStoreConfigFragment {
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
            affinity: get_affinity(cluster_name, role, opa_config),
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
    use stackable_operator::versioned::{TrackingInto, test_utils::RoundtripTestData};
    use strum::IntoEnumIterator;

    use super::*;
    use crate::crd::v1alpha2::{IcebergRestCatalogConfig, ThriftApiConfig};

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *METASTORE_ROLE_NAME;
        let _ = *DEFAULT_LISTENER_CLASS;
        let _ = *STACKABLE_CONFIG_DIR_NAME;
        let _ = *STACKABLE_CONFIG_MOUNT_DIR_NAME;
        let _ = *STACKABLE_LOG_DIR_NAME;
        let _ = *STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME;
        let _ = *HIVE_CONTAINER_NAME;
        let _ = *VECTOR_CONTAINER_NAME;
    }

    /// The typed container names returned by `name` must agree with the strum `Display`
    /// of `Container`, which the logging configuration still uses as the per-container key.
    #[test]
    fn container_names_match_display() {
        for container in Container::iter() {
            assert_eq!(container.name().to_string(), container.to_string());
        }
    }

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

    impl RoundtripTestData for v1alpha2::HiveClusterSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              - image:
                  productVersion: 1.2.3
                clusterConfig:
                  metadataDatabase:
                    postgresql:
                      host: postgresql
                      database: hive
                      credentialsSecretName: hive-credentials
                metastore:
                  roleGroups:
                    default:
                      replicas: 1
              - image:
                  productVersion: 1.2.3
                clusterConfig:
                  thriftApi:
                    enabled: true
                    authentication:
                      kerberos:
                        secretClass: my-kerberos
                  icebergRestCatalog:
                    enabled: false
                  metadataDatabase:
                    postgresql:
                      host: postgresql
                      database: hive
                      credentialsSecretName: hive-credentials
                metastore:
                  roleGroups:
                    default:
                      replicas: 1
              - image:
                  productVersion: 1.2.3
                clusterConfig:
                  thriftApi:
                    enabled: true
                    authentication:
                      kerberos:
                        secretClass: my-kerberos
                  icebergRestCatalog:
                    enabled: true # defaults to false
                    authentication:
                      oAuth2:
                        issuerUrl: https://keycloak.my-namespace.svc.cluster.local:8443/realms/test
                        audience: hive-metastore
                  metadataDatabase:
                    postgresql:
                      host: postgresql
                      database: hive
                      credentialsSecretName: hive-credentials
                metastore:
                  roleGroups:
                    default:
                      replicas: 1
        "#})
            .expect("Failed to parse HiveClusterSpec YAML")
        }
    }

    #[test]
    fn cluster_config_v1_to_v2_keeps_thrift_authentication() {
        let mut status = HiveClusterStatusWithChangedValues::default();
        let v1: v1alpha1::HiveClusterSpec =
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              image:
                productVersion: 1.2.3
              clusterConfig:
                authentication:
                  kerberos:
                    secretClass: my-kerberos
                metadataDatabase:
                  postgresql:
                    host: postgresql
                    database: hive
                    credentialsSecretName: hive-credentials
              metastore:
                roleGroups:
                  default:
                    replicas: 1
        "#})
            .expect("Failed to parse HiveClusterSpec YAML");

        let v2: v1alpha2::HiveClusterSpec = v1.clone().tracking_into(&mut status, "$");
        // assert_eq!(v2.cluster_config.deprecated_authentication, None);
        assert_eq!(
            v2.cluster_config.thrift_api,
            ThriftApiConfig {
                enabled: true,
                authentication: v1.cluster_config.authentication
            }
        );
        assert_eq!(
            v2.cluster_config.iceberg_rest_catalog,
            IcebergRestCatalogConfig {
                enabled: false,
                authentication: None
            }
        );
    }
}

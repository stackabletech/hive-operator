//! Ensures that `Pod`s are configured and running for each [`v1alpha1::HiveCluster`]

mod build;
mod dereference;
mod validate;

use std::{collections::BTreeMap, hash::Hasher, str::FromStr, sync::Arc};

use const_format::concatcp;
use fnv::FnvHasher;
use snafu::{ResultExt, Snafu};
pub use stackable_operator::v2::types::operator::RoleGroupName;
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    commons::{
        affinity::StackableAffinity,
        product_image_selection::ResolvedProductImage,
        rbac::build_rbac_resources,
        resources::{NoRuntimeLimits, Resources},
    },
    crd::{listener::v1alpha1::Listener, s3},
    database_connections::drivers::jdbc::JdbcDatabaseConnectionDetails,
    kube::{
        Resource, ResourceExt,
        api::ObjectMeta,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    kvp::Labels,
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        cluster_resources::cluster_resources_new,
        kvp::label::{recommended_labels, role_group_selector},
        role_group_utils::ResourceNames,
        types::{
            kubernetes::{ListenerClassName, ListenerName, SecretClassName},
            operator::{ControllerName, OperatorName, ProductName, ProductVersion, RoleName},
        },
    },
};
use strum::EnumDiscriminants;

use crate::{
    OPERATOR_NAME,
    controller::build::resource::{
        discovery,
        listener::build_role_listener,
        pdb::build_pdb,
        service::{build_rolegroup_headless_service, build_rolegroup_metrics_service},
    },
    crd::{APP_NAME, HdfsConnection, HiveClusterStatus, HiveRole, MetaStoreConfig, v1alpha1},
};

pub const HIVE_CONTROLLER_NAME: &str = "hivecluster";
pub const HIVE_FULL_CONTROLLER_NAME: &str = concatcp!(HIVE_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(strum::IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to apply Service for role group {role_group}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    BuildRoleGroupConfigMap {
        source: build::resource::config_map::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to apply ConfigMap for role group {role_group}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to apply StatefulSet for role group {role_group}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },

    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to apply PodDisruptionBudget"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("HiveCluster object is invalid"))]
    InvalidHiveCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to apply group listener for {role}"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
        role: String,
    },
    #[snafu(display("failed to dereference cluster resources"))]
    Dereference {
        source: crate::controller::dereference::Error,
    },

    #[snafu(display("failed to validate cluster configuration"))]
    Validate { source: validate::Error },

    #[snafu(display("failed to build StatefulSet for role group {role_group}"))]
    BuildRoleGroupStatefulSet {
        source: build::resource::statefulset::Error,
        role_group: RoleGroupName,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

/// A validated, merged Hive metastore role-group config.
pub type HiveRoleGroupConfig = stackable_operator::v2::role_utils::RoleGroupConfig<
    ValidatedMetaStoreConfig,
    stackable_operator::v2::role_utils::JavaCommonConfig,
    v1alpha1::HiveConfigOverrides,
>;

/// A validated Hive metastore config: the merged [`MetaStoreConfig`] with its raw `logging`
/// replaced by the up-front [`ValidatedLogging`](validate::ValidatedLogging) (so an invalid
/// custom log ConfigMap name or a missing Vector aggregator name fails reconciliation during
/// validation).
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedMetaStoreConfig {
    pub affinity: StackableAffinity,
    pub graceful_shutdown_timeout: Option<Duration>,
    pub logging: validate::ValidatedLogging,
    pub resources: Resources<crate::crd::MetastoreStorageConfig, NoRuntimeLimits>,
    pub warehouse_dir: Option<String>,
}

impl ValidatedMetaStoreConfig {
    /// Builds the validated config from the merged [`MetaStoreConfig`], swapping in the
    /// already-validated logging.
    fn from_merged(merged: MetaStoreConfig, logging: validate::ValidatedLogging) -> Self {
        Self {
            warehouse_dir: merged.warehouse_dir,
            resources: merged.resources,
            logging,
            affinity: merged.affinity,
            graceful_shutdown_timeout: merged.graceful_shutdown_timeout,
        }
    }

    /// Wraps a merged [`MetaStoreConfig`] with trivial automatic logging, for builder tests
    /// that only exercise the non-logging parts of the config.
    #[cfg(test)]
    pub(crate) fn from_merged_for_test(merged: MetaStoreConfig) -> Self {
        use stackable_operator::{
            product_logging::spec::AutomaticContainerLogConfig,
            v2::product_logging::framework::ValidatedContainerLogConfigChoice,
        };

        Self::from_merged(
            merged,
            validate::ValidatedLogging {
                hive_container: ValidatedContainerLogConfigChoice::Automatic(
                    AutomaticContainerLogConfig::default(),
                ),
                vector_container: None,
                enable_vector_agent: false,
            },
        )
    }
}

/// The validated cluster: the typed, merged result of the validate step. Subsequent
/// build steps consume this struct instead of re-reading the raw CRD.
///
/// The cluster identity (`name`, `namespace`, `uid`) is captured here so that owner
/// references for child objects can be built straight from this struct
/// (via its [`Resource`] impl) without threading the raw [`v1alpha1::HiveCluster`]
/// around.
pub struct ValidatedCluster {
    /// `ObjectMeta` carrying `name`, `namespace` and `uid`, so this struct can act as the
    /// owner [`Resource`] for child objects.
    metadata: ObjectMeta,
    pub name: stackable_operator::v2::types::operator::ClusterName,
    pub namespace: stackable_operator::v2::types::kubernetes::NamespaceName,
    pub uid: stackable_operator::v2::types::kubernetes::Uid,
    pub image: ResolvedProductImage,
    /// The product version as a valid label value, used for the recommended `app.kubernetes.io/version`
    /// label. Derived from the resolved image's app version label value.
    pub product_version: ProductVersion,
    pub role_config: Option<ValidatedRoleConfig>,
    pub cluster_config: ValidatedClusterConfig,
    pub role_group_configs: BTreeMap<HiveRole, BTreeMap<RoleGroupName, HiveRoleGroupConfig>>,
}

impl ValidatedCluster {
    pub fn new(
        name: stackable_operator::v2::types::operator::ClusterName,
        namespace: stackable_operator::v2::types::kubernetes::NamespaceName,
        uid: stackable_operator::v2::types::kubernetes::Uid,
        image: ResolvedProductImage,
        role_config: Option<ValidatedRoleConfig>,
        cluster_config: ValidatedClusterConfig,
        role_group_configs: BTreeMap<HiveRole, BTreeMap<RoleGroupName, HiveRoleGroupConfig>>,
    ) -> Self {
        // `app_version_label_value` is constructed to be a valid label value, so it is also a
        // valid `ProductVersion`.
        let product_version = ProductVersion::from_str(&image.app_version_label_value)
            .expect("the app version label value is a valid product version");
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            name,
            namespace,
            uid,
            image,
            product_version,
            role_config,
            cluster_config,
            role_group_configs,
        }
    }

    /// The single Hive role name (`metastore`).
    pub fn role_name() -> RoleName {
        RoleName::from_str(&HiveRole::MetaStore.to_string())
            .expect("the metastore role name is a valid role name")
    }

    /// Type-safe names for the resources of a given role group.
    pub(crate) fn resource_names(&self, role_group_name: &RoleGroupName) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: Self::role_name(),
            role_group_name: role_group_name.clone(),
        }
    }

    /// Recommended labels for a role-group resource, using the given product version.
    fn recommended_labels_for(
        &self,
        product_version: &ProductVersion,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            product_version,
            &operator_name(),
            &controller_name(),
            &Self::role_name(),
            role_group_name,
        )
    }

    /// Recommended labels for a role-group resource.
    pub fn recommended_labels(&self, role_group_name: &RoleGroupName) -> Labels {
        self.recommended_labels_for(&self.product_version, role_group_name)
    }

    /// Selector labels matching the pods of a role group.
    pub fn role_group_selector(&self, role_group_name: &RoleGroupName) -> Labels {
        role_group_selector(self, &product_name(), &Self::role_name(), role_group_name)
    }

    /// Whether Kerberos is enabled for this cluster (a Kerberos `SecretClass` was configured).
    pub fn has_kerberos_enabled(&self) -> bool {
        self.cluster_config.kerberos_secret_class.is_some()
    }

    /// The name of the per-role [`Listener`] object.
    ///
    /// Must stay in sync with [`v1alpha1::HiveCluster::role_listener_name`], which derives the
    /// same name from the raw cluster (used e.g. by the StatefulSet listener-volume PVC).
    pub fn role_listener_name(&self, hive_role: &HiveRole) -> ListenerName {
        ListenerName::from_str(&format!(
            "{name}-{role}",
            name = self.name,
            role = hive_role
        ))
        .expect("the role listener name is a valid Listener name")
    }

    /// Returns an [`ObjectMetaBuilder`] pre-filled with the namespace, an owner reference back to
    /// this cluster, and the recommended labels for a resource named `name` in `role_group_name`.
    ///
    /// Consolidates the metadata chain repeated by the child-resource builders. Call sites that
    /// need extra labels/annotations chain them onto the returned builder.
    pub(crate) fn object_meta(
        &self,
        name: impl Into<String>,
        role_group_name: &RoleGroupName,
    ) -> stackable_operator::builder::meta::ObjectMetaBuilder {
        let mut builder = stackable_operator::builder::meta::ObjectMetaBuilder::new();
        builder
            .name_and_namespace(self)
            .name(name)
            .ownerreference(
                stackable_operator::v2::builder::meta::ownerreference_from_resource(
                    self,
                    None,
                    Some(true),
                ),
            )
            .with_labels(self.recommended_labels(role_group_name));
        builder
    }
}

/// Lets [`ValidatedCluster`] stand in for the raw [`v1alpha1::HiveCluster`] when building owner
/// references and metadata for child objects. Kind/group/version are delegated to the CRD; the
/// `metadata` (name, namespace, uid) is captured during validation.
impl Resource for ValidatedCluster {
    type DynamicType = <v1alpha1::HiveCluster as Resource>::DynamicType;
    type Scope = <v1alpha1::HiveCluster as Resource>::Scope;

    fn kind(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HiveCluster::kind(dt)
    }

    fn group(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HiveCluster::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HiveCluster::version(dt)
    }

    fn plural(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HiveCluster::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

impl HasName for ValidatedCluster {
    fn to_name(&self) -> String {
        self.name.to_string()
    }
}

impl HasUid for ValidatedCluster {
    fn to_uid(&self) -> stackable_operator::v2::types::kubernetes::Uid {
        self.uid.clone()
    }
}

impl NameIsValidLabelValue for ValidatedCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

/// The product name (`hive`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("'hive' is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(HIVE_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
}

/// Cluster-wide settings resolved during validation and dereferencing.
///
/// Everything the config-file builders need is resolved here so they never have to
/// read the raw [`v1alpha1::HiveCluster`] spec.
pub struct ValidatedClusterConfig {
    pub metadata_database_connection_details: JdbcDatabaseConnectionDetails,
    /// The resolved JDBC driver class (Derby version special-casing already applied).
    pub connection_driver: String,
    /// The database type passed to Hive via the `--db-type` CLI argument (e.g. `derby`).
    pub db_type: String,
    /// The HDFS connection (discovery ConfigMap reference), if an HDFS backend is configured.
    pub hdfs: Option<HdfsConnection>,
    pub s3_connection_spec: Option<s3::v1alpha1::ConnectionSpec>,
    pub hive_opa_config: Option<dereference::ResolvedOpaConfig>,
    /// The Kerberos `SecretClass` name, if Kerberos is enabled.
    pub kerberos_secret_class: Option<SecretClassName>,
}

impl ValidatedClusterConfig {
    /// Whether a `core-site.xml` with `hadoop.security.authentication=kerberos` is required:
    /// Kerberos is enabled and there is no HDFS backend (i.e. S3).
    pub fn needs_kerberos_core_site(&self) -> bool {
        self.kerberos_secret_class.is_some() && self.hdfs.is_none()
    }
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
    pub listener_class: ListenerClassName,
}

pub async fn reconcile_hive(
    hive: Arc<DeserializeGuard<v1alpha1::HiveCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");
    let hive = hive
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidHiveClusterSnafu)?;
    let client = &ctx.client;

    let dereferenced_objects = crate::controller::dereference::dereference(client, hive)
        .await
        .context(DereferenceSnafu)?;

    let validated_cluster = validate::validate_cluster(
        hive,
        &ctx.operator_environment.image_repository,
        dereferenced_objects,
    )
    .context(ValidateSnafu)?;

    let mut cluster_resources = cluster_resources_new(
        &product_name(),
        &operator_name(),
        &controller_name(),
        &validated_cluster.name,
        &validated_cluster.namespace,
        &validated_cluster.uid,
        ClusterResourceApplyStrategy::from(&hive.spec.cluster_operation),
        &hive.spec.object_overrides,
    );

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        hive,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(GetRequiredLabelsSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    let rbac_sa = cluster_resources
        .add(client, rbac_sa)
        .await
        .context(ApplyServiceAccountSnafu)?;

    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    for (hive_role, role_group_configs) in &validated_cluster.role_group_configs {
        for (role_group_name, rg) in role_group_configs {
            let rg_metrics_service =
                build_rolegroup_metrics_service(&validated_cluster, role_group_name);

            let rg_headless_service =
                build_rolegroup_headless_service(&validated_cluster, role_group_name);

            let rg_configmap = build::resource::config_map::build_metastore_rolegroup_config_map(
                &validated_cluster,
                &client.kubernetes_cluster_info,
                role_group_name,
                rg,
            )
            .with_context(|_| BuildRoleGroupConfigMapSnafu {
                role_group: role_group_name.clone(),
            })?;

            let rg_statefulset =
                build::resource::statefulset::build_metastore_rolegroup_statefulset(
                    hive_role,
                    &validated_cluster,
                    role_group_name,
                    rg,
                    &rbac_sa.name_any(),
                )
                .with_context(|_| BuildRoleGroupStatefulSetSnafu {
                    role_group: role_group_name.clone(),
                })?;

            cluster_resources
                .add(client, rg_metrics_service)
                .await
                .context(ApplyRoleGroupServiceSnafu {
                    role_group: role_group_name.clone(),
                })?;

            cluster_resources
                .add(client, rg_headless_service)
                .await
                .context(ApplyRoleGroupServiceSnafu {
                    role_group: role_group_name.clone(),
                })?;

            cluster_resources.add(client, rg_configmap).await.context(
                ApplyRoleGroupConfigSnafu {
                    role_group: role_group_name.clone(),
                },
            )?;

            // Note: The StatefulSet needs to be applied after all ConfigMaps and Secrets it
            // mounts to prevent unnecessary Pod restarts.
            // See https://github.com/stackabletech/commons-operator/issues/111 for details.
            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .context(ApplyRoleGroupStatefulSetSnafu {
                        role_group: role_group_name.clone(),
                    })?,
            );
        }
    }

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);

    if let Some(role_config) = &validated_cluster.role_config {
        if let Some(pdb) = build_pdb(&role_config.pdb, &validated_cluster, &HiveRole::MetaStore) {
            cluster_resources
                .add(client, pdb)
                .await
                .context(ApplyPdbSnafu)?;
        }

        let role_listener: Listener = build_role_listener(
            &validated_cluster,
            &HiveRole::MetaStore,
            &role_config.listener_class,
        );
        let listener = cluster_resources.add(client, role_listener).await.context(
            ApplyGroupListenerSnafu {
                role: HiveRole::MetaStore.to_string(),
            },
        )?;

        for discovery_cm in discovery::build_discovery_configmaps(
            &validated_cluster,
            HiveRole::MetaStore,
            None,
            listener,
        )
        .await
        .context(BuildDiscoveryConfigSnafu)?
        {
            let discovery_cm = cluster_resources
                .add(client, discovery_cm)
                .await
                .context(ApplyDiscoveryConfigSnafu)?;
            if let Some(generation) = discovery_cm.metadata.resource_version {
                discovery_hash.write(generation.as_bytes())
            }
        }
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hive.spec.cluster_operation);

    let status = HiveClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
        conditions: compute_conditions(hive, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    client
        .apply_patch_status(OPERATOR_NAME, hive, &status)
        .await
        .context(ApplyStatusSnafu)?;

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::HiveCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // An invalid HiveCluster was deserialized. Await for it to change.
        Error::InvalidHiveCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use stackable_operator::utils::yaml_from_str_singleton_map;

    use super::{ValidatedCluster, dereference::DereferencedObjects, validate::validate_cluster};
    use crate::crd::v1alpha1;

    /// Minimal Derby-backed `HiveCluster` fixture shared across the crate's tests.
    ///
    /// Includes a `uid` so owner references can be derived from it.
    pub const DERBY_YAML: &str = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: "4.0.0"
          clusterConfig:
            metadataDatabase:
              derby: {}
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;

    pub fn minimal_hive(yaml: &str) -> v1alpha1::HiveCluster {
        yaml_from_str_singleton_map(yaml).expect("invalid test HiveCluster YAML")
    }

    /// Runs the real validate step against a minimal (S3/OPA-free) fixture.
    pub fn validated_cluster(hive: &v1alpha1::HiveCluster) -> ValidatedCluster {
        validate_cluster(
            hive,
            "oci.example.org",
            DereferencedObjects {
                s3_connection_spec: None,
                hive_opa_config: None,
            },
        )
        .expect("validate should succeed for the test fixture")
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{RoleGroupName, test_support::*};

    #[test]
    fn object_meta_sets_namespace_owner_and_recommended_labels() {
        let hive = minimal_hive(DERBY_YAML);
        let cluster = validated_cluster(&hive);
        let role_group_name = RoleGroupName::from_str("default").expect("valid role group name");

        let meta = cluster.object_meta("test-name", &role_group_name).build();

        assert_eq!(meta.name.as_deref(), Some("test-name"));
        assert_eq!(meta.namespace.as_deref(), Some(cluster.namespace.as_ref()));
        assert!(meta.owner_references.is_some());
        assert!(meta.labels.is_some());
    }
}

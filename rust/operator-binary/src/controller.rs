use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use const_format::concatcp;
use product_config::ProductConfigManager;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        affinity::StackableAffinity,
        product_image_selection::ResolvedProductImage,
        resources::{NoRuntimeLimits, Resources},
    },
    crd::listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{
            ConfigMap, Container as K8sContainer, EnvVar, PersistentVolumeClaim, PodTemplateSpec,
            Service, ServiceAccount, Volume, VolumeMount,
        },
        policy::v1::PodDisruptionBudget,
        rbac::v1::RoleBinding,
    },
    kube::{
        Resource,
        api::ObjectMeta,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    OPERATOR_NAME,
    crd::{APP_NAME, HiveRole, MetastoreStorageConfig, v1alpha1},
    framework::{
        HasName, HasUid, NameIsValidLabelValue,
        product_logging::framework::{ValidatedContainerLogConfigChoice, VectorContainerLogConfig},
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};

pub mod apply;
pub mod build;
pub mod dereference;
pub mod update_status;
pub mod validate;

pub const HIVE_CONTROLLER_NAME: &str = "hivecluster";
pub const CONTAINER_IMAGE_BASE_NAME: &str = "hive";
pub const HIVE_FULL_CONTROLLER_NAME: &str = concatcp!(HIVE_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
    pub operator_environment: OperatorEnvironmentOptions,
}

pub(crate) struct Prepared;
pub(crate) struct Applied;

pub(crate) struct KubernetesResources<T> {
    pub stateful_sets: Vec<StatefulSet>,
    pub config_maps: Vec<ConfigMap>,
    pub services: Vec<Service>,
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    pub listeners: Vec<listener::v1alpha1::Listener>,
    pub discovery_hash: Option<String>,
    pub _status: PhantomData<T>,
}

#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    pub resources: Resources<MetastoreStorageConfig, NoRuntimeLimits>,
    pub logging: ValidatedLogging,
    pub affinity: StackableAffinity,
    pub graceful_shutdown_timeout: Duration,
    pub hive_site_xml_content: String,
    pub jvm_security_properties_content: String,
    pub core_site_xml_content: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb_enabled: bool,
    pub pdb_max_unavailable: u16,
    pub listener_class: String,
    pub listener_name: String,
}

#[derive(Clone)]
pub struct PrecomputedPodData {
    pub env_vars: Vec<EnvVar>,
    pub commands: Vec<String>,
    pub kerberos_volumes: Vec<Volume>,
    pub kerberos_volume_mounts: Vec<VolumeMount>,
    pub s3_volumes: Vec<Volume>,
    pub s3_volume_mounts: Vec<VolumeMount>,
    pub hdfs_volumes: Vec<Volume>,
    pub hdfs_volume_mounts: Vec<VolumeMount>,
    pub opa_volumes: Vec<Volume>,
    pub opa_volume_mounts: Vec<VolumeMount>,
    pub vector_container: Option<K8sContainer>,
    pub service_account_name: String,
    pub replicas: Option<u16>,
    pub pod_overrides: PodTemplateSpec,
    pub listener_volume_claim_template: PersistentVolumeClaim,
}

#[derive(Clone, Debug)]
pub struct ValidatedLogging {
    pub hive_container: ValidatedContainerLogConfigChoice,
    pub vector_container: Option<VectorContainerLogConfig>,
}

impl ValidatedLogging {
    pub fn is_vector_agent_enabled(&self) -> bool {
        self.vector_container.is_some()
    }
}

#[derive(Clone)]
pub struct ValidatedHiveCluster {
    metadata: ObjectMeta,
    pub image: ResolvedProductImage,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    pub role_groups: BTreeMap<HiveRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub precomputed_pod_data: BTreeMap<HiveRole, BTreeMap<String, PrecomputedPodData>>,
    pub role_configs: BTreeMap<HiveRole, ValidatedRoleConfig>,
}

impl ValidatedHiveCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        image: ResolvedProductImage,
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        role_groups: BTreeMap<HiveRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
        precomputed_pod_data: BTreeMap<HiveRole, BTreeMap<String, PrecomputedPodData>>,
        role_configs: BTreeMap<HiveRole, ValidatedRoleConfig>,
    ) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            image,
            name,
            namespace,
            uid,
            role_groups,
            precomputed_pod_data,
            role_configs,
        }
    }
}

impl HasName for ValidatedHiveCluster {
    fn to_name(&self) -> String {
        self.name.to_string()
    }
}

impl HasUid for ValidatedHiveCluster {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl NameIsValidLabelValue for ValidatedHiveCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

impl ValidatedHiveCluster {
    pub fn rolegroup_ref(
        &self,
        role: &HiveRole,
        role_group: &str,
    ) -> stackable_operator::role_utils::RoleGroupRef<Self> {
        stackable_operator::role_utils::RoleGroupRef {
            cluster: stackable_operator::kube::runtime::reflector::ObjectRef::from_obj(self),
            role: role.to_string(),
            role_group: role_group.to_string(),
        }
    }
}

impl Resource for ValidatedHiveCluster {
    type DynamicType = <v1alpha1::HiveCluster as stackable_operator::kube::Resource>::DynamicType;
    type Scope = <v1alpha1::HiveCluster as stackable_operator::kube::Resource>::Scope;

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

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("HiveCluster object is invalid"))]
    InvalidHiveCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to dereference resources"))]
    Dereference { source: dereference::Error },

    #[snafu(display("failed to validate cluster"))]
    Validate { source: validate::Error },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply resources"))]
    Apply { source: apply::Error },

    #[snafu(display("failed to update status"))]
    UpdateStatus { source: update_status::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile(
    hive: Arc<DeserializeGuard<v1alpha1::HiveCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let hive = hive
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidHiveClusterSnafu)?;

    // --- dereference (async, fallible) ---
    let dereferenced = dereference::dereference(
        &ctx.client,
        hive,
        CONTAINER_IMAGE_BASE_NAME,
        &ctx.operator_environment.image_repository,
        crate::built_info::PKG_VERSION,
    )
    .await
    .context(DereferenceSnafu)?;

    // --- validate (sync, fallible) ---
    let validated = validate::validate_cluster(hive, &dereferenced, &ctx.product_config)
        .context(ValidateSnafu)?;

    // --- build (sync, infallible) ---
    let prepared = build::build(&validated);

    // --- apply (async, fallible) ---
    let cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        HIVE_CONTROLLER_NAME,
        &hive.object_ref(&()),
        ClusterResourceApplyStrategy::from(&hive.spec.cluster_operation),
        &hive.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

    let applied = apply::Applier::new(&ctx.client, cluster_resources)
        .apply(prepared, &validated)
        .await
        .context(ApplySnafu)?;

    // --- update status (async, fallible) ---
    update_status::update_status(&ctx.client, hive, applied)
        .await
        .context(UpdateStatusSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::HiveCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidHiveCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(10)),
    }
}

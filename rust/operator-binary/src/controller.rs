//! Ensures that `Pod`s are configured and running for each [`v1alpha1::HiveCluster`]

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    sync::Arc,
};

use const_format::concatcp;
use fnv::FnvHasher;
use indoc::formatdoc;
use product_config::{
    ProductConfigManager,
    types::PropertyNameKind,
    writer::{PropertiesWriterError, to_hadoop_xml, to_java_properties_string},
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::{
                ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
                ListenerReference, VolumeBuilder,
            },
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        product_image_selection::ResolvedProductImage, rbac::build_rbac_resources,
        tls_verification::TlsClientDetailsError,
    },
    crd::{listener::v1alpha1::Listener, s3},
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, Probe, TCPSocketAction,
                Volume,
            },
        },
        apimachinery::pkg::{
            api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
        },
    },
    kube::{
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    kvp::{Labels, ObjectLabels},
    logging::controller::ReconcilerError,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        framework::{
            LoggingError, create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
        },
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::{GenericRoleConfig, RoleGroupRef},
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    time::Duration,
    utils::{COMMON_BASH_TRAP_FUNCTIONS, cluster_info::KubernetesClusterInfo},
};
use strum::EnumDiscriminants;
use tracing::warn;

use crate::{
    OPERATOR_NAME,
    command::build_container_command_args,
    config::jvm::{construct_hadoop_heapsize_env, construct_non_heap_jvm_args},
    crd::{
        APP_NAME, CORE_SITE_XML, Container, DB_PASSWORD_ENV, DB_USERNAME_ENV, HIVE_PORT,
        HIVE_PORT_NAME, HIVE_SITE_XML, HiveClusterStatus, HiveRole, JVM_SECURITY_PROPERTIES_FILE,
        METRICS_PORT, METRICS_PORT_NAME, MetaStoreConfig, STACKABLE_CONFIG_DIR,
        STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_MOUNT_DIR, STACKABLE_CONFIG_MOUNT_DIR_NAME,
        STACKABLE_LOG_CONFIG_MOUNT_DIR, STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME, STACKABLE_LOG_DIR,
        STACKABLE_LOG_DIR_NAME,
        v1alpha1::{self, HiveMetastoreRoleConfig},
    },
    discovery::{self},
    kerberos::{
        self, add_kerberos_pod_config, kerberos_config_properties,
        kerberos_container_start_commands,
    },
    listener::{LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME, build_role_listener},
    operations::{graceful_shutdown::add_graceful_shutdown_config, pdb::add_pdbs},
    product_logging::extend_role_group_config_map,
    service::{
        build_rolegroup_headless_service, build_rolegroup_metrics_service,
        rolegroup_headless_service_name,
    },
};

pub const HIVE_CONTROLLER_NAME: &str = "hivecluster";
pub const HIVE_FULL_CONTROLLER_NAME: &str = concatcp!(HIVE_CONTROLLER_NAME, '.', OPERATOR_NAME);

const DOCKER_IMAGE_BASE_NAME: &str = "hive";

pub const MAX_HIVE_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(strum::IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("object defines no metastore role"))]
    NoMetaStoreRole,

    #[snafu(display("failed to calculate service name for role {rolegroup}"))]
    RoleGroupServiceNameNotFound {
        rolegroup: RoleGroupRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HiveCluster>,
    },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
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

    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: stackable_operator::crd::s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("failed to configure S3 TLS client details"))]
    ConfigureS3TlsClientDetails { source: TlsClientDetailsError },

    #[snafu(display(
        "Hive does not support skipping the verification of the tls enabled S3 server"
    ))]
    S3TlsNoVerificationNotSupported,

    #[snafu(display("failed to resolve and merge resource config for role and role group"))]
    FailedToResolveResourceConfig { source: crate::crd::Error },

    #[snafu(display("invalid java heap config - missing default or value in crd?"))]
    InvalidJavaHeapConfig,

    #[snafu(display("failed to convert java heap config to unit [{unit}]"))]
    FailedToConvertJavaHeap {
        source: stackable_operator::memory::Error,
        unit: String,
    },

    #[snafu(display("failed to create hive container [{name}]"))]
    FailedToCreateHiveContainer {
        source: stackable_operator::builder::pod::container::Error,
        name: String,
    },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
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

    #[snafu(display("internal operator failure"))]
    InternalOperatorError { source: crate::crd::Error },

    #[snafu(display(
        "failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}",
        rolegroup
    ))]
    JvmSecurityPoperties {
        source: PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("failed to build TLS certificate SecretClass Volume"))]
    TlsCertSecretClassVolumeBuild {
        source: stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build S3 credentials SecretClass Volume"))]
    S3CredentialsSecretClassVolumeBuild {
        source: stackable_operator::commons::secret_class::SecretClassVolumeError,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display(
        "there was an error adding LDAP Volumes and VolumeMounts to the Pod and Containers"
    ))]
    AddLdapVolumes {
        source: stackable_operator::crd::authentication::ldap::v1alpha1::Error,
    },

    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig { source: kerberos::Error },

    #[snafu(display("failed to build vector container"))]
    BuildVectorContainer { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("HiveCluster object is invalid"))]
    InvalidHiveCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: crate::config::jvm::Error },

    #[snafu(display("failed to apply group listener for {role}"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
        role: String,
    },
    #[snafu(display("failed to configure listener"))]
    ListenerConfiguration { source: crate::listener::Error },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("faild to configure service"))]
    ServiceConfiguration { source: crate::service::Error },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
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
    let hive_namespace = hive.namespace().context(ObjectHasNoNamespaceSnafu)?;

    let resolved_product_image: ResolvedProductImage = hive
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);
    let role = hive.spec.metastore.as_ref().context(NoMetaStoreRoleSnafu)?;
    let hive_role = HiveRole::MetaStore;

    let s3_connection_spec: Option<s3::v1alpha1::ConnectionSpec> =
        if let Some(s3) = &hive.spec.cluster_config.s3 {
            Some(
                s3.clone()
                    .resolve(
                        client,
                        &hive.namespace().ok_or(Error::ObjectHasNoNamespace)?,
                    )
                    .await
                    .context(ConfigureS3ConnectionSnafu)?,
            )
        } else {
            None
        };

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(
            hive,
            [(
                HiveRole::MetaStore.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::Cli,
                        PropertyNameKind::File(HIVE_SITE_XML.to_string()),
                        PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                    ],
                    role.clone(),
                ),
            )]
            .into(),
        )
        .context(GenerateProductConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let metastore_config = validated_config
        .get(&HiveRole::MetaStore.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        HIVE_CONTROLLER_NAME,
        &hive.object_ref(&()),
        ClusterResourceApplyStrategy::from(&hive.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

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

    for (rolegroup_name, rolegroup_config) in metastore_config.iter() {
        let rolegroup = hive.metastore_rolegroup_ref(rolegroup_name);

        let config = hive
            .merged_config(&HiveRole::MetaStore, &rolegroup)
            .context(FailedToResolveResourceConfigSnafu)?;

        let rg_metrics_service =
            build_rolegroup_metrics_service(hive, &resolved_product_image, &rolegroup)
                .context(ServiceConfigurationSnafu)?;

        let rg_headless_service =
            build_rolegroup_headless_service(hive, &resolved_product_image, &rolegroup)
                .context(ServiceConfigurationSnafu)?;

        let rg_configmap = build_metastore_rolegroup_config_map(
            hive,
            &hive_namespace,
            &resolved_product_image,
            &rolegroup,
            rolegroup_config,
            s3_connection_spec.as_ref(),
            &config,
            &client.kubernetes_cluster_info,
        )?;
        let rg_statefulset = build_metastore_rolegroup_statefulset(
            hive,
            &hive_role,
            &resolved_product_image,
            &rolegroup,
            rolegroup_config,
            s3_connection_spec.as_ref(),
            &config,
            &rbac_sa.name_any(),
        )?;

        cluster_resources
            .add(client, rg_metrics_service)
            .await
            .context(ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;

        cluster_resources
            .add(client, rg_headless_service)
            .await
            .context(ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;

        cluster_resources
            .add(client, rg_configmap)
            .await
            .context(ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup.clone(),
            })?;

        ss_cond_builder.add(
            cluster_resources
                .add(client, rg_statefulset)
                .await
                .context(ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup.clone(),
                })?,
        );
    }

    let role_config = hive.role_config(&hive_role);
    if let Some(HiveMetastoreRoleConfig {
        common: GenericRoleConfig {
            pod_disruption_budget: pdb,
        },
        ..
    }) = role_config
    {
        add_pdbs(pdb, hive, &hive_role, client, &mut cluster_resources)
            .await
            .context(FailedToCreatePdbSnafu)?;
    }

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);

    if let Some(HiveMetastoreRoleConfig { listener_class, .. }) = role_config {
        let role_listener: Listener =
            build_role_listener(hive, &resolved_product_image, &hive_role, listener_class)
                .context(ListenerConfigurationSnafu)?;
        let listener = cluster_resources.add(client, role_listener).await.context(
            ApplyGroupListenerSnafu {
                role: hive_role.to_string(),
            },
        )?;

        for discovery_cm in discovery::build_discovery_configmaps(
            hive,
            hive,
            hive_role,
            &resolved_product_image,
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

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
#[allow(clippy::too_many_arguments)]
fn build_metastore_rolegroup_config_map(
    hive: &v1alpha1::HiveCluster,
    hive_namespace: &str,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>,
    role_group_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    s3_connection_spec: Option<&s3::v1alpha1::ConnectionSpec>,
    merged_config: &MetaStoreConfig,
    cluster_info: &KubernetesClusterInfo,
) -> Result<ConfigMap> {
    let mut hive_site_data = String::new();

    for (property_name_kind, config) in role_group_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HIVE_SITE_XML => {
                let mut data = BTreeMap::new();

                data.insert(
                    MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
                    Some("/stackable/warehouse".to_string()),
                );

                if let Some(s3) = s3_connection_spec {
                    data.insert(
                        MetaStoreConfig::S3_ENDPOINT.to_string(),
                        Some(
                            s3.endpoint()
                                .context(ConfigureS3ConnectionSnafu)?
                                .to_string(),
                        ),
                    );

                    data.insert(
                        MetaStoreConfig::S3_REGION_NAME.to_string(),
                        Some(s3.region.name.clone()),
                    );

                    if let Some((access_key_file, secret_key_file)) = s3.credentials_mount_paths() {
                        // Will be replaced by config-utils
                        data.insert(
                            MetaStoreConfig::S3_ACCESS_KEY.to_string(),
                            Some(format!("${{file:UTF-8:{access_key_file}}}")),
                        );
                        data.insert(
                            MetaStoreConfig::S3_SECRET_KEY.to_string(),
                            Some(format!("${{file:UTF-8:{secret_key_file}}}")),
                        );
                    }

                    data.insert(
                        MetaStoreConfig::S3_SSL_ENABLED.to_string(),
                        Some(s3.tls.uses_tls().to_string()),
                    );
                    data.insert(
                        MetaStoreConfig::S3_PATH_STYLE_ACCESS.to_string(),
                        Some((s3.access_style == s3::v1alpha1::S3AccessStyle::Path).to_string()),
                    );
                }

                for (property_name, property_value) in
                    kerberos_config_properties(hive, hive_namespace, cluster_info)
                {
                    data.insert(property_name.to_string(), Some(property_value.to_string()));
                }

                // overrides
                for (property_name, property_value) in config {
                    data.insert(property_name.to_string(), Some(property_value.to_string()));
                }

                hive_site_data = to_hadoop_xml(data.iter());
            }
            _ => {}
        }
    }

    let jvm_sec_props: BTreeMap<String, Option<String>> = role_group_config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hive)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(hive, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(build_recommended_labels(
                    hive,
                    &resolved_product_image.app_version_label,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(HIVE_SITE_XML, hive_site_data)
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                JvmSecurityPopertiesSnafu {
                    rolegroup: rolegroup.role_group.clone(),
                }
            })?,
        );

    if hive.has_kerberos_enabled() && hive.spec.cluster_config.hdfs.is_none() {
        // if kerberos is activated but we have no HDFS as backend (i.e. S3) then a core-site.xml is
        // needed to set "hadoop.security.authentication"
        let mut data = BTreeMap::new();
        data.insert(
            "hadoop.security.authentication".to_string(),
            Some("kerberos".to_string()),
        );
        cm_builder.add_data(CORE_SITE_XML, to_hadoop_xml(data.iter()));
    }

    extend_role_group_config_map(rolegroup, &merged_config.logging, &mut cm_builder).context(
        InvalidLoggingConfigSnafu {
            cm_name: rolegroup.object_name(),
        },
    )?;

    cm_builder
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the
/// corresponding [`Service`](`stackable_operator::k8s_openapi::api::core::v1::Service`) (via [`build_rolegroup_headless_service`] and metrics from [`build_rolegroup_metrics_service`]).
#[allow(clippy::too_many_arguments)]
fn build_metastore_rolegroup_statefulset(
    hive: &v1alpha1::HiveCluster,
    hive_role: &HiveRole,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HiveCluster>,
    metastore_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    s3_connection: Option<&s3::v1alpha1::ConnectionSpec>,
    merged_config: &MetaStoreConfig,
    sa_name: &str,
) -> Result<StatefulSet> {
    let role = hive.role(hive_role).context(InternalOperatorSnafu)?;
    let rolegroup = hive
        .rolegroup(rolegroup_ref)
        .context(InternalOperatorSnafu)?;

    let mut container_builder =
        ContainerBuilder::new(APP_NAME).context(FailedToCreateHiveContainerSnafu {
            name: APP_NAME.to_string(),
        })?;

    let credentials_secret_name = hive.spec.cluster_config.database.credentials_secret.clone();

    container_builder
        // load database credentials to environment variables: these will be used to replace
        // the placeholders in hive-site.xml so that the operator does not "touch" the secret.
        .add_env_var_from_secret(DB_USERNAME_ENV, &credentials_secret_name, "username")
        .add_env_var_from_secret(DB_PASSWORD_ENV, &credentials_secret_name, "password")
        .add_env_var(
            "HADOOP_HEAPSIZE",
            construct_hadoop_heapsize_env(merged_config).context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            "HADOOP_OPTS",
            construct_non_heap_jvm_args(hive, role, &rolegroup_ref.role_group)
                .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        );

    for (property_name_kind, config) in metastore_config {
        if property_name_kind == &PropertyNameKind::Env {
            // overrides
            for (property_name, property_value) in config {
                if property_name.is_empty() {
                    warn!(
                        property_name,
                        property_value,
                        "The env variable had an empty name, not adding it to the container"
                    );
                    continue;
                }
                container_builder.add_env_var(property_name, property_value);
            }
        }
    }

    let mut pod_builder = PodBuilder::new();

    if let Some(hdfs) = &hive.spec.cluster_config.hdfs {
        pod_builder
            .add_volume(
                VolumeBuilder::new("hdfs-discovery")
                    .with_config_map(&hdfs.config_map)
                    .build(),
            )
            .context(AddVolumeSnafu)?;
        container_builder
            .add_volume_mount("hdfs-discovery", "/stackable/mount/hdfs-config")
            .context(AddVolumeMountSnafu)?;
    }

    if let Some(s3) = s3_connection {
        s3.add_volumes_and_mounts(&mut pod_builder, vec![&mut container_builder])
            .context(ConfigureS3ConnectionSnafu)?;

        if s3.tls.uses_tls() && !s3.tls.uses_tls_verification() {
            S3TlsNoVerificationNotSupportedSnafu.fail()?;
        }
    }

    let db_type = hive.db_type();
    let start_command = if resolved_product_image.product_version.starts_with("3.") {
        // The schematool version in 3.1.x does *not* support the `-initOrUpgradeSchema` flag yet, so we can not use that.
        // As we *only* support HMS 3.1.x (or newer) since SDP release 23.11, we can safely assume we are always coming
        // from an existing 3.1.x installation. There is no need to upgrade the schema, we can just check if the schema
        // is already there and create it if it isn't.
        // The script `bin/start-metastore` is buggy (e.g. around version upgrades), but it's sufficient for that job :)
        //
        // TODO: Once we drop support for HMS 3.1.x we can remove this condition and very likely get rid of the
        // "bin/start-metastore" script.
        format!(
            "bin/start-metastore --config {STACKABLE_CONFIG_DIR} --db-type {db_type} --hive-bin-dir bin &"
        )
    } else {
        // schematool versions 4.0.x (and above) support the `-initOrUpgradeSchema`, which is exactly what we need :)
        // Some docs for the schemaTool can be found here: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=34835119
        formatdoc! {"
            bin/base --config \"{STACKABLE_CONFIG_DIR}\" --service schemaTool -dbType \"{db_type}\" -initOrUpgradeSchema
            bin/base --config \"{STACKABLE_CONFIG_DIR}\" --service metastore &
        "}
    };

    let container_builder = container_builder
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(build_container_command_args(
            hive,
            formatdoc! {"
            {kerberos_container_start_commands}

            {COMMON_BASH_TRAP_FUNCTIONS}
            {remove_vector_shutdown_file_command}
            prepare_signal_handlers
            containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
            {start_command}
            wait_for_termination $!
            {create_vector_shutdown_file_command}
            ",
                kerberos_container_start_commands = kerberos_container_start_commands(hive),
                remove_vector_shutdown_file_command =
                    remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
                create_vector_shutdown_file_command =
                    create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            },
            s3_connection,
        ))
        .add_volume_mount(STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_CONFIG_MOUNT_DIR_NAME, STACKABLE_CONFIG_MOUNT_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(
            STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME,
            STACKABLE_LOG_CONFIG_MOUNT_DIR,
        )
        .context(AddVolumeMountSnafu)?
        .add_container_port(HIVE_PORT_NAME, HIVE_PORT.into())
        .add_container_port(METRICS_PORT_NAME, METRICS_PORT.into())
        .resources(merged_config.resources.clone().into())
        .readiness_probe(Probe {
            initial_delay_seconds: Some(10),
            period_seconds: Some(10),
            failure_threshold: Some(5),
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(HIVE_PORT_NAME.to_string()),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        })
        .liveness_probe(Probe {
            initial_delay_seconds: Some(30),
            period_seconds: Some(10),
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(HIVE_PORT_NAME.to_string()),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        });

    // TODO: refactor this when CRD versioning is in place
    // Warn if the capacity field has been set to anything other than 0Mi
    if let Some(Quantity(capacity)) = merged_config.resources.storage.data.capacity.as_ref() {
        if capacity != &"0Mi".to_string() {
            tracing::warn!(
                "The 'storage' CRD property is set to [{capacity}]. This field is not used and will be removed in a future release."
            );
        }
    }

    let recommended_object_labels = build_recommended_labels(
        hive,
        &resolved_product_image.app_version_label,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    );
    // Used for PVC templates that cannot be modified once they are deployed
    let unversioned_recommended_labels = Labels::recommended(build_recommended_labels(
        hive,
        // A version value is required, and we do want to use the "recommended" format for the other desired labels
        "none",
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    ))
    .context(LabelBuildSnafu)?;

    let metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(recommended_object_labels.clone())
        .context(MetadataBuildSnafu)?
        .build();

    let pvc = ListenerOperatorVolumeSourceBuilder::new(
        &ListenerReference::ListenerName(hive.role_listener_name(hive_role)),
        &unversioned_recommended_labels,
    )
    .build_pvc(LISTENER_VOLUME_NAME.to_owned())
    .context(BuildListenerVolumeSnafu)?;

    container_builder
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?;

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(Volume {
            name: STACKABLE_CONFIG_DIR_NAME.to_string(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: Some(Quantity("10Mi".to_string())),
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
            name: STACKABLE_CONFIG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            STACKABLE_LOG_DIR_NAME,
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_HIVE_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .affinity(&merged_config.affinity)
        .service_account_name(sa_name)
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = merged_config.logging.containers.get(&Container::Hive)
    {
        pod_builder
            .add_volume(Volume {
                name: STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME.to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: config_map.into(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    } else {
        pod_builder
            .add_volume(Volume {
                name: STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME.to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: rolegroup_ref.object_name(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    }

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    if hive.has_kerberos_enabled() {
        add_kerberos_pod_config(hive, hive_role, container_builder, &mut pod_builder)
            .context(AddKerberosConfigSnafu)?;
    }

    // this is the main container
    pod_builder.add_container(container_builder.build());

    // N.B. the vector container should *follow* the hive container so that the hive one is the
    // default, is started first and can provide any dependencies that vector expects
    if merged_config.logging.enable_vector_agent {
        match &hive.spec.cluster_config.vector_aggregator_config_map_name {
            Some(vector_aggregator_config_map_name) => {
                pod_builder.add_container(
                    product_logging::framework::vector_container(
                        resolved_product_image,
                        STACKABLE_CONFIG_MOUNT_DIR_NAME,
                        STACKABLE_LOG_DIR_NAME,
                        merged_config.logging.containers.get(&Container::Vector),
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                        vector_aggregator_config_map_name,
                    )
                    .context(BuildVectorContainerSnafu)?,
                );
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }

    let mut pod_template = pod_builder.build_template();
    pod_template.merge_from(role.config.pod_overrides.clone());
    pod_template.merge_from(rolegroup.config.pod_overrides.clone());

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(rolegroup_ref.object_name())
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(recommended_object_labels)
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: rolegroup.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        hive,
                        APP_NAME,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                    .context(LabelBuildSnafu)?
                    .into(),
                ),
                ..LabelSelector::default()
            },
            // TODO: Use method on RoleGroupRef once op-rs is released
            service_name: Some(rolegroup_headless_service_name(rolegroup_ref)),
            template: pod_template,
            volume_claim_templates: Some(vec![pvc]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::HiveCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // An invalid HBaseCluster was deserialized. Await for it to change.
        Error::InvalidHiveCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

/// Creates recommended `ObjectLabels` to be used in deployed resources
pub fn build_recommended_labels<'a, T>(
    owner: &'a T,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name: HIVE_CONTROLLER_NAME,
        role,
        role_group,
    }
}

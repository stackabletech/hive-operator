use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use product_config::{
    ProductConfigManager,
    types::PropertyNameKind,
    writer::{to_hadoop_xml, to_java_properties_string},
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::pod::{
        PodBuilder,
        container::ContainerBuilder,
        resources::ResourceRequirementsBuilder,
        volume::{
            ListenerOperatorVolumeSourceBuilder, ListenerReference,
            SecretOperatorVolumeSourceBuilder, VolumeBuilder,
        },
    },
    commons::{
        product_image_selection::ResolvedProductImage,
        secret_class::SecretClassVolumeProvisionParts,
    },
    crd::s3,
    k8s_openapi::{
        DeepMerge,
        api::core::v1::{EnvVar, PodTemplateSpec, VolumeMount},
    },
    kube::{Resource, ResourceExt},
    kvp::{Labels, ObjectLabels},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{self, framework::LoggingError},
    role_utils::RoleGroupRef,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::{
    HIVE_CONTROLLER_NAME, PrecomputedPodData, ValidatedHiveCluster, ValidatedLogging,
    ValidatedRoleConfig, ValidatedRoleGroupConfig, dereference::DereferencedObjects,
};
use crate::{
    OPERATOR_NAME,
    command::build_container_command_args,
    config::{
        jvm::{construct_hadoop_heapsize_env, construct_non_heap_jvm_args},
        opa::{HiveOpaConfig, OPA_TLS_VOLUME_NAME},
    },
    crd::{
        APP_NAME, Container, HIVE_SITE_XML, HiveRole, JVM_SECURITY_PROPERTIES_FILE,
        MetaStoreConfig, STACKABLE_CONFIG_DIR, STACKABLE_CONFIG_MOUNT_DIR_NAME, STACKABLE_LOG_DIR,
        STACKABLE_LOG_DIR_NAME,
        databases::{MetadataDatabaseConnection, derby_driver_class},
        v1alpha1,
    },
    framework::{
        product_logging::framework::{
            VectorContainerLogConfig, validate_logging_configuration_for_container,
        },
        types::{
            kubernetes::{ConfigMapName, NamespaceName, Uid},
            operator::ClusterName,
        },
    },
    kerberos::{kerberos_config_properties, kerberos_container_start_commands},
    listener::LISTENER_VOLUME_NAME,
};

pub const MAX_HIVE_LOG_FILES_SIZE: stackable_operator::memory::MemoryQuantity =
    stackable_operator::memory::MemoryQuantity {
        value: 10.0,
        unit: stackable_operator::memory::BinaryMultiple::Mebi,
    };

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to validate cluster name"))]
    InvalidClusterName {
        source: crate::framework::macros::attributed_string_type::Error,
    },

    #[snafu(display("failed to validate namespace"))]
    InvalidNamespace {
        source: crate::framework::macros::attributed_string_type::Error,
    },

    #[snafu(display("failed to validate UID"))]
    InvalidUid {
        source: crate::framework::macros::attributed_string_type::Error,
    },

    #[snafu(display("object defines no name"))]
    MissingName,

    #[snafu(display("object defines no namespace"))]
    MissingNamespace,

    #[snafu(display("object defines no UID"))]
    MissingUid,

    #[snafu(display("object defines no metastore role"))]
    NoMetaStoreRole,

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to resolve and merge resource config for role and role group"))]
    FailedToResolveResourceConfig { source: crate::crd::Error },

    #[snafu(display("failed to validate logging configuration"))]
    InvalidLoggingConfig {
        source: crate::framework::product_logging::framework::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to parse vector aggregator ConfigMap name"))]
    InvalidVectorAggregatorConfigMapName {
        source: crate::framework::macros::attributed_string_type::Error,
    },

    #[snafu(display("missing graceful shutdown timeout"))]
    MissingGracefulShutdownTimeout,

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: crate::config::jvm::Error },

    #[snafu(display(
        "Hive does not support skipping the verification of the tls enabled S3 server"
    ))]
    S3TlsNoVerificationNotSupported,

    #[snafu(display("failed to create hive container [{name}]"))]
    FailedToCreateHiveContainer {
        source: stackable_operator::builder::pod::container::Error,
        name: String,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build vector container"))]
    BuildVectorContainer { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: stackable_operator::builder::pod::volume::ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("internal operator failure"))]
    InternalOperatorFailure { source: crate::crd::Error },

    #[snafu(display("failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {rolegroup}"))]
    JvmSecurityProperties {
        source: product_config::writer::PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: stackable_operator::crd::s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("failed to build TLS certificate SecretClass Volume"))]
    TlsCertSecretClassVolumeBuild {
        source: stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilderError,
    },
}

pub fn validate_cluster(
    hive: &v1alpha1::HiveCluster,
    dereferenced: &DereferencedObjects,
    product_config_manager: &ProductConfigManager,
) -> Result<ValidatedHiveCluster, Error> {
    let cluster_name = ClusterName::from_str(&hive.meta().name.clone().context(MissingNameSnafu)?)
        .context(InvalidClusterNameSnafu)?;

    let namespace = NamespaceName::from_str(
        &hive
            .meta()
            .namespace
            .clone()
            .context(MissingNamespaceSnafu)?,
    )
    .context(InvalidNamespaceSnafu)?;

    let uid = Uid::from_str(&hive.meta().uid.clone().context(MissingUidSnafu)?)
        .context(InvalidUidSnafu)?;

    let role = hive.spec.metastore.as_ref().context(NoMetaStoreRoleSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &dereferenced.resolved_product_image.product_version,
        &transform_all_roles_to_config(
            hive,
            &[(
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
        product_config_manager,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let metastore_config = validated_config
        .get(&HiveRole::MetaStore.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let mut role_groups = BTreeMap::new();
    let mut precomputed_pod_data = BTreeMap::new();
    let mut metastore_rg_configs = BTreeMap::new();
    let mut metastore_pod_data = BTreeMap::new();

    let hive_namespace = namespace.to_string();

    for (rolegroup_name, rolegroup_config) in metastore_config.iter() {
        let rolegroup = hive.metastore_rolegroup_ref(rolegroup_name);
        let merged_config = hive
            .merged_config(&HiveRole::MetaStore, &rolegroup)
            .context(FailedToResolveResourceConfigSnafu)?;

        let validated_rg_config = validate_role_group_config(
            hive,
            &hive_namespace,
            &dereferenced.resolved_product_image,
            &rolegroup,
            rolegroup_config,
            &dereferenced.metadata_database_connection_details,
            dereferenced.s3_connection_spec.as_ref(),
            &merged_config,
            &dereferenced.hive_opa_config,
            &dereferenced.cluster_info,
        )?;

        let pod_data = compute_precomputed_pod_data(
            hive,
            &HiveRole::MetaStore,
            &dereferenced.resolved_product_image,
            &rolegroup,
            rolegroup_config,
            &dereferenced.metadata_database_connection_details,
            dereferenced.s3_connection_spec.as_ref(),
            &merged_config,
            &dereferenced.hive_opa_config,
        )?;

        metastore_rg_configs.insert(rolegroup_name.clone(), validated_rg_config);
        metastore_pod_data.insert(rolegroup_name.clone(), pod_data);
    }

    role_groups.insert(HiveRole::MetaStore, metastore_rg_configs);
    precomputed_pod_data.insert(HiveRole::MetaStore, metastore_pod_data);

    let mut role_configs = BTreeMap::new();
    for role in [HiveRole::MetaStore] {
        if let Some(role_config) = hive.role_config(&role) {
            let pdb = &role_config.common.pod_disruption_budget;
            role_configs.insert(
                role.clone(),
                ValidatedRoleConfig {
                    pdb_enabled: pdb.enabled,
                    pdb_max_unavailable: pdb.max_unavailable.unwrap_or(1),
                    listener_class: role_config.listener_class.clone(),
                    listener_name: hive.role_listener_name(&role),
                },
            );
        }
    }

    Ok(ValidatedHiveCluster::new(
        dereferenced.resolved_product_image.clone(),
        cluster_name,
        namespace,
        uid,
        role_groups,
        precomputed_pod_data,
        role_configs,
    ))
}

#[allow(clippy::too_many_arguments)]
fn validate_role_group_config(
    hive: &v1alpha1::HiveCluster,
    hive_namespace: &str,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>,
    role_group_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    database_connection_details: &stackable_operator::database_connections::drivers::jdbc::JdbcDatabaseConnectionDetails,
    s3_connection_spec: Option<&s3::v1alpha1::ConnectionSpec>,
    merged_config: &MetaStoreConfig,
    hive_opa_config: &Option<HiveOpaConfig>,
    cluster_info: &stackable_operator::utils::cluster_info::KubernetesClusterInfo,
) -> Result<ValidatedRoleGroupConfig, Error> {
    let hive_container_log_config =
        validate_logging_configuration_for_container(&merged_config.logging, Container::Hive)
            .context(InvalidLoggingConfigSnafu)?;

    let vector_container = if merged_config.logging.enable_vector_agent {
        ConfigMapName::from_str(
            hive.spec
                .cluster_config
                .vector_aggregator_config_map_name
                .as_deref()
                .context(VectorAggregatorConfigMapMissingSnafu)?,
        )
        .context(InvalidVectorAggregatorConfigMapNameSnafu)?;

        let vector_log_config =
            validate_logging_configuration_for_container(&merged_config.logging, Container::Vector)
                .context(InvalidLoggingConfigSnafu)?;

        Some(VectorContainerLogConfig {
            log_config: vector_log_config,
        })
    } else {
        None
    };

    let validated_logging = ValidatedLogging {
        hive_container: hive_container_log_config,
        vector_container,
    };

    let graceful_shutdown_timeout = merged_config
        .graceful_shutdown_timeout
        .context(MissingGracefulShutdownTimeoutSnafu)?;

    // Pre-compute hive-site.xml content
    let hive_site_xml_content = generate_hive_site_xml(
        hive,
        hive_namespace,
        resolved_product_image,
        role_group_config,
        database_connection_details,
        s3_connection_spec,
        &hive.spec.cluster_config.metadata_database,
        hive_opa_config.as_ref(),
        cluster_info,
    )?;

    // Pre-compute JVM security properties
    let jvm_security_properties_content =
        generate_jvm_security_properties(role_group_config, &rolegroup.role_group)?;

    // Pre-compute core-site.xml content (only needed when Kerberos is enabled without HDFS)
    let core_site_xml_content =
        if hive.has_kerberos_enabled() && hive.spec.cluster_config.hdfs.is_none() {
            let mut data = std::collections::BTreeMap::new();
            data.insert(
                "hadoop.security.authentication".to_string(),
                Some("kerberos".to_string()),
            );
            Some(product_config::writer::to_hadoop_xml(data.iter()))
        } else {
            None
        };

    Ok(ValidatedRoleGroupConfig {
        resources: merged_config.resources.clone(),
        logging: validated_logging,
        affinity: merged_config.affinity.clone(),
        graceful_shutdown_timeout,
        hive_site_xml_content,
        jvm_security_properties_content,
        core_site_xml_content,
    })
}

#[allow(clippy::too_many_arguments)]
fn generate_hive_site_xml(
    hive: &v1alpha1::HiveCluster,
    hive_namespace: &str,
    resolved_product_image: &ResolvedProductImage,
    role_group_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    database_connection_details: &stackable_operator::database_connections::drivers::jdbc::JdbcDatabaseConnectionDetails,
    s3_connection_spec: Option<&s3::v1alpha1::ConnectionSpec>,
    metadata_database: &MetadataDatabaseConnection,
    hive_opa_config: Option<&HiveOpaConfig>,
    cluster_info: &stackable_operator::utils::cluster_info::KubernetesClusterInfo,
) -> Result<String, Error> {
    let mut hive_site_data = String::new();

    for (property_name_kind, config) in role_group_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HIVE_SITE_XML => {
                let mut data = BTreeMap::new();

                data.insert(
                    MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
                    Some("/stackable/warehouse".to_string()),
                );

                let driver = match metadata_database {
                    MetadataDatabaseConnection::Derby(_) => {
                        derby_driver_class(&resolved_product_image.product_version)
                    }
                    _ => database_connection_details.driver.as_str(),
                };
                data.insert(
                    MetaStoreConfig::CONNECTION_DRIVER_NAME.to_string(),
                    Some(driver.to_owned()),
                );
                data.insert(
                    MetaStoreConfig::CONNECTION_URL.to_string(),
                    Some(database_connection_details.connection_url.to_string()),
                );
                if let Some(EnvVar {
                    name: username_env_name,
                    ..
                }) = &database_connection_details.username_env
                {
                    data.insert(
                        MetaStoreConfig::CONNECTION_USER_NAME.to_string(),
                        Some(format!("${{env:{username_env_name}}}")),
                    );
                }
                if let Some(EnvVar {
                    name: password_env_name,
                    ..
                }) = &database_connection_details.password_env
                {
                    data.insert(
                        MetaStoreConfig::CONNECTION_PASSWORD.to_string(),
                        Some(format!("${{env:{password_env_name}}}")),
                    );
                }

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

                // OPA settings
                if let Some(opa_config) = hive_opa_config {
                    data.extend(
                        opa_config
                            .as_config(&resolved_product_image.product_version)
                            .into_iter()
                            .map(|(k, v)| (k, Some(v)))
                            .collect::<BTreeMap<String, Option<String>>>(),
                    );
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

    Ok(hive_site_data)
}

fn generate_jvm_security_properties(
    role_group_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    rolegroup_name: &str,
) -> Result<String, Error> {
    let jvm_sec_props: BTreeMap<String, Option<String>> = role_group_config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();

    to_java_properties_string(jvm_sec_props.iter()).context(JvmSecurityPropertiesSnafu {
        rolegroup: rolegroup_name.to_owned(),
    })
}

#[allow(clippy::too_many_arguments)]
fn compute_precomputed_pod_data(
    hive: &v1alpha1::HiveCluster,
    hive_role: &HiveRole,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HiveCluster>,
    metastore_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    database_connection_details: &stackable_operator::database_connections::drivers::jdbc::JdbcDatabaseConnectionDetails,
    s3_connection: Option<&s3::v1alpha1::ConnectionSpec>,
    merged_config: &MetaStoreConfig,
    hive_opa_config: &Option<HiveOpaConfig>,
) -> Result<PrecomputedPodData, Error> {
    let role = hive.role(hive_role).context(InternalOperatorFailureSnafu)?;
    let rolegroup = hive
        .rolegroup(rolegroup_ref)
        .context(InternalOperatorFailureSnafu)?;

    // Build env vars
    let mut env_vars = vec![
        EnvVar {
            name: "HADOOP_HEAPSIZE".to_string(),
            value: Some(
                construct_hadoop_heapsize_env(merged_config).context(ConstructJvmArgumentsSnafu)?,
            ),
            ..EnvVar::default()
        },
        EnvVar {
            name: "HADOOP_OPTS".to_string(),
            value: Some(
                construct_non_heap_jvm_args(hive, role, &rolegroup_ref.role_group)
                    .context(ConstructJvmArgumentsSnafu)?,
            ),
            ..EnvVar::default()
        },
        EnvVar {
            name: "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
            value: Some(format!("{STACKABLE_LOG_DIR}/containerdebug")),
            ..EnvVar::default()
        },
    ];

    // Add database env vars
    if let Some(ref env) = database_connection_details.username_env {
        env_vars.push(env.clone());
    }
    if let Some(ref env) = database_connection_details.password_env {
        env_vars.push(env.clone());
    }

    // Add env overrides from product config
    for (property_name_kind, config) in metastore_config {
        if property_name_kind == &PropertyNameKind::Env {
            for (property_name, property_value) in config {
                if !property_name.is_empty() {
                    env_vars.push(EnvVar {
                        name: property_name.clone(),
                        value: Some(property_value.clone()),
                        ..EnvVar::default()
                    });
                }
            }
        }
    }

    // HDFS volumes/mounts
    let mut hdfs_volumes = Vec::new();
    let mut hdfs_volume_mounts = Vec::new();
    if let Some(hdfs) = &hive.spec.cluster_config.hdfs {
        hdfs_volumes.push(
            VolumeBuilder::new("hdfs-discovery")
                .with_config_map(&hdfs.config_map)
                .build(),
        );
        hdfs_volume_mounts.push(VolumeMount {
            name: "hdfs-discovery".to_string(),
            mount_path: "/stackable/mount/hdfs-config".to_string(),
            ..VolumeMount::default()
        });
    }

    // S3 volumes/mounts
    let mut s3_volumes = Vec::new();
    let mut s3_volume_mounts = Vec::new();
    if let Some(s3) = s3_connection {
        // We need to use the builder to get the right volumes — unfortunately S3 add_volumes_and_mounts
        // takes PodBuilder/ContainerBuilder. We'll compute those through a temporary builder.
        let mut temp_pod_builder = PodBuilder::new();
        let mut temp_container_builder =
            ContainerBuilder::new(APP_NAME).context(FailedToCreateHiveContainerSnafu {
                name: APP_NAME.to_string(),
            })?;
        s3.add_volumes_and_mounts(&mut temp_pod_builder, vec![&mut temp_container_builder])
            .context(ConfigureS3ConnectionSnafu)?;

        if s3.tls.uses_tls() && !s3.tls.uses_tls_verification() {
            S3TlsNoVerificationNotSupportedSnafu.fail()?;
        }

        let temp_pod = temp_pod_builder.build_template();
        if let Some(spec) = &temp_pod.spec {
            if let Some(vols) = &spec.volumes {
                s3_volumes.extend(vols.clone());
            }
        }
        let temp_container = temp_container_builder.build();
        if let Some(mounts) = &temp_container.volume_mounts {
            s3_volume_mounts.extend(mounts.clone());
        }
    }

    // OPA volumes/mounts
    let mut opa_volumes = Vec::new();
    let mut opa_volume_mounts = Vec::new();
    if let Some((tls_secret_class, tls_mount_path)) =
        hive_opa_config.as_ref().and_then(|opa_config| {
            opa_config
                .tls_secret_class
                .as_ref()
                .zip(opa_config.tls_ca_cert_mount_path())
        })
    {
        opa_volume_mounts.push(VolumeMount {
            name: OPA_TLS_VOLUME_NAME.to_string(),
            mount_path: tls_mount_path,
            ..VolumeMount::default()
        });

        let opa_tls_volume = VolumeBuilder::new(OPA_TLS_VOLUME_NAME)
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(
                    tls_secret_class,
                    SecretClassVolumeProvisionParts::Public,
                )
                .build()
                .context(TlsCertSecretClassVolumeBuildSnafu)?,
            )
            .build();
        opa_volumes.push(opa_tls_volume);
    }

    // Kerberos volumes/mounts
    let mut kerberos_volumes = Vec::new();
    let mut kerberos_volume_mounts = Vec::new();
    if hive.has_kerberos_enabled() {
        if let Some(kerberos_secret_class) = hive.kerberos_secret_class() {
            let kerberos_secret_operator_volume = SecretOperatorVolumeSourceBuilder::new(
                &kerberos_secret_class,
                SecretClassVolumeProvisionParts::PublicPrivate,
            )
            .with_service_scope(hive.name_any())
            .with_kerberos_service_name(hive_role.kerberos_service_name())
            .build()
            .context(TlsCertSecretClassVolumeBuildSnafu)?;
            kerberos_volumes.push(
                VolumeBuilder::new("kerberos")
                    .ephemeral(kerberos_secret_operator_volume)
                    .build(),
            );
            kerberos_volume_mounts.push(VolumeMount {
                name: "kerberos".to_string(),
                mount_path: "/stackable/kerberos".to_string(),
                ..VolumeMount::default()
            });
            env_vars.push(EnvVar {
                name: "KRB5_CONFIG".to_string(),
                value: Some("/stackable/kerberos/krb5.conf".to_string()),
                ..EnvVar::default()
            });
        }
    }

    // Container commands
    let db_type = hive.spec.cluster_config.metadata_database.as_hive_db_type();
    let start_command = if resolved_product_image.product_version.starts_with("3.") {
        format!(
            "bin/start-metastore --config {STACKABLE_CONFIG_DIR} --db-type {db_type} --hive-bin-dir bin &"
        )
    } else {
        indoc::formatdoc! {"
            bin/base --config \"{STACKABLE_CONFIG_DIR}\" --service schemaTool -dbType \"{db_type}\" -initOrUpgradeSchema
            bin/base --config \"{STACKABLE_CONFIG_DIR}\" --service metastore &
        "}
    };

    let commands = build_container_command_args(
        hive,
        indoc::formatdoc! {"
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
            COMMON_BASH_TRAP_FUNCTIONS = stackable_operator::utils::COMMON_BASH_TRAP_FUNCTIONS,
            remove_vector_shutdown_file_command =
                product_logging::framework::remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            create_vector_shutdown_file_command =
                product_logging::framework::create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        },
        s3_connection,
        hive_opa_config.as_ref(),
    );

    // RBAC service account name
    let sa_name = format!("{}-{}-serviceaccount", APP_NAME, hive.name_any());

    // Vector container
    let vector_container = if merged_config.logging.enable_vector_agent {
        let vector_aggregator_config_map_name = hive
            .spec
            .cluster_config
            .vector_aggregator_config_map_name
            .as_deref()
            .context(VectorAggregatorConfigMapMissingSnafu)?;

        Some(
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
        )
    } else {
        None
    };

    // Pod overrides
    let mut pod_overrides = PodTemplateSpec::default();
    pod_overrides.merge_from(role.config.pod_overrides.clone());
    pod_overrides.merge_from(rolegroup.config.pod_overrides.clone());

    // Listener PVC
    let recommended_object_labels = ObjectLabels {
        owner: hive,
        app_name: APP_NAME,
        app_version: "none",
        operator_name: OPERATOR_NAME,
        controller_name: HIVE_CONTROLLER_NAME,
        role: &rolegroup_ref.role,
        role_group: &rolegroup_ref.role_group,
    };
    let unversioned_recommended_labels =
        Labels::recommended(&recommended_object_labels).context(LabelBuildSnafu)?;

    let listener_volume_claim_template = ListenerOperatorVolumeSourceBuilder::new(
        &ListenerReference::ListenerName(hive.role_listener_name(hive_role)),
        &unversioned_recommended_labels,
    )
    .build_pvc(LISTENER_VOLUME_NAME.to_owned())
    .context(BuildListenerVolumeSnafu)?;

    Ok(PrecomputedPodData {
        env_vars,
        commands,
        kerberos_volumes,
        kerberos_volume_mounts,
        s3_volumes,
        s3_volume_mounts,
        hdfs_volumes,
        hdfs_volume_mounts,
        opa_volumes,
        opa_volume_mounts,
        vector_container,
        service_account_name: sa_name,
        replicas: rolegroup.replicas,
        pod_overrides,
        listener_volume_claim_template,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::{
        commons::{networking::DomainName, product_image_selection::ResolvedProductImage},
        kvp::LabelValue,
        utils::cluster_info::KubernetesClusterInfo,
    };

    use super::{ErrorDiscriminants, validate_cluster};
    use crate::{controller::dereference::DereferencedObjects, crd::v1alpha1};

    // CARGO_MANIFEST_DIR points to rust/operator-binary; properties.yaml is two levels up.
    const PROPERTIES_YAML: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/config-spec/properties.yaml"
    );

    fn test_hive_cluster() -> v1alpha1::HiveCluster {
        let yaml = indoc::indoc! {r#"
            apiVersion: hive.stackable.tech/v1alpha1
            kind: HiveCluster
            metadata:
              name: test-hive
              namespace: default
              uid: c27b3971-ca72-42c1-80a4-abdfc1db0ddd
            spec:
              image:
                productVersion: "4.0.1"
              clusterConfig:
                metadataDatabase:
                  derby:
                    databaseName: metastore_db
              metastore:
                roleGroups:
                  default:
                    replicas: 2
        "#};
        stackable_operator::utils::yaml_from_str_singleton_map(yaml)
            .expect("HiveCluster YAML should parse")
    }

    fn test_dereferenced_objects(hive: &v1alpha1::HiveCluster) -> DereferencedObjects {
        let resolved_product_image = ResolvedProductImage {
            product_version: "4.0.1".to_string(),
            app_version_label_value: LabelValue::from_str("4.0.1").unwrap(),
            image: "oci.stackable.tech/sdp/hive:4.0.1-stackable0.0.0-dev".to_string(),
            image_pull_policy: "IfNotPresent".to_string(),
            pull_secrets: None,
        };

        let metadata_database_connection_details = hive
            .spec
            .cluster_config
            .metadata_database
            .jdbc_connection_details("METADATA")
            .expect("derby JDBC connection details should be valid");

        DereferencedObjects {
            resolved_product_image,
            s3_connection_spec: None,
            metadata_database_connection_details,
            hive_opa_config: None,
            cluster_info: KubernetesClusterInfo {
                cluster_domain: DomainName::from_str("cluster.local")
                    .expect("cluster.local should be a valid DomainName"),
            },
        }
    }

    fn test_product_config() -> product_config::ProductConfigManager {
        product_config::ProductConfigManager::from_yaml_file(PROPERTIES_YAML)
            .expect("properties.yaml should be valid")
    }

    fn assert_validate_err(
        mutate: impl FnOnce(&mut v1alpha1::HiveCluster),
        expected: ErrorDiscriminants,
    ) {
        let mut hive = test_hive_cluster();
        mutate(&mut hive);
        let deref = test_dereferenced_objects(&hive);
        let product_config = test_product_config();
        let result = validate_cluster(&hive, &deref, &product_config);
        match result {
            Err(err) => assert_eq!(expected, ErrorDiscriminants::from(err)),
            Ok(_) => panic!("validate_cluster should have failed with {expected:?}"),
        }
    }

    #[test]
    fn test_validate_ok() {
        let hive = test_hive_cluster();
        let deref = test_dereferenced_objects(&hive);
        let product_config = test_product_config();
        let result = validate_cluster(&hive, &deref, &product_config);
        assert!(result.is_ok(), "validate_cluster should succeed");
        let validated = result.unwrap();
        assert_eq!(validated.name.to_string(), "test-hive");
        assert_eq!(validated.namespace.to_string(), "default");
    }

    #[test]
    fn test_validate_err_invalid_name() {
        assert_validate_err(
            |hive| hive.metadata.name = Some("UPPERCASE".to_string()),
            ErrorDiscriminants::InvalidClusterName,
        );
    }

    #[test]
    fn test_validate_err_missing_namespace() {
        assert_validate_err(
            |hive| hive.metadata.namespace = None,
            ErrorDiscriminants::MissingNamespace,
        );
    }

    #[test]
    fn test_validate_err_missing_uid() {
        assert_validate_err(
            |hive| hive.metadata.uid = None,
            ErrorDiscriminants::MissingUid,
        );
    }

    #[test]
    fn test_validate_err_missing_metastore_role() {
        assert_validate_err(
            |hive| hive.spec.metastore = None,
            ErrorDiscriminants::NoMetaStoreRole,
        );
    }
}

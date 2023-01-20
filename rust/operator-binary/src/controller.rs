//! Ensures that `Pod`s are configured and running for each [`HiveCluster`]
use crate::command::{self, build_container_command_args, S3_SECRET_DIR};
use crate::product_logging::{extend_role_group_config_map, resolve_vector_aggregator_address};
use crate::{discovery, OPERATOR_NAME};

use fnv::FnvHasher;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hive_crd::{
    Container, DbType, HiveCluster, HiveClusterStatus, HiveRole, MetaStoreConfig, APP_NAME,
    CERTS_DIR, HADOOP_HEAPSIZE, HIVE_ENV_SH, HIVE_PORT, HIVE_PORT_NAME, HIVE_SITE_XML,
    JVM_HEAP_FACTOR, METRICS_PORT, METRICS_PORT_NAME, STACKABLE_CONFIG_DIR,
    STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_MOUNT_DIR, STACKABLE_CONFIG_MOUNT_DIR_NAME,
    STACKABLE_LOG_DIR, STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_MOUNT_DIR,
    STACKABLE_LOG_MOUNT_DIR_NAME,
};
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        PodSecurityContextBuilder, SecretOperatorVolumeSourceBuilder, VolumeBuilder,
    },
    cluster_resources::ClusterResources,
    commons::{
        product_image_selection::ResolvedProductImage,
        s3::{S3AccessStyle, S3ConnectionSpec},
        tls::{CaCert, TlsVerification},
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, Probe, Service,
                ServicePort, ServiceSpec, TCPSocketAction, Volume, VolumeMount,
            },
        },
        apimachinery::pkg::{
            api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
        },
    },
    kube::{runtime::controller::Action, Resource, ResourceExt},
    labels::{role_group_selector_labels, role_selector_labels, ObjectLabels},
    logging::controller::ReconcilerError,
    memory::{to_java_heap_value, BinaryMultiple},
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::EnumDiscriminants;
use tracing::warn;

pub const HIVE_CONTROLLER_NAME: &str = "hivecluster";
const DOCKER_IMAGE_BASE_NAME: &str = "hive";

pub const MAX_HIVE_LOG_FILES_SIZE_IN_MIB: u32 = 10;

const OVERFLOW_BUFFER_ON_LOG_VOLUME_IN_MIB: u32 = 1;
const LOG_VOLUME_SIZE_IN_MIB: u32 =
    MAX_HIVE_LOG_FILES_SIZE_IN_MIB + OVERFLOW_BUFFER_ON_LOG_VOLUME_IN_MIB;

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
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to calculate service name for role {rolegroup}"))]
    RoleGroupServiceNameNotFound {
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },
    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to parse db type {db_type}"))]
    InvalidDbType {
        source: strum::ParseError,
        db_type: String,
    },
    #[snafu(display("failed to resolve S3 connection"))]
    ResolveS3Connection {
        source: stackable_operator::error::Error,
    },
    #[snafu(display(
        "Hive does not support skipping the verification of the tls enabled S3 server"
    ))]
    S3TlsNoVerificationNotSupported,
    #[snafu(display("failed to resolve and merge resource config for role and role group"))]
    FailedToResolveResourceConfig { source: stackable_hive_crd::Error },
    #[snafu(display("invalid java heap config - missing default or value in crd?"))]
    InvalidJavaHeapConfig,
    #[snafu(display("failed to convert java heap config to unit [{unit}]"))]
    FailedToConvertJavaHeap {
        source: stackable_operator::error::Error,
        unit: String,
    },
    #[snafu(display("failed to create hive container [{name}]"))]
    FailedToCreateHiveContainer {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_hive(hive: Arc<HiveCluster>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");
    let client = &ctx.client;
    let resolved_product_image: ResolvedProductImage =
        hive.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let s3_connection_spec: Option<S3ConnectionSpec> =
        if let Some(s3) = &hive.spec.cluster_config.s3 {
            Some(
                s3.resolve(
                    client,
                    &hive.namespace().ok_or(Error::ObjectHasNoNamespace)?,
                )
                .await
                .context(ResolveS3ConnectionSnafu)?,
            )
        } else {
            None
        };

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(
            hive.as_ref(),
            [(
                HiveRole::MetaStore.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::Cli,
                        PropertyNameKind::File(HIVE_SITE_XML.to_string()),
                        PropertyNameKind::File(HIVE_ENV_SH.to_string()),
                    ],
                    hive.spec.metastore.clone().context(NoMetaStoreRoleSnafu)?,
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
    )
    .context(CreateClusterResourcesSnafu)?;

    let metastore_role_service = build_metastore_role_service(&hive, &resolved_product_image)?;

    // we have to get the assigned ports
    let metastore_role_service = cluster_resources
        .add(client, &metastore_role_service)
        .await
        .context(ApplyRoleServiceSnafu)?;

    let vector_aggregator_address = resolve_vector_aggregator_address(&hive, client)
        .await
        .context(ResolveVectorAggregatorAddressSnafu)?;

    for (rolegroup_name, rolegroup_config) in metastore_config.iter() {
        let rolegroup = hive.metastore_rolegroup_ref(rolegroup_name);

        let config = hive
            .merged_config(&HiveRole::MetaStore, &rolegroup)
            .context(FailedToResolveResourceConfigSnafu)?;

        let rg_service = build_rolegroup_service(&hive, &resolved_product_image, &rolegroup)?;
        let rg_configmap = build_metastore_rolegroup_config_map(
            &hive,
            &resolved_product_image,
            &rolegroup,
            rolegroup_config,
            s3_connection_spec.as_ref(),
            &config,
            vector_aggregator_address.as_deref(),
        )?;
        let rg_statefulset = build_metastore_rolegroup_statefulset(
            &hive,
            &resolved_product_image,
            &rolegroup,
            rolegroup_config,
            s3_connection_spec.as_ref(),
            &config,
        )?;

        cluster_resources
            .add(client, &rg_service)
            .await
            .context(ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;

        cluster_resources
            .add(client, &rg_configmap)
            .await
            .context(ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup.clone(),
            })?;

        cluster_resources
            .add(client, &rg_statefulset)
            .await
            .context(ApplyRoleGroupStatefulSetSnafu {
                rolegroup: rolegroup.clone(),
            })?;
    }

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    for discovery_cm in discovery::build_discovery_configmaps(
        client,
        &*hive,
        &hive,
        &resolved_product_image,
        &metastore_role_service,
        None,
    )
    .await
    .context(BuildDiscoveryConfigSnafu)?
    {
        let discovery_cm = cluster_resources
            .add(client, &discovery_cm)
            .await
            .context(ApplyDiscoveryConfigSnafu)?;
        if let Some(generation) = discovery_cm.metadata.resource_version {
            discovery_hash.write(generation.as_bytes())
        }
    }

    let status = HiveClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
    };

    client
        .apply_patch_status(OPERATOR_NAME, &*hive, &status)
        .await
        .context(ApplyStatusSnafu)?;

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
}

/// The server-role service is the primary endpoint that should be used by clients that do not
/// perform internal load balancing including targets outside of the cluster.
pub fn build_metastore_role_service(
    hive: &HiveCluster,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    let role_name = HiveRole::MetaStore.to_string();

    let role_svc_name = hive
        .metastore_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(role_svc_name)
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                hive,
                &resolved_product_image.app_version_label,
                &role_name,
                "global",
            ))
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(service_ports()),
            selector: Some(role_selector_labels(hive, APP_NAME, &role_name)),
            type_: Some(
                hive.spec
                    .cluster_config
                    .service_type
                    .clone()
                    .unwrap_or_default()
                    .to_string(),
            ),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_metastore_rolegroup_config_map(
    hive: &HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<HiveCluster>,
    role_group_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    s3_connection_spec: Option<&S3ConnectionSpec>,
    merged_config: &MetaStoreConfig,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap> {
    let mut hive_site_data = String::new();
    let mut hive_env_data = String::new();

    for (property_name_kind, config) in role_group_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HIVE_ENV_SH => {
                let mut data = BTreeMap::new();
                // heap in mebi
                let heap_in_mebi = to_java_heap_value(
                    merged_config
                        .resources
                        .memory
                        .limit
                        .as_ref()
                        .context(InvalidJavaHeapConfigSnafu)?,
                    JVM_HEAP_FACTOR,
                    BinaryMultiple::Mebi,
                )
                .context(FailedToConvertJavaHeapSnafu {
                    unit: BinaryMultiple::Mebi.to_java_memory_unit(),
                })?;

                data.insert(HADOOP_HEAPSIZE.to_string(), Some(heap_in_mebi.to_string()));

                // other properties /  overrides
                for (property_name, property_value) in config {
                    data.insert(property_name.to_string(), Some(property_value.to_string()));
                }

                hive_env_data = data
                    .into_iter()
                    .map(|(key, value)| {
                        format!("export {key}={val}", val = value.unwrap_or_default())
                    })
                    .collect::<Vec<String>>()
                    .join("\n");
            }
            PropertyNameKind::File(file_name) if file_name == HIVE_SITE_XML => {
                let mut data = BTreeMap::new();

                data.insert(
                    MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
                    Some("/stackable/warehouse".to_string()),
                );

                if let Some(s3) = s3_connection_spec {
                    data.insert(MetaStoreConfig::S3_ENDPOINT.to_string(), s3.endpoint());
                    // The variable substitution is only available from version 3.
                    // if s3.secret_class.is_some() {
                    //     data.insert(
                    //         MetaStoreConfig::S3_ACCESS_KEY.to_string(),
                    //         Some(format!("${{env.{ENV_S3_ACCESS_KEY}}}")),
                    //     );
                    //     data.insert(
                    //         MetaStoreConfig::S3_SECRET_KEY.to_string(),
                    //         Some(format!("${{env.{ENV_S3_SECRET_KEY}}}")),
                    //     );
                    // }
                    // Thats why we need to replace this via script in the container command.
                    if s3.credentials.is_some() {
                        data.insert(
                            MetaStoreConfig::S3_ACCESS_KEY.to_string(),
                            Some(command::ACCESS_KEY_PLACEHOLDER.to_string()),
                        );
                        data.insert(
                            MetaStoreConfig::S3_SECRET_KEY.to_string(),
                            Some(command::SECRET_KEY_PLACEHOLDER.to_string()),
                        );
                    }
                    // END

                    data.insert(
                        MetaStoreConfig::S3_SSL_ENABLED.to_string(),
                        Some(s3.tls.is_some().to_string()),
                    );
                    data.insert(
                        MetaStoreConfig::S3_PATH_STYLE_ACCESS.to_string(),
                        Some((s3.access_style == Some(S3AccessStyle::Path)).to_string()),
                    );
                }

                // overrides
                for (property_name, property_value) in config {
                    data.insert(property_name.to_string(), Some(property_value.to_string()));
                }

                hive_site_data =
                    stackable_operator::product_config::writer::to_hadoop_xml(data.iter());
            }
            _ => {}
        }
    }

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
                .build(),
        )
        .add_data(HIVE_SITE_XML, hive_site_data)
        .add_data(HIVE_ENV_SH, hive_env_data);

    extend_role_group_config_map(
        rolegroup,
        vector_aggregator_address,
        &merged_config.logging,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: rolegroup.object_name(),
    })?;

    cm_builder
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    hive: &HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<HiveCluster>,
) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                hive,
                &resolved_product_image.app_version_label,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(service_ports()),
            selector: Some(role_group_selector_labels(
                hive,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the
/// corresponding [`Service`] (from [`build_rolegroup_service`]).
fn build_metastore_rolegroup_statefulset(
    hive: &HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<HiveCluster>,
    metastore_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    s3_connection: Option<&S3ConnectionSpec>,
    merged_config: &MetaStoreConfig,
) -> Result<StatefulSet> {
    let rolegroup = hive
        .spec
        .metastore
        .as_ref()
        .context(NoMetaStoreRoleSnafu)?
        .role_groups
        .get(&rolegroup_ref.role_group);
    let mut db_type: Option<DbType> = None;
    let mut container_builder =
        ContainerBuilder::new(APP_NAME).context(FailedToCreateHiveContainerSnafu {
            name: APP_NAME.to_string(),
        })?;

    for (property_name_kind, config) in metastore_config {
        match property_name_kind {
            PropertyNameKind::Env => {
                // overrides
                for (property_name, property_value) in config {
                    if property_name.is_empty() {
                        warn!("Received empty property_name for ENV... skipping");
                        continue;
                    }
                    container_builder.add_env_var(property_name, property_value);
                }
            }
            PropertyNameKind::Cli => {
                for (property_name, property_value) in config {
                    if property_name == MetaStoreConfig::DB_TYPE_CLI {
                        db_type = Some(DbType::from_str(property_value).with_context(|_| {
                            InvalidDbTypeSnafu {
                                db_type: property_value.to_string(),
                            }
                        })?);
                    }
                }
            }
            _ => {}
        }
    }

    let mut pod_builder = PodBuilder::new();

    if let Some(hdfs) = &hive.spec.cluster_config.hdfs {
        pod_builder.add_volume(
            VolumeBuilder::new("hdfs-site")
                .with_config_map(&hdfs.config_map)
                .build(),
        );
        container_builder.add_volume_mounts(vec![VolumeMount {
            name: "hdfs-site".to_string(),
            mount_path: format!("{STACKABLE_CONFIG_DIR}/hdfs-site.xml"),
            sub_path: Some("hdfs-site.xml".to_string()),
            ..VolumeMount::default()
        }]);
    }

    if let Some(s3_conn) = s3_connection {
        if let Some(credentials) = &s3_conn.credentials {
            pod_builder.add_volume(credentials.to_volume("s3-credentials"));
            container_builder.add_volume_mount("s3-credentials", S3_SECRET_DIR);
        }

        if let Some(tls) = &s3_conn.tls {
            match &tls.verification {
                TlsVerification::None {} => return S3TlsNoVerificationNotSupportedSnafu.fail(),
                TlsVerification::Server(server_verification) => {
                    match &server_verification.ca_cert {
                        CaCert::WebPki {} => {}
                        CaCert::SecretClass(secret_class) => {
                            let volume_name = format!("{secret_class}-tls-certificate");

                            let volume = VolumeBuilder::new(&volume_name)
                                .ephemeral(
                                    SecretOperatorVolumeSourceBuilder::new(secret_class).build(),
                                )
                                .build();
                            pod_builder.add_volume(volume);
                            container_builder.add_volume_mount(
                                &volume_name,
                                format!("{CERTS_DIR}{volume_name}"),
                            );
                        }
                    }
                }
            }
        }
    }

    let container_hive = container_builder
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-c".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
        ])
        .args(build_container_command_args(
            HiveRole::MetaStore
                .get_command(true, &db_type.unwrap_or_default().to_string())
                .join(" "),
            s3_connection,
        ))
        .add_volume_mount(STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .add_volume_mount(STACKABLE_CONFIG_MOUNT_DIR_NAME, STACKABLE_CONFIG_MOUNT_DIR)
        .add_volume_mount(STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_DIR)
        .add_volume_mount(STACKABLE_LOG_MOUNT_DIR_NAME, STACKABLE_LOG_MOUNT_DIR)
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
        })
        .build();

    pod_builder
        .metadata_builder(|m| {
            m.with_recommended_labels(build_recommended_labels(
                hive,
                &resolved_product_image.app_version_label,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
        })
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(container_hive)
        .add_volume(Volume {
            name: STACKABLE_CONFIG_DIR_NAME.to_string(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: Some(Quantity(format!("10Mi"))),
            }),
            ..Volume::default()
        })
        .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
            name: STACKABLE_CONFIG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .add_volume(Volume {
            name: STACKABLE_LOG_DIR_NAME.to_string(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: Some(Quantity(format!("{LOG_VOLUME_SIZE_IN_MIB}Mi"))),
            }),
            ..Volume::default()
        })
        .node_selector_opt(rolegroup.and_then(|rg| rg.selector.clone()))
        .security_context(
            PodSecurityContextBuilder::new()
                .run_as_user(1000)
                .run_as_group(1000)
                .fs_group(1000)
                .build(),
        );

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = merged_config.logging.containers.get(&Container::Hive)
    {
        pod_builder.add_volume(Volume {
            name: STACKABLE_LOG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(config_map.into()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    } else {
        pod_builder.add_volume(Volume {
            name: STACKABLE_LOG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    }

    if merged_config.logging.enable_vector_agent {
        pod_builder.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            STACKABLE_CONFIG_DIR_NAME,
            STACKABLE_LOG_DIR_NAME,
            merged_config.logging.containers.get(&Container::Vector),
        ));
    }

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                hive,
                &resolved_product_image.app_version_label,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if hive.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.and_then(|rg| rg.replicas).map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    hive,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: pod_builder.build_template(),
            volume_claim_templates: Some(vec![merged_config
                .resources
                .storage
                .data
                .build_pvc("data", Some(vec!["ReadWriteOnce"]))]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn error_policy(_obj: Arc<HiveCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

pub fn service_ports() -> Vec<ServicePort> {
    vec![
        ServicePort {
            name: Some(HIVE_PORT_NAME.to_string()),
            port: HIVE_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        },
        ServicePort {
            name: Some(METRICS_PORT_NAME.to_string()),
            port: METRICS_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        },
    ]
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
